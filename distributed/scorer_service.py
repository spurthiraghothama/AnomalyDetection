"""
Scorer Service (FastAPI)
------------------------
HTTP service that serves Isolation Forest model inference.
Implements Redis-based leader election:
- Leader: retrains the model periodically and uploads to MinIO
- Followers: serve inference, periodically sync model from MinIO

Endpoints:
- POST /score     — score a metric data point
- GET  /health    — node role, model age, uptime
- GET  /metrics   — Prometheus metrics (auto-registered by prometheus_client)
"""

import os
import sys
import time
import threading
import tempfile
import logging
from datetime import datetime, timezone
from contextlib import asynccontextmanager

import redis
import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from dotenv import load_dotenv
from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST
from starlette.responses import Response

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from model.detector import detect, reload_model, set_model, get_model
from model.train import train_model
from distributed.model_store import upload_model, download_model, get_model_metadata

# Load configurations
load_dotenv()

# ============================================================
# Configuration
# ============================================================

NODE_ID = os.getenv("NODE_ID", "scorer-unknown")
METRICS_PORT = int(os.getenv("METRICS_PORT", "8002"))
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

# Leader election settings
LEADER_LOCK_KEY = "scorer-leader"
LEADER_LOCK_TTL = 30        # Lock expires after 30s (requires heartbeat renewal)
LEADER_HEARTBEAT_INTERVAL = 10  # Renew lock every 10s
RETRAIN_INTERVAL = 300       # Retrain model every 5 minutes (leader only)

# Follower model sync interval
FOLLOWER_SYNC_INTERVAL = 60  # Check MinIO for new model every 60s

# ============================================================
# Logging
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format=f"[{NODE_ID}] %(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S"
)
logger = logging.getLogger(__name__)

# ============================================================
# Prometheus Metrics
# ============================================================

inference_latency = Histogram(
    'inference_latency_seconds',
    'Time to score a single metric point',
    ['node_id'],
    buckets=[0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1]
)
inference_requests = Counter(
    'inference_requests_total',
    'Total inference requests served',
    ['node_id']
)
model_retrain_counter = Counter(
    'model_retrain_total',
    'Number of model retraining cycles completed',
    ['node_id']
)
model_age_gauge = Gauge(
    'model_age_seconds',
    'Seconds since the current model was loaded',
    ['node_id']
)
node_role_gauge = Gauge(
    'node_role',
    'Current role: 1=leader, 0=follower',
    ['node_id']
)
leader_election_events = Counter(
    'leader_election_events_total',
    'Number of leader election transitions',
    ['node_id', 'event_type']
)

# ============================================================
# State
# ============================================================

_role = "follower"
_model_loaded_at = time.time()
_start_time = time.time()
_shutdown_event = threading.Event()
_elector = None  # Set during startup; read by /health


# ============================================================
# Leader Election (Redis-based)
# ============================================================

class LeaderElector:
    """
    Implements leader election using Redis SET NX with TTL.
    - Attempt to acquire lock: SET scorer-leader <node_id> NX EX 30
    - Leader heartbeat: renew the TTL every 10s
    - On failure to renew: demote to follower
    """

    # Check-and-act on a lock has to be one atomic operation. A GET followed
    # by an EXPIRE can interleave with another node's acquire, and we would
    # then extend a lease we no longer hold. Redis runs a script atomically.
    _RENEW_LUA = """
    if redis.call('GET', KEYS[1]) == ARGV[1] then
        return redis.call('EXPIRE', KEYS[1], ARGV[2])
    end
    return 0
    """

    _RELEASE_LUA = """
    if redis.call('GET', KEYS[1]) == ARGV[1] then
        return redis.call('DEL', KEYS[1])
    end
    return 0
    """

    def __init__(self, redis_url, node_id, lock_key, lock_ttl):
        self.node_id = node_id
        self.lock_key = lock_key
        self.lock_ttl = lock_ttl
        self.redis_client = None
        self._connect(redis_url)
        self._renew_script = self.redis_client.register_script(self._RENEW_LUA)
        self._release_script = self.redis_client.register_script(self._RELEASE_LUA)

    def _connect(self, redis_url):
        for attempt in range(10):
            try:
                self.redis_client = redis.from_url(redis_url, decode_responses=True)
                self.redis_client.ping()
                logger.info(f"LeaderElector connected to Redis")
                return
            except Exception as e:
                logger.warning(f"Redis connection attempt {attempt + 1}/10: {e}")
                time.sleep(3)
        raise ConnectionError("LeaderElector could not connect to Redis")

    def try_acquire(self):
        """Attempt to become leader. Returns True if lock was acquired."""
        try:
            result = self.redis_client.set(
                self.lock_key,
                self.node_id,
                nx=True,
                ex=self.lock_ttl
            )
            return bool(result)
        except redis.RedisError as e:
            logger.error(f"Redis error during lock acquisition: {e}")
            return False

    def renew(self):
        """
        Atomically renew the lock, but only if we still own it.
        Returns True if we are still the leader.
        """
        try:
            renewed = self._renew_script(
                keys=[self.lock_key], args=[self.node_id, self.lock_ttl]
            )
            return bool(renewed)
        except redis.RedisError as e:
            # Treat an unreachable Redis as lost leadership. Demoting on a
            # transient error costs us one retrain cycle; staying leader while
            # blind risks two nodes believing they lead at once.
            logger.error(f"Redis error during lock renewal: {e}")
            return False

    def release(self):
        """Atomically release the lock, but only if we own it."""
        try:
            if self._release_script(keys=[self.lock_key], args=[self.node_id]):
                logger.info("Leader lock released.")
        except redis.RedisError:
            pass

    def get_current_leader(self):
        """Returns the node_id of the current leader, or None."""
        try:
            return self.redis_client.get(self.lock_key)
        except redis.RedisError:
            return None


# ============================================================
# Background Threads
# ============================================================

def leader_election_loop(elector):
    """
    Continuously attempts to acquire/maintain leadership.
    If leader: renews lock + triggers retraining.
    If follower: attempts to acquire lock + syncs model from MinIO.
    """
    global _role, _model_loaded_at

    last_retrain = 0
    last_sync = 0
    local_model_mtime = None  # Track MinIO model version

    while not _shutdown_event.is_set():
        try:
            current_leader = elector.get_current_leader()

            if _role == "leader":
                # Renew the lock
                if elector.renew():
                    node_role_gauge.labels(node_id=NODE_ID).set(1)

                    # Periodic retraining
                    if time.time() - last_retrain >= RETRAIN_INTERVAL:
                        logger.info("Leader initiating model retraining...")
                        _retrain_and_upload()
                        last_retrain = time.time()
                        _model_loaded_at = time.time()
                else:
                    # Lost leadership
                    logger.warning("Lost leader lock! Demoting to follower.")
                    _role = "follower"
                    node_role_gauge.labels(node_id=NODE_ID).set(0)
                    leader_election_events.labels(node_id=NODE_ID, event_type="demoted").inc()

            else:  # follower
                # Try to become leader
                if elector.try_acquire():
                    logger.info("🏆 Acquired leader lock! Promoted to LEADER.")
                    _role = "leader"
                    node_role_gauge.labels(node_id=NODE_ID).set(1)
                    leader_election_events.labels(node_id=NODE_ID, event_type="promoted").inc()
                    last_retrain = time.time()  # Don't retrain immediately after election
                else:
                    node_role_gauge.labels(node_id=NODE_ID).set(0)

                    # Periodic model sync from MinIO (followers only)
                    if time.time() - last_sync >= FOLLOWER_SYNC_INTERVAL:
                        _sync_model_from_minio(local_model_mtime)
                        meta = get_model_metadata()
                        if meta:
                            local_model_mtime = meta["last_modified"]
                        last_sync = time.time()

            # Update model age
            model_age_gauge.labels(node_id=NODE_ID).set(time.time() - _model_loaded_at)

        except Exception as e:
            logger.error(f"Leader election loop error: {e}")

        # Sleep with shutdown check
        _shutdown_event.wait(timeout=LEADER_HEARTBEAT_INTERVAL)


def _retrain_and_upload():
    """Leader-only: retrain the Isolation Forest and upload to MinIO."""
    try:
        # Use a temp file for training
        with tempfile.NamedTemporaryFile(suffix=".pkl", delete=False) as f:
            tmp_path = f.name

        # Train new model (reuses existing training logic)
        model = train_model(num_samples=1000, save_path=tmp_path)

        # Update in-memory model
        set_model(model)

        # Upload to MinIO for other nodes
        version_tag = f"v-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}"
        upload_model(tmp_path, version_tag=version_tag)

        model_retrain_counter.labels(node_id=NODE_ID).inc()
        logger.info(f"Model retrained and uploaded: {version_tag}")

        # Cleanup temp file
        os.unlink(tmp_path)

    except Exception as e:
        logger.error(f"Retraining failed: {e}", exc_info=True)


def _sync_model_from_minio(local_mtime):
    """Follower-only: check MinIO for newer model and hot-reload."""
    global _model_loaded_at
    try:
        meta = get_model_metadata()
        if meta is None:
            return  # No model in MinIO yet

        # Compare timestamps
        if local_mtime and meta["last_modified"] <= local_mtime:
            return  # No newer model available

        logger.info(f"Newer model found in MinIO (version={meta['version']}). Downloading...")
        tmp_path = os.path.join(tempfile.gettempdir(), f"model_{NODE_ID}.pkl")
        if download_model(tmp_path):
            reload_model(tmp_path)
            _model_loaded_at = time.time()
            logger.info("Model hot-reloaded from MinIO.")

    except Exception as e:
        logger.error(f"Model sync failed: {e}")


def _initialize_model():
    """
    Startup initialization: try MinIO first, then fall back to baked-in model.
    """
    global _model_loaded_at

    # Try to download from MinIO
    try:
        tmp_path = os.path.join(tempfile.gettempdir(), f"model_{NODE_ID}.pkl")
        if download_model(tmp_path):
            reload_model(tmp_path)
            _model_loaded_at = time.time()
            logger.info("Loaded model from MinIO checkpoint.")
            return
    except Exception as e:
        logger.warning(f"Could not load model from MinIO: {e}")

    # Fall back to baked-in model from Docker image
    try:
        get_model()  # This loads from model/anomaly_model.pkl
        _model_loaded_at = time.time()
        logger.info("Loaded baked-in model from disk.")

        # If we're the first node, upload the baked-in model to MinIO
        try:
            from model.detector import MODEL_PATH
            upload_model(MODEL_PATH, version_tag="initial")
            logger.info("Uploaded baked-in model to MinIO as initial checkpoint.")
        except Exception:
            pass  # Another node may have already done this

    except FileNotFoundError:
        logger.error("No model available! Training a fresh model...")
        model = train_model(num_samples=1000)
        set_model(model)
        _model_loaded_at = time.time()


# ============================================================
# FastAPI Application
# ============================================================

class ScoreRequest(BaseModel):
    cpu_usage: float
    memory_usage: float
    request_latency: float
    error_rate: float


class ScoreResponse(BaseModel):
    is_anomaly: bool
    anomaly_score: float
    scored_by: str


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup/shutdown lifecycle for FastAPI."""
    # Startup
    logger.info(f"Scorer service starting: {NODE_ID}")
    _initialize_model()

    # Start leader election background thread
    global _elector
    elector = LeaderElector(REDIS_URL, NODE_ID, LEADER_LOCK_KEY, LEADER_LOCK_TTL)
    _elector = elector  # Exposed so /health can report the current leader
    election_thread = threading.Thread(
        target=leader_election_loop,
        args=(elector,),
        daemon=True,
        name="leader-election"
    )
    election_thread.start()

    yield  # App is running

    # Shutdown
    logger.info("Shutting down scorer service...")
    _shutdown_event.set()
    elector.release()
    election_thread.join(timeout=5)


app = FastAPI(
    title="Anomaly Scorer Service",
    description=f"Node: {NODE_ID}",
    lifespan=lifespan,
)


@app.post("/score", response_model=ScoreResponse)
def score_metric(req: ScoreRequest):
    """Score a single metric data point using the Isolation Forest model."""
    start = time.monotonic()

    try:
        is_anomaly, anomaly_score = detect(
            req.cpu_usage,
            req.memory_usage,
            req.request_latency,
            req.error_rate
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Scoring error: {e}")

    elapsed = time.monotonic() - start
    inference_latency.labels(node_id=NODE_ID).observe(elapsed)
    inference_requests.labels(node_id=NODE_ID).inc()

    return ScoreResponse(
        is_anomaly=is_anomaly,
        anomaly_score=anomaly_score,
        scored_by=NODE_ID,
    )


@app.get("/health")
def health():
    """Returns node health, role, model age, and who currently holds the lock."""
    return {
        "status": "healthy",
        "node_id": NODE_ID,
        "role": _role,
        "model_age_seconds": round(time.time() - _model_loaded_at, 1),
        "uptime_seconds": round(time.time() - _start_time, 1),
        "current_leader": _elector.get_current_leader() if _elector else None,
    }


@app.get("/metrics")
def metrics():
    """Prometheus metrics endpoint."""
    return Response(
        content=generate_latest(),
        media_type=CONTENT_TYPE_LATEST,
    )


# ============================================================
# Entrypoint
# ============================================================

if __name__ == "__main__":
    logger.info(f"Starting scorer service on port {METRICS_PORT}")
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=METRICS_PORT,
        log_level="info",
        access_log=False,
    )
