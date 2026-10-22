"""
Distributed Consumer Worker
----------------------------
Multi-node Kafka consumer with:
- Consumer group membership (auto partition assignment)
- Manual offset commits (at-least-once delivery)
- Redis-based idempotent deduplication
- HTTP scoring via scorer service
- Prometheus metrics exposition
- Graceful shutdown on SIGTERM
"""

import os
import sys
import json
import time
import signal
import logging
import threading
from datetime import datetime

import redis
import httpx
from dotenv import load_dotenv
from kafka import KafkaConsumer, TopicPartition, OffsetAndMetadata
from kafka.consumer.subscription_state import ConsumerRebalanceListener
from prometheus_client import Counter, Histogram, Gauge, start_http_server

# Add project root to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.db import insert_metric

# Load configurations
load_dotenv()

# ============================================================
# Configuration
# ============================================================

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "server-metrics")
CONSUMER_GROUP = os.getenv("CONSUMER_GROUP", "anomaly-consumers")
NODE_ID = os.getenv("NODE_ID", "consumer-unknown")
METRICS_PORT = int(os.getenv("METRICS_PORT", "8001"))
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
SCORER_URLS = os.getenv("SCORER_URL", "http://localhost:8002").split(",")
DEDUPE_TTL_SECONDS = int(os.getenv("DEDUPE_TTL_SECONDS", "3600"))  # 1 hour

# How many times to retry one message before advancing past it. Without a
# cap, a permanently unprocessable message would block its partition forever.
MAX_PROCESS_ATTEMPTS = int(os.getenv("MAX_PROCESS_ATTEMPTS", "3"))

# (partition, offset) -> consecutive failure count. Local to this node; a
# rebalance moves the partition and the new owner starts its own count.
_failed_attempts = {}

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

events_consumed = Counter(
    'events_consumed_total',
    'Total events consumed from Kafka',
    ['node_id', 'partition']
)
events_duplicates_skipped = Counter(
    'events_duplicates_skipped_total',
    'Events skipped due to deduplication',
    ['node_id']
)
anomalies_detected = Counter(
    'anomalies_detected_total',
    'Total anomalies detected',
    ['node_id']
)
events_dropped = Counter(
    'events_dropped_total',
    'Events advanced past without being persisted (should stay at zero)',
    ['node_id', 'reason']
)
processing_latency = Histogram(
    'processing_latency_seconds',
    'End-to-end processing latency per event',
    ['node_id'],
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5]
)
consumer_lag_gauge = Gauge(
    'consumer_lag',
    'Consumer lag per partition',
    ['node_id', 'partition']
)
rebalance_counter = Counter(
    'consumer_rebalance_total',
    'Number of consumer group rebalances',
    ['node_id']
)

# ============================================================
# Redis Deduplication
# ============================================================

class RedisDeduplicator:
    """
    Two-phase deduplication so an event is never marked done before its
    effects are durable.

    claim()   -> SET NX a short-lived "pending" marker. We own the event.
    confirm() -> overwrite with "done" + the long TTL, only after the DB write.
    release() -> delete the marker so a retry can re-claim immediately.

    A crash between claim and confirm leaves a "pending" marker. We treat
    that as "proceed anyway" rather than "skip", because skipping would drop
    the event permanently. Reprocessing is harmless: the event_id UNIQUE
    constraint in PostgreSQL is the real guarantee against double-writes.
    """

    PENDING_TTL_SECONDS = 60  # Far longer than the ~10ms it takes to process

    def __init__(self, redis_url, ttl_seconds=3600):
        self.redis_client = None
        self.ttl = ttl_seconds
        self.redis_url = redis_url
        self._connect()

    def _connect(self):
        """Connect to Redis with retries."""
        for attempt in range(10):
            try:
                self.redis_client = redis.from_url(self.redis_url, decode_responses=True)
                self.redis_client.ping()
                logger.info(f"Connected to Redis: {self.redis_url}")
                return
            except Exception as e:
                logger.warning(f"Redis connection attempt {attempt + 1}/10 failed: {e}")
                time.sleep(3)
        raise ConnectionError("Could not connect to Redis")

    def claim(self, event_id):
        """
        Attempts to claim an event for processing.

        Returns True if we should process it, False only when the event is
        confirmed complete by an earlier run. On any ambiguity (a stale
        pending marker, or Redis being unreachable) we return True and let
        the database's UNIQUE constraint arbitrate, because processing twice
        is harmless and skipping loses data.
        """
        key = f"event:{event_id}"
        try:
            claimed = self.redis_client.set(
                key, f"pending:{NODE_ID}", nx=True, ex=self.PENDING_TTL_SECONDS
            )
            if claimed:
                return True

            # Key exists. Only "done" means a completed earlier run.
            state = self.redis_client.get(key)
            if state == "done":
                return False

            logger.warning(
                f"Event {event_id[:8]}... has a stale pending marker ({state}). "
                f"Reprocessing; the DB UNIQUE constraint will reject a double-write."
            )
            return True
        except redis.RedisError as e:
            logger.error(f"Redis error during claim, processing anyway: {e}")
            return True

    def confirm(self, event_id):
        """Marks an event as durably processed. Called after the DB write commits."""
        try:
            self.redis_client.set(f"event:{event_id}", "done", ex=self.ttl)
        except redis.RedisError as e:
            # Not fatal: the pending marker expires on its own and the DB
            # UNIQUE constraint still prevents a duplicate row.
            logger.error(f"Redis error during confirm: {e}")

    def release(self, event_id):
        """Drops our claim after a failure so the retry can re-claim at once."""
        try:
            self.redis_client.delete(f"event:{event_id}")
        except redis.RedisError as e:
            logger.error(f"Redis error during release: {e}")


# ============================================================
# Rebalance Listener
# ============================================================

class RebalanceListener(ConsumerRebalanceListener):
    """
    Logs partition assignment changes during consumer group rebalancing.
    """

    def __init__(self, node_id):
        self.node_id = node_id

    def on_partitions_revoked(self, revoked):
        partitions = [f"P{tp.partition}" for tp in revoked]
        if partitions:
            logger.info(f"Partitions REVOKED: {', '.join(partitions)}")
        # Whoever inherits these partitions tracks its own retries, so drop
        # ours rather than letting them accumulate.
        for tp in revoked:
            for key in [k for k in _failed_attempts if k[0] == tp.partition]:
                _failed_attempts.pop(key, None)

    def on_partitions_assigned(self, assigned):
        partitions = [f"P{tp.partition}" for tp in assigned]
        if partitions:
            logger.info(f"Partitions ASSIGNED: {', '.join(partitions)}")
            rebalance_counter.labels(node_id=self.node_id).inc()


# ============================================================
# Scorer Client
# ============================================================

class ScorerClient:
    """
    HTTP client for the scorer service with automatic failover
    between multiple scorer URLs.
    """

    def __init__(self, scorer_urls):
        self.urls = [url.strip() for url in scorer_urls]
        self.current_idx = 0
        self.client = httpx.Client(timeout=5.0)

    def score(self, cpu, memory, latency, error_rate):
        """
        Calls the scorer service to classify a metric point.
        Falls back to the next scorer URL on failure.
        """
        payload = {
            "cpu_usage": cpu,
            "memory_usage": memory,
            "request_latency": latency,
            "error_rate": error_rate,
        }

        last_error = None
        for i in range(len(self.urls)):
            url = self.urls[(self.current_idx + i) % len(self.urls)]
            try:
                response = self.client.post(f"{url}/score", json=payload)
                response.raise_for_status()
                result = response.json()
                self.current_idx = (self.current_idx + i) % len(self.urls)
                return result["is_anomaly"], result["anomaly_score"]
            except Exception as e:
                last_error = e
                logger.warning(f"Scorer {url} failed: {e}")

        raise ConnectionError(f"All scorer nodes failed. Last error: {last_error}")

    def close(self):
        self.client.close()


# ============================================================
# Main Consumer Loop
# ============================================================

_shutdown = threading.Event()


def signal_handler(signum, frame):
    """Graceful shutdown on SIGTERM/SIGINT."""
    logger.info(f"Received signal {signum}, shutting down gracefully...")
    _shutdown.set()


def create_consumer():
    """
    Creates a KafkaConsumer with manual offset commit and consumer group.
    Retries on connection failure.
    """
    max_retries = 15
    retry_delay = 5

    logger.info(f"Connecting to Kafka broker at: {KAFKA_BROKER}")
    for attempt in range(1, max_retries + 1):
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=[KAFKA_BROKER],
                group_id=CONSUMER_GROUP,
                auto_offset_reset='earliest',  # Process all unprocessed messages
                enable_auto_commit=False,       # Manual commit after successful processing
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                max_poll_records=50,
                session_timeout_ms=30000,
                heartbeat_interval_ms=10000,
            )
            logger.info(f"Connected to Kafka topic '{KAFKA_TOPIC}' in group '{CONSUMER_GROUP}'")
            return consumer
        except Exception as e:
            logger.warning(f"Kafka connection attempt {attempt}/{max_retries} failed: {e}")
            if attempt < max_retries:
                time.sleep(retry_delay)

    raise ConnectionError("Could not connect to Kafka broker")


def run_consumer():
    """
    Main processing loop:
    1. Poll messages from Kafka
    2. Deduplicate via Redis
    3. Score via HTTP scorer service
    4. Persist to PostgreSQL
    5. Commit offset only on success
    """
    # Register signal handlers
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    # Start Prometheus metrics server
    try:
        start_http_server(METRICS_PORT)
        logger.info(f"Prometheus metrics exposed on port {METRICS_PORT}")
    except Exception as e:
        logger.warning(f"Could not start metrics server: {e}")

    # Initialize components
    deduplicator = RedisDeduplicator(REDIS_URL, ttl_seconds=DEDUPE_TTL_SECONDS)
    scorer = ScorerClient(SCORER_URLS)
    consumer = create_consumer()

    # Subscribe with rebalance listener
    consumer.subscribe([KAFKA_TOPIC], listener=RebalanceListener(NODE_ID))

    logger.info("Consumer is ready. Processing messages...")

    try:
        while not _shutdown.is_set():
            # Poll for messages (timeout prevents blocking shutdown)
            message_batch = consumer.poll(timeout_ms=1000, max_records=50)

            if not message_batch:
                continue

            for topic_partition, messages in message_batch.items():
                # Highest offset in this partition whose work is fully durable.
                # Only this is safe to commit.
                last_good_offset = None
                # Offset to rewind to, when a message failed and deserves a retry.
                retry_from_offset = None

                for message in messages:
                    if _shutdown.is_set():
                        break

                    start_time = time.monotonic()
                    payload = message.value
                    partition = message.partition
                    attempt_key = (partition, message.offset)

                    # Extract fields
                    event_id = payload.get("event_id")
                    timestamp = payload.get("timestamp")
                    cpu = payload.get("cpu_usage")
                    memory = payload.get("memory_usage")
                    latency_val = payload.get("request_latency")
                    error_rate = payload.get("error_rate")

                    # Validate payload. A malformed event will never become
                    # valid, so advance past it rather than retrying forever.
                    if None in (timestamp, cpu, memory, latency_val, error_rate):
                        logger.warning(f"Skipped malformed event at offset {message.offset}: {payload}")
                        events_dropped.labels(node_id=NODE_ID, reason="malformed").inc()
                        last_good_offset = message.offset
                        continue

                    # Claim the event. False means an earlier run confirmed it.
                    if event_id and not deduplicator.claim(event_id):
                        events_duplicates_skipped.labels(node_id=NODE_ID).inc()
                        logger.debug(f"Duplicate event skipped: {event_id[:8]}...")
                        last_good_offset = message.offset
                        continue

                    try:
                        is_anomaly, anomaly_score = scorer.score(cpu, memory, latency_val, error_rate)
                        insert_metric(
                            timestamp, cpu, memory, latency_val, error_rate,
                            is_anomaly, anomaly_score, event_id=event_id
                        )
                    except Exception as e:
                        # Drop the claim so a retry can pick the event back up.
                        if event_id:
                            deduplicator.release(event_id)

                        attempts = _failed_attempts.get(attempt_key, 0) + 1
                        _failed_attempts[attempt_key] = attempts

                        if attempts >= MAX_PROCESS_ATTEMPTS:
                            # Poison message. Advance past it instead of
                            # blocking the whole partition indefinitely.
                            logger.error(
                                f"Giving up on offset {message.offset} in P{partition} after "
                                f"{attempts} attempts, advancing past it: {e}"
                            )
                            events_dropped.labels(node_id=NODE_ID, reason="max_attempts").inc()
                            _failed_attempts.pop(attempt_key, None)
                            last_good_offset = message.offset
                            continue

                        logger.warning(
                            f"Processing failed for offset {message.offset} in P{partition} "
                            f"(attempt {attempts}/{MAX_PROCESS_ATTEMPTS}), will retry: {e}"
                        )
                        retry_from_offset = message.offset
                        break  # Stop this partition here; everything after gets re-polled

                    # Durable. Mark the event done and let its offset be committed.
                    if event_id:
                        deduplicator.confirm(event_id)
                    _failed_attempts.pop(attempt_key, None)
                    last_good_offset = message.offset

                    # Track metrics
                    elapsed = time.monotonic() - start_time
                    events_consumed.labels(node_id=NODE_ID, partition=str(partition)).inc()
                    processing_latency.labels(node_id=NODE_ID).observe(elapsed)

                    if is_anomaly:
                        anomalies_detected.labels(node_id=NODE_ID).inc()

                    # Log to console
                    status = "🚨 ANOMALY" if is_anomaly else "✅ NORMAL "
                    eid_short = event_id[:8] if event_id else "no-id"
                    logger.info(
                        f"{status} | {eid_short}... | P{partition} | "
                        f"CPU:{cpu}% Mem:{memory}% Lat:{latency_val}ms Err:{error_rate*100:.2f}% "
                        f"Score:{anomaly_score:.4f} | {elapsed*1000:.1f}ms"
                    )

                # Commit only up to the last fully durable offset. Anything
                # after it stays uncommitted and will be redelivered.
                if last_good_offset is not None:
                    try:
                        consumer.commit({
                            topic_partition: OffsetAndMetadata(last_good_offset + 1, None)
                        })
                    except Exception as e:
                        logger.error(f"Offset commit failed for {topic_partition}: {e}")

                # poll() already advanced our in-memory position past the failed
                # message, so an uncommitted offset is not enough on its own.
                # Seek back explicitly to have it redelivered.
                if retry_from_offset is not None:
                    try:
                        consumer.seek(topic_partition, retry_from_offset)
                    except Exception as e:
                        logger.error(f"Seek to {retry_from_offset} failed for {topic_partition}: {e}")

            # Update consumer lag gauges
            try:
                for tp in consumer.assignment():
                    position = consumer.position(tp)
                    end_offsets = consumer.end_offsets([tp])
                    lag = end_offsets[tp] - position
                    consumer_lag_gauge.labels(
                        node_id=NODE_ID,
                        partition=str(tp.partition)
                    ).set(lag)
            except Exception:
                pass  # Lag measurement is best-effort

    except Exception as e:
        logger.error(f"Fatal consumer error: {e}", exc_info=True)
    finally:
        logger.info("Closing consumer and scorer connections...")
        scorer.close()
        consumer.close()
        logger.info("Consumer shut down complete.")


# ============================================================
# Entrypoint
# ============================================================
#
# This process has no /health route. The Prometheus metrics server on
# METRICS_PORT is the liveness signal, and the container healthcheck scrapes
# /metrics directly (see HEALTH_PATH in the Dockerfile). The scorer service
# is the one that serves /health, because FastAPI gives it one for free.

if __name__ == "__main__":
    logger.info(f"Starting distributed consumer worker: {NODE_ID}")
    run_consumer()
