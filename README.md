# Fault-Tolerant Distributed Real-Time Anomaly Detection Platform

A production-grade, fault-tolerant distributed system for detecting anomalies in server metrics using real-time streaming, the **Isolation Forest** algorithm, and distributed-systems primitives (consumer groups, leader election, idempotent processing, chaos testing).

![Python](https://img.shields.io/badge/Python-3.11-blue)
![Kafka](https://img.shields.io/badge/Kafka-7.3.0-red)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED)
![Redis](https://img.shields.io/badge/Redis-7-DC382D)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-336791)
![Prometheus](https://img.shields.io/badge/Prometheus-Monitoring-E6522C)
![Grafana](https://img.shields.io/badge/Grafana-Dashboards-F46800)

---

## 📋 Key Capabilities

- ⚡ **Multi-Node Consumer Group**: 3 Kafka consumers with automatic partition assignment and failover
- 🔒 **Idempotent Processing**: UUID event IDs + Redis SETNX dedup + PostgreSQL UNIQUE constraints
- 🏆 **Leader Election**: Redis-based leader election for model retraining (30s TTL heartbeat)
- 📦 **State Replication**: MinIO (S3-compatible) for model checkpoint sharing across scorer nodes
- 📊 **Full Observability**: Prometheus metrics + pre-provisioned Grafana dashboards
- 💥 **Chaos Testing**: Automated fault injection with recovery time measurement
- 🤖 **Isolation Forest ML**: Unsupervised anomaly detection on 4 server metrics

---

## 🏗️ Architecture

```
                              ┌──────────────────────────────────────────────────────────────┐
                              │                    Docker Compose Cluster                     │
                              └──────────────────────────────────────────────────────────────┘

  ┌──────────────┐       ┌─────────────────┐       ┌────────────────────────────────────────┐
  │   Producer   │──────▶│  Kafka Broker    │──────▶│    Consumer Group (3 nodes)             │
  │  (1 node)    │       │  (6 partitions)  │       │    ┌──────────┬──────────┬──────────┐  │
  │              │       │  + Zookeeper     │       │    │consumer-1│consumer-2│consumer-3│  │
  │  event_id:   │       └─────────────────┘       │    │ manual   │ manual   │ manual   │  │
  │  UUID/msg    │                                  │    │ commit   │ commit   │ commit   │  │
  └──────────────┘                                  │    └────┬─────┴────┬─────┴────┬─────┘  │
                                                     └─────────┼──────────┼──────────┼────────┘
                                                               │          │          │
                         ┌─────────────────┐                   ▼          ▼          ▼
                         │     Redis        │◀──── Dedupe check (SET NX event_id, TTL 1h)
                         │  (dedupe store   │
                         │  + leader lock)  │         ┌─────────────────────────────┐
                         └─────────────────┘         │   Scorer Nodes (2 nodes)    │
                                                      │  ┌─────────┐ ┌─────────┐   │
                         ┌─────────────────┐         │  │scorer-1 │ │scorer-2 │   │
                         │     MinIO        │◀───────│  │(LEADER) │ │(FOLLOW) │   │
                         │  (model store)   │         │  │retrain  │ │infer    │   │
                         └─────────────────┘         │  │+infer   │ │only     │   │
                                                      │  └─────────┘ └─────────┘   │
                         ┌─────────────────┐         └─────────────────────────────┘
                         │   PostgreSQL     │◀──── All consumers write scored results
                         │  (metrics DB)    │
                         └─────────────────┘              ┌──────────────────┐
                                                           │  Chaos Test      │
                         ┌─────────────────┐              │  Runner          │
                         │   Prometheus     │◀──── scrape  │  (kills random   │
                         │   + Grafana      │      /metrics│   consumer q2min)│
                         └─────────────────┘              └──────────────────┘
```

### Service Inventory (14 containers)

| Service | Image / Build | Purpose |
|---------|--------------|---------|
| `zookeeper` | `confluentinc/cp-zookeeper:7.3.0` | Kafka coordination |
| `kafka` | `confluentinc/cp-kafka:7.3.0` | Event streaming broker (6 partitions) |
| `kafka-setup` | One-shot script | Creates topic with 6 partitions |
| `redis` | `redis:7-alpine` | Deduplication store + leader election lock |
| `postgres` | `postgres:15-alpine` | Multi-writer metrics database |
| `minio` | `minio/minio:latest` | S3-compatible model checkpoint store |
| `producer` | Build from `./` | Kafka metric producer with UUID event IDs |
| `consumer-1/2/3` | Build from `./` | Consumer group with manual offset commit |
| `scorer-1/2` | Build from `./` | Model inference + leader-elected retraining |
| `prometheus` | `prom/prometheus:latest` | Metrics scraping (10s interval) |
| `grafana` | `grafana/grafana:latest` | Pre-provisioned dashboards |
| `chaos-runner` | Build from `./` | Automated fault injection |

---

## 🚀 Quick Start

### Prerequisites
- Docker & Docker Compose (v2+)
- Git
- ~4GB RAM available for the cluster

### Launch the Full Cluster

```bash
# Clone the repository
git clone https://github.com/YOUR_USERNAME/AnomalyDetection.git
cd AnomalyDetection

# Build and start all 14 services
docker compose up --build -d

# Watch the logs
docker compose logs -f consumer-1 consumer-2 consumer-3

# View the Grafana dashboard
# Open http://localhost:3000 (admin/admin)
```

### Verify Consumer Group

```bash
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe --group anomaly-consumers
```

Expected output: 6 partitions distributed across 3 consumers (2 partitions each).

### Stop the Cluster

```bash
docker compose down -v  # -v removes volumes for a clean restart
```

---

## 📊 Observability

### Grafana Dashboard (http://localhost:3000)

Pre-provisioned dashboard with panels for:
- **Events Consumed/sec** (by node) — shows throughput per consumer
- **Processing Latency** (p50/p95/p99) — end-to-end event processing time
- **Consumer Lag** (by partition) — shows rebalance impact during failures
- **Rebalance Events** — bar chart of Kafka consumer group rebalances
- **Anomalies Detected/min** — live anomaly detection rate
- **Scorer Node Roles** — 1=Leader, 0=Follower timeline
- **Inference Latency** — p50/p95 model scoring latency
- **Chaos Kill Timeline** — visualization of fault injection events
- **Recovery Time** — seconds to recover after each kill
- **Service Health** — UP/DOWN status for all nodes

### Prometheus (http://localhost:9090)

Key metrics exposed:
| Metric | Type | Description |
|--------|------|-------------|
| `events_produced_total` | Counter | Events published to Kafka |
| `events_consumed_total` | Counter | Events processed per node/partition |
| `events_duplicates_skipped_total` | Counter | Deduplicated events skipped |
| `processing_latency_seconds` | Histogram | Consumer processing latency |
| `consumer_lag` | Gauge | Per-partition consumer lag |
| `consumer_rebalance_total` | Counter | Rebalance events |
| `inference_latency_seconds` | Histogram | Model scoring latency |
| `node_role` | Gauge | 1=leader, 0=follower |
| `chaos_kills_total` | Counter | Containers killed by chaos runner |
| `chaos_recovery_seconds` | Histogram | Recovery time distribution |

---

## 💥 Chaos Testing

The chaos runner automatically starts 60 seconds after the cluster stabilizes and performs fault injection.

### How It Works

1. **Selects** a random live consumer container
2. **Records** pre-kill metrics (consumer lag, event count)
3. **Kills** the container via Docker SDK
4. **Measures** recovery time (container restart + Kafka rebalance)
5. **Verifies** no data loss or duplication
6. **Repeats** every 2 minutes for 10 cycles

### Watch Chaos Tests Live

```bash
# Follow chaos runner output
docker compose logs -f chaos-runner

# Watch Grafana dashboard during chaos for visual impact
# Open http://localhost:3000
```

### View Results

After all 10 kills complete, results are written to `./chaos_results/`:

```bash
cat chaos_results/chaos_report.md    # Human-readable report
cat chaos_results/chaos_results.json # Structured JSON data
```

### Measured Results

From an actual run on 2026-10-22 (5 kill cycles, 60s apart). Raw output is in
`chaos_results/chaos_report.md` and `chaos_results/chaos_results.json`.

| Metric | Value |
|--------|-------|
| Total kill cycles | 5 |
| Successful recoveries | 5 / 5 |
| Container restart time (avg) | 1.74s |
| Container restart time (min / max) | 1.55s / 2.34s |
| Events processed during the run | 3,967 → 5,333 |
| Rows in PostgreSQL afterwards | 5,355 |
| Distinct `event_id` values | 5,355 |
| **Duplicate rows** | **0** |
| **Data loss** | **None** |

Partition assignment returned to an even 2/2/2 across the three consumers with
zero lag on every partition after the run.

**What these numbers do and do not say.** The recovery figure is the time for a
killed container to be running again. It is *not* the time for the cluster to
resume full throughput. Kafka only declares a consumer dead after
`session_timeout_ms` (30s here) before reassigning its partitions, so the dead
node's two partitions stall for substantially longer than 1.74s. The
"events during kill" column in the report shows that effect directly: throughput
drops sharply inside each kill window and recovers afterwards.

The zero-duplicate and zero-data-loss results are stronger claims, because they
were verified directly against PostgreSQL rather than inferred: 5,355 rows and
5,355 distinct event IDs means the deduplication path held across all five
failures.

Two caveats worth knowing when reading the Grafana panels:

- `consumer_rebalance_total` is a per-process counter, so it resets to zero when
  a killed container restarts. Use `increase()` over a window rather than the
  raw value.
- The chaos runner is a short-lived job that exits when the test ends, so its
  Prometheus target goes down afterwards. That is expected, not a failure.

---

## 🔧 Distributed Systems Design

### 1. Multi-Node Consumer Group with Failover

- **3 consumer instances** join the `anomaly-consumers` Kafka consumer group
- Kafka auto-assigns 6 partitions across 3 consumers (2 each)
- **Manual offset commits** (`enable_auto_commit=False`) ensure offsets are only committed after:
  1. Deduplication check passes
  2. Scorer returns a result
  3. Database write succeeds
- On node failure, Kafka rebalances partitions to surviving consumers within ~15-25s

### 2. Idempotent Processing

Three-layer deduplication:
1. **Redis SETNX**: `SET event:<uuid> 1 NX EX 3600` — first-write-wins with 1h TTL
2. **PostgreSQL UNIQUE**: `event_id TEXT UNIQUE` column with `ON CONFLICT DO NOTHING`
3. **At-least-once delivery**: Manual commit ensures no events are lost; dedup ensures no doubles

### 3. Leader Election for Model Scoring

- Uses Redis `SET scorer-leader <node_id> NX EX 30` for distributed lock
- Leader **heartbeats** every 10s by renewing the TTL
- Leader **retrains** the Isolation Forest every 5 minutes and uploads to MinIO
- On leader failure, the lock expires in ≤30s and a follower acquires it
- All nodes (leader + followers) continue serving inference throughout

### 4. State Replication

- **MinIO** acts as a shared model checkpoint store
- Leader uploads retrained model with version metadata
- Followers poll MinIO every 60s for newer checkpoints and hot-reload
- On fresh cluster start, the first node uploads the baked-in model as initial checkpoint
- Ensures zero cold-start: every node loads from MinIO on boot

---

## 📁 Project Structure

```
AnomalyDetection/
├── chaos/
│   ├── __init__.py
│   └── chaos_runner.py              # Automated fault injection + recovery measurement
├── config/
│   ├── prometheus.yml               # Prometheus scrape configuration
│   └── grafana/
│       └── provisioning/
│           ├── datasources/
│           │   └── prometheus.yml    # Grafana → Prometheus datasource
│           └── dashboards/
│               ├── dashboard.yml     # Dashboard auto-provisioning
│               └── anomaly_detection.json  # Pre-built Grafana dashboard
├── dashboard/
│   ├── app.py                       # Original Kafka-based dashboard (local dev)
│   └── streamlit_app.py             # Cloud-ready Streamlit version
├── database/
│   └── db.py                        # PostgreSQL + SQLite dual-backend database layer
├── distributed/
│   ├── __init__.py
│   ├── consumer_worker.py           # Multi-node Kafka consumer with manual commit + dedup
│   ├── scorer_service.py            # FastAPI scorer with Redis leader election
│   └── model_store.py               # MinIO model checkpoint upload/download
├── model/
│   ├── train.py                     # Isolation Forest training (reusable train_model())
│   ├── detector.py                  # Model inference with hot-reload support
│   └── anomaly_model.pkl            # Trained model artifact
├── alerts/
│   └── notifier.py                  # Email & desktop alerts (local dev)
├── producer.py                      # Kafka producer with UUID event_ids + Prometheus
├── consumer.py                      # Legacy single-node consumer (local dev only)
├── Dockerfile                       # Multi-service image (producer/consumer/scorer/chaos)
├── docker-compose.yml               # Full 14-service cluster orchestration
├── .env                             # Local development config
├── .env.docker                      # Docker-specific overrides
├── requirements.txt                 # Python dependencies
├── .gitignore
└── README.md                        # This file
```

---

## 🛠️ Local Development (Without Docker)

For developing and testing individual components without the full cluster:

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Train the model
python model/train.py

# Start Kafka (Docker still needed for Kafka)
docker compose up -d zookeeper kafka

# Run the legacy single-node consumer
python consumer.py

# Run the producer
python producer.py

# Launch the dashboard
streamlit run dashboard/app.py
```

---

## 📊 The Isolation Forest Algorithm

**Why Isolation Forest?**
- ✅ **Unsupervised**: No labeled data required
- ✅ **Fast**: O(log n) average complexity, <5ms inference
- ✅ **Effective**: Handles multi-dimensional outliers well
- ✅ **Interpretable**: Easy to understand tree structure

**How it works**:
1. Randomly selects features and split points
2. Builds isolation trees to separate anomalies from normal points
3. Anomalies require fewer splits to isolate (shallow trees)
4. Normal points require many splits (deep trees)

**Configuration**: `contamination=0.05` (5% expected anomaly rate), trained on synthetic normal server telemetry across 4 metrics (CPU, Memory, Latency, Error Rate).

---

## 📜 License

MIT License
