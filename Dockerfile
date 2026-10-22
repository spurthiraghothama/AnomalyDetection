FROM python:3.11-slim

# System dependencies for psycopg2, etc.
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libpq-dev curl && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Train the initial model (baked into image as fallback)
RUN python model/train.py

# Expose ports for Prometheus metrics + FastAPI
EXPOSE 8000 8001 8002

# SERVICE_ROLE determines which entrypoint to run:
#   producer  -> python producer.py
#   consumer  -> python -m distributed.consumer_worker
#   scorer    -> python -m distributed.scorer_service
#   chaos     -> python -m chaos.chaos_runner
ENV SERVICE_ROLE=consumer

# Healthcheck: the scorer serves a real /health via FastAPI; the producer,
# consumer and chaos roles only run a Prometheus metrics server, so they are
# probed on /metrics. Compose sets HEALTH_PATH=/health for the scorers.
HEALTHCHECK --interval=15s --timeout=5s --retries=3 \
    CMD curl -f http://localhost:${METRICS_PORT:-8001}${HEALTH_PATH:-/metrics} || exit 1

CMD ["sh", "-c", "\
    if [ \"$SERVICE_ROLE\" = 'producer' ]; then python producer.py; \
    elif [ \"$SERVICE_ROLE\" = 'consumer' ]; then python -m distributed.consumer_worker; \
    elif [ \"$SERVICE_ROLE\" = 'scorer' ]; then python -m distributed.scorer_service; \
    elif [ \"$SERVICE_ROLE\" = 'chaos' ]; then python -m chaos.chaos_runner; \
    else echo 'Unknown SERVICE_ROLE: $SERVICE_ROLE' && exit 1; \
    fi"]
