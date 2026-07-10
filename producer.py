import time
import json
import random
import os
from datetime import datetime
from dotenv import load_dotenv
from kafka import KafkaProducer
import numpy as np

# Load configuration parameters from the .env file
load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "server-metrics")

def create_kafka_producer():
    """
    Attempts to initialize and connect to the Kafka broker.
    Retries automatically if Kafka is still spinning up.
    """
    producer = None
    max_retries = 10
    retry_delay = 5  # seconds
    
    print(f"Connecting to Kafka broker at: {KAFKA_BROKER}...")
    for attempt in range(1, max_retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                # Automatically serialize Python dictionaries into JSON strings
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print("Connected successfully to Kafka!")
            return producer
        except Exception as e:
            print(f"Attempt {attempt}/{max_retries} failed to connect to Kafka: {e}")
            if attempt < max_retries:
                print(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
                
    raise ConnectionError("Could not establish connection with Kafka broker. Is it running?")

def simulate_metrics(inject_anomaly=False):
    """
    Generates simulated server metrics.
    If inject_anomaly is True, injects high-impact outliers (spikes).
    """
    timestamp = datetime.now().isoformat()

    if not inject_anomaly:
        # ---- Normal healthy behavior (following realistic statistical distributions) ----
        # CPU: centered around 40%, ranging mostly from 20% to 60%
        cpu_usage = float(np.clip(np.random.normal(loc=40.0, scale=10.0), 0.0, 100.0))
        # Memory: centered around 50%, stable
        memory_usage = float(np.clip(np.random.normal(loc=50.0, scale=8.0), 0.0, 100.0))
        # Latency: log-normal distribution, centered around 120ms with occasional minor variance
        request_latency = float(np.clip(np.random.lognormal(mean=4.8, sigma=0.25), 10.0, 1000.0))
        # Error Rate: heavily skewed towards 0 (normal rate < 2%)
        error_rate = float(np.clip(np.random.beta(a=1, b=20) * 0.1, 0.0, 1.0))
    else:
        # ---- Anomalous behavior (Simulating distinct types of server failures) ----
        anomaly_type = random.choice(["cpu_spike", "memory_leak", "latency_storm", "error_burst"])
        print(f"\n🚨 [Simulation] INJECTING ANOMALY TYPE: {anomaly_type.upper()} 🚨")

        if anomaly_type == "cpu_spike":
            # CPU spikes near max capacity, others remain slightly elevated
            cpu_usage = float(random.uniform(90.0, 100.0))
            memory_usage = float(random.uniform(60.0, 80.0))
            request_latency = float(random.uniform(500.0, 800.0))
            error_rate = float(random.uniform(0.05, 0.15))
            
        elif anomaly_type == "memory_leak":
            # Memory usage hits critical levels (95-100%)
            cpu_usage = float(random.uniform(50.0, 75.0))
            memory_usage = float(random.uniform(95.0, 100.0))
            request_latency = float(random.uniform(200.0, 400.0))
            error_rate = float(random.uniform(0.01, 0.05))
            
        elif anomaly_type == "latency_storm":
            # 10x spike in request latency (e.g. database thread pool exhausted)
            cpu_usage = float(random.uniform(30.0, 50.0))
            memory_usage = float(random.uniform(45.0, 60.0))
            request_latency = float(random.uniform(1200.0, 2000.0))  # 10x standard latency
            error_rate = float(random.uniform(0.10, 0.30))  # elevated timeouts
            
        else: # "error_burst"
            # Massive surge in HTTP 5xx errors (e.g. microservice down)
            cpu_usage = float(random.uniform(30.0, 50.0))
            memory_usage = float(random.uniform(40.0, 60.0))
            request_latency = float(random.uniform(150.0, 300.0))
            error_rate = float(random.uniform(0.70, 0.98))  # 70% to 98% error rates

    # Pack metrics into JSON envelope
    return {
        "timestamp": timestamp,
        "cpu_usage": round(cpu_usage, 2),
        "memory_usage": round(memory_usage, 2),
        "request_latency": round(request_latency, 2),
        "error_rate": round(error_rate, 4)
    }

def run_producer():
    """
    Main loop that connects to Kafka and streams metrics.
    Automatically schedules periodic anomaly injections.
    """
    try:
        producer = create_kafka_producer()
    except Exception as e:
        print(f"Fatal error initializing producer: {e}")
        return

    print(f"Successfully streaming metrics to Kafka topic '{KAFKA_TOPIC}'...")
    print("Metrics are pushed every 2 seconds. Press Ctrl+C to terminate.")

    # Schedule the first anomaly
    # Since we sleep 2s between metrics, 15 to 30 iterations = 30 to 60 seconds
    iterations_to_anomaly = random.randint(15, 30)
    print(f"[Simulation] Next anomaly planned in {iterations_to_anomaly * 2} seconds.")

    try:
        while True:
            iterations_to_anomaly -= 1
            
            # Check if it's time to inject an anomaly
            inject = (iterations_to_anomaly <= 0)
            
            # Generate the simulated metric payload
            payload = simulate_metrics(inject_anomaly=inject)
            
            # Reset anomaly timer if we just injected one
            if inject:
                iterations_to_anomaly = random.randint(15, 30)
                print(f"[Simulation] Anomaly injected. Next scheduled in {iterations_to_anomaly * 2} seconds.")

            # Send payload to Kafka
            producer.send(KAFKA_TOPIC, payload)
            
            # Output message preview to console for debugging
            status_label = "🚨 ANOMALY" if inject else "✅ NORMAL "
            print(f"{status_label} | {payload['timestamp']} | "
                  f"CPU: {payload['cpu_usage']}% | "
                  f"Mem: {payload['memory_usage']}% | "
                  f"Latency: {payload['request_latency']}ms | "
                  f"Error: {payload['error_rate']*100:.2f}%")
            
            time.sleep(2)
            
    except KeyboardInterrupt:
        print("\nStopping metric simulation producer...")
    finally:
        # Flush buffer and close socket connection safely
        producer.close()
        print("Kafka connection closed.")

if __name__ == "__main__":
    run_producer()
