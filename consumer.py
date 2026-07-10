import os
import sys
import json
import time
from dotenv import load_dotenv
from kafka import KafkaConsumer

# Import application modules
from model.detector import detect
from database.db import insert_metric
from alerts.notifier import trigger_alert

# Load configurations
load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "server-metrics")

def create_kafka_consumer():
    """
    Attempts to initialize and connect to the Kafka broker.
    Retries automatically if Kafka is still spinning up.
    """
    consumer = None
    max_retries = 10
    retry_delay = 5  # seconds
    
    print(f"Connecting to Kafka broker at: {KAFKA_BROKER}...")
    for attempt in range(1, max_retries + 1):
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=[KAFKA_BROKER],
                auto_offset_reset='latest',  # Read new messages only
                enable_auto_commit=True,     # Periodically commit offset
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )
            print(f"Connected successfully to Kafka topic '{KAFKA_TOPIC}'!")
            return consumer
        except Exception as e:
            print(f"Attempt {attempt}/{max_retries} failed to connect to Kafka: {e}")
            if attempt < max_retries:
                print(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
                
    raise ConnectionError("Could not establish connection with Kafka broker. Is it running?")

def run_consumer():
    """
    Main loop to consume server metric events, detect anomalies,
    save results to the database, and trigger alerts.
    """
    try:
        consumer = create_kafka_consumer()
    except Exception as e:
        print(f"Fatal error initializing consumer: {e}")
        return

    print("Kafka Consumer is listening for metric events. Press Ctrl+C to terminate...")
    
    try:
        for message in consumer:
            payload = message.value
            
            # Extract metrics from the payload
            timestamp = payload.get("timestamp")
            cpu = payload.get("cpu_usage")
            memory = payload.get("memory_usage")
            latency = payload.get("request_latency")
            error_rate = payload.get("error_rate")
            
            # Guard check to ensure all required fields are present
            if None in (timestamp, cpu, memory, latency, error_rate):
                print(f"[Consumer] Warning: Skipped malformed event message: {payload}")
                continue
            
            # Run model scoring
            try:
                is_anomaly, anomaly_score = detect(cpu, memory, latency, error_rate)
            except Exception as e:
                print(f"[Consumer] Error scoring metrics: {e}")
                continue
                
            # Log results to the SQLite database
            try:
                insert_metric(timestamp, cpu, memory, latency, error_rate, is_anomaly, anomaly_score)
            except Exception as e:
                print(f"[Consumer] Error saving metrics to database: {e}")
                
            # Print feedback to console
            status_label = "🚨 ANOMALY" if is_anomaly else "✅ NORMAL "
            print(f"{status_label} | {timestamp} | CPU: {cpu}% | Mem: {memory}% | Latency: {latency}ms | Err: {error_rate*100:.2f}% | Score: {anomaly_score:.4f}")
            
            # Dispatch alerts if an anomaly is detected
            if is_anomaly:
                metric_summary = f"CPU: {cpu}%, Memory: {memory}%, Latency: {latency}ms, Error Rate: {error_rate * 100:.2f}%"
                try:
                    trigger_alert(metric_summary, anomaly_score)
                except Exception as e:
                    print(f"[Consumer] Error sending alert: {e}")
                    
    except KeyboardInterrupt:
        print("\nStopping metric consumer daemon...")
    finally:
        if consumer:
            consumer.close()
            print("Kafka connection closed.")

if __name__ == "__main__":
    run_consumer()
