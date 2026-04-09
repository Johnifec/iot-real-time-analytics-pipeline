import os
import json
import time
import random
from datetime import datetime, timezone

from kafka import KafkaProducer


# Environment configuration

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "iot-pipeline")
SEND_INTERVAL = float(os.getenv("SEND_INTERVAL", "1"))  # seconds


# Kafka Producer

def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        retries=5,
    )


# Data Generation

def generate_data():
    """
    Simulate IoT sensor data with occasional anomalies.
    """
    is_anomaly = random.random() < 0.05  # 5% anomaly rate

    if is_anomaly:
        pressure = random.uniform(120, 150)
        temperature = random.uniform(80, 100)
        vibration = random.uniform(0.08, 0.2)
    else:
        pressure = random.uniform(60, 100)
        temperature = random.uniform(40, 70)
        vibration = random.uniform(0.01, 0.05)

    return {
       "timestamp": datetime.utcnow().isoformat(),
        "pipeline_id": "pipe_1",
        "pressure": round(pressure, 2),
        "temperature": round(temperature, 2),
        "vibration": round(vibration, 3),
    }


# Main Loop

def run():
    producer = create_producer()
    print(f"Starting producer on {KAFKA_BROKER}, topic '{TOPIC}'...")

    try:
        while True:
            data = generate_data()
            producer.send(TOPIC, data)
            print("Sent:", data)
            time.sleep(SEND_INTERVAL)

    except KeyboardInterrupt:
        print("\nStopping producer...")

    finally:
        producer.flush()
        producer.close()
        print("Producer closed.")


# Entry Point
if __name__ == "__main__":
    run()