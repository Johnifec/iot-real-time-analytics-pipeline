import os
import statistics
from datetime import datetime

import faust
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS


# Environment configuration

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "iot-pipeline")

INFLUX_URL = os.getenv("INFLUX_URL", "http://localhost:8086")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN", "admin123")
INFLUX_ORG = os.getenv("INFLUX_ORG", "my-org")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET", "iot_data")

WINDOW_SIZE = int(os.getenv("WINDOW_SIZE", "20"))
MIN_OBSERVATIONS = int(os.getenv("MIN_OBSERVATIONS", "6"))


# Faust application

app = faust.App(
    "iot-app",
    broker=f"kafka://{KAFKA_BROKER}",
    value_serializer="json",
)

topic = app.topic(KAFKA_TOPIC)


# InfluxDB client

client = InfluxDBClient(
    url=INFLUX_URL,
    token=INFLUX_TOKEN,
    org=INFLUX_ORG,
)
write_api = client.write_api(write_options=SYNCHRONOUS)


# Rolling windows for recent sensor values

pressure_window = []
temperature_window = []
vibration_window = []


def update_window(window_list, value, max_size):
    """Append a new value and keep only the most recent max_size values."""
    window_list.append(value)
    if len(window_list) > max_size:
        window_list.pop(0)


def detect_anomaly(current_value, values_window):
    """
    Detect anomaly using a simple z-score style rule:
    value > mean + 3 * standard deviation

    Returns:
        int: 1 if anomaly detected, otherwise 0
    """
    if len(values_window) < MIN_OBSERVATIONS:
        return 0

    avg_value = statistics.mean(values_window)
    std_value = statistics.stdev(values_window)

    if std_value == 0:
        return 0

    return int(current_value > avg_value + 3 * std_value)


@app.agent(topic)
async def process(stream):
    """
    Consume streaming sensor data from Kafka, compute rolling statistics,
    detect anomalies, and write results to InfluxDB.
    """
    async for event in stream:
        pressure = event["pressure"]
        temperature = event["temperature"]
        vibration = event["vibration"]
        timestamp = datetime.fromisoformat(event["timestamp"])

        # Update rolling windows
        update_window(pressure_window, pressure, WINDOW_SIZE)
        update_window(temperature_window, temperature, WINDOW_SIZE)
        update_window(vibration_window, vibration, WINDOW_SIZE)

        # Wait until enough observations are available
        if len(pressure_window) < MIN_OBSERVATIONS:
            continue

        # Per-sensor anomaly detection
        pressure_anomaly = detect_anomaly(pressure, pressure_window)
        temperature_anomaly = detect_anomaly(temperature, temperature_window)
        vibration_anomaly = detect_anomaly(vibration, vibration_window)

        any_anomaly = int(
            pressure_anomaly or temperature_anomaly or vibration_anomaly
        )

        # Write each sensor reading with anomaly flag
        for sensor_name, sensor_value, anomaly_flag in [
            ("pressure", pressure, pressure_anomaly),
            ("temperature", temperature, temperature_anomaly),
            ("vibration", vibration, vibration_anomaly),
        ]:
            point = (
                Point("sensor_data")
                .tag("sensor", sensor_name)
                .field("value", float(sensor_value))
                .field("anomaly", int(anomaly_flag))
                .time(timestamp)
            )
            write_api.write(bucket=INFLUX_BUCKET, record=point)

        app.logger.info("Processed event | Any anomaly: %s", any_anomaly)