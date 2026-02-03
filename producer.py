import time
import json
import requests
import random
from kafka import KafkaProducer

# Kafka settings
KAFKA_TOPIC = "printer_raw_topic"
KAFKA_BOOTSTRAP = "127.0.0.1:9092"  # for host machine

MIN_INTERVAL_SECONDS = 0.01
MAX_INTERVAL_SECONDS = 60
BLOCK_SECONDS = 60

# API Endpoint
API_URL = "http://127.0.0.1:5000/getPrinterStatus"

def create_producer(KAFKA_BOOTSTRAP=KAFKA_BOOTSTRAP) -> KafkaProducer:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        retries=3
    )
    return producer

def fetch_printer_data():
    try:
        response = requests.get(API_URL, timeout=5)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"[ERROR] API fetch failed: {e}")
        return []

def send_to_kafka(producer, topic, records):
    # Normalize records: API may return a single dict or a list of dicts.
    if isinstance(records, dict):
        records = [records]
    elif isinstance(records, (str, bytes)):
        # treat raw string/bytes as a single record
        records = [records]

    try:
        size = len(records)
    except Exception:
        size = "unknown"
    print(f"send_to_kafka: sending {size} records (type={type(records)})")

    for record in records:
        producer.send(topic, record)
        print(f"Sent record: {record}")
    producer.flush()

def run_loop(interval_seconds=5.0):
    producer = create_producer()
    print("Producer started, sending data every", interval_seconds, "secs")

    i=0
    while i<5:
        data = fetch_printer_data()
        if data:
            print(f"Fetched {len(data)} records, sending to Kafka...")
            send_to_kafka(producer, KAFKA_TOPIC, data)
        else:
            print("No data fetched.")

        time.sleep(interval_seconds)


def run_loop_test(
    fast_sleep=0.01,
    slow_sleep=10,
    block_seconds=60
    ):
    producer = create_producer()
    print("Producer started test mode")

    start_time = time.time()

    while True:
        elapsed = time.time() - start_time
        block_index = int(elapsed // block_seconds)

        # Alternate every block
        interval_seconds = fast_sleep if block_index % 2 == 0 else slow_sleep

        data = fetch_printer_data()
        if data:
            print(f"Fetched {len(data)} records, sending to Kafka...")
            send_to_kafka(producer, KAFKA_TOPIC, data)
        else:
            print("No data fetched.")

        time.sleep(interval_seconds)

if __name__ == "__main__":
    run_loop_test(
        fast_sleep=MIN_INTERVAL_SECONDS,
        slow_sleep=MAX_INTERVAL_SECONDS,
        block_seconds=BLOCK_SECONDS   
    )
    #run_loop(interval_seconds=random.uniform(MIN_INTERVAL_SECONDS, MAX_INTERVAL_SECONDS))
    #run_loop(interval_seconds=0.016667)  # for testing purposes)