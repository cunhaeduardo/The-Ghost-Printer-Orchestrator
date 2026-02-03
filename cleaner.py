import os
from pydoc import doc
import time
import json
import logging
import datetime
import pandas as pd
from kafka import KafkaConsumer
import producer as kafka_producer

# Kafka settings
KAFKA_TOPIC = "printer_raw_topic"
KAFKA_TOPIC2 = "printer_cleaned_topic"
KAFKA_BOOTSTRAP = "127.0.0.1:9092"  # for host machine

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
log = logging.getLogger("cleaner")


def create_consumer(topic: str, bootstrap: str, group_id: str) -> KafkaConsumer:
    """Create a KafkaConsumer that decodes JSON messages (JSON over UTF-8)."""
    return KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id=group_id,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    )

def extract_printer_record(msg: dict) -> dict:
    """Extracts normalized fields from a raw printer status message.

    The extractor is defensive and looks for common key variants.
    Returns a dict suitable for conversion to a pandas DataFrame.
    """
    printerId = msg.get("printerId") 
    timestamp = msg.get("timestamp")

    # Firmware details might be at several keys depending on producer
    firmwareDetails = msg.get("firmwareDetails") or {}
    # prefer top-level `printerStatus` when the producer includes it there; otherwise fall back
    # to firmwareDetails.printerStatus for older message shapes
    printerStatus = msg.get("printerStatus") or firmwareDetails.get("printerStatus", {}) or {}

    # Normalize remainingLabels inside printerStatus (keep the nested structure only)
    try:
        rl = printerStatus.get("remainingLabels")
        printerStatus["remainingLabels"] = int(rl) if rl is not None else None
    except Exception:
        printerStatus["remainingLabels"] = None

    # Normalize timestamp to pandas datetime when possible
    ts = None
    if timestamp is not None:
        try:
            # coerce invalid or empty timestamps to NaT rather than raising
            ts = pd.to_datetime(timestamp, errors="coerce")
        except Exception:
            ts = timestamp

    # Return a normalized record keeping nested `printerStatus` only (no duplicated top-level fields)
    return {
        "printerId": printerId,
        "timestamp": ts,
        "printerStatus": printerStatus,
    }


def process_cleaner():
    # Create Kafka producer for output topic
    producer = kafka_producer.create_producer(KAFKA_BOOTSTRAP)

    # Create Kafka consumer for input topic
    consumer = create_consumer(KAFKA_TOPIC, KAFKA_BOOTSTRAP, group_id="printSimulatorAPI_group")
    log.info("Cleaner started, listening for messages...")
    
    for message in consumer:
        log.info(f"Received message: {message.value}")
        message_data = message.value

        # Create a normalized record
        record = extract_printer_record(message_data)
        

        ts = record.get("timestamp")
        # Normalize timestamp to an ISO 8601 string (JSON-friendly) or None
        if isinstance(ts, pd.Timestamp):
            if pd.isna(ts):
                record["timestamp"] = None
            else:
                record["timestamp"] = ts.to_pydatetime().isoformat()
        elif isinstance(ts, datetime.datetime):
            record["timestamp"] = ts.isoformat()
        elif isinstance(ts, str):
            parsed = pd.to_datetime(ts, errors="coerce")
            record["timestamp"] = None if pd.isna(parsed) else parsed.to_pydatetime().isoformat()
        else:
            # Fallback: ensure value is a JSON-serializable string or None
            record["timestamp"] = None if ts is None else str(ts)

        # Ensure nested printerStatus.remainingLabels is an int or None
        ps = record.get("printerStatus") or {}
        rl = ps.get("remainingLabels")
        try:
            if rl is None:
                ps["remainingLabels"] = None
            else:
                ps["remainingLabels"] = int(rl)
        except Exception:
            # if coercion fails, set to None
            ps["remainingLabels"] = None
        record["printerStatus"] = ps

        # Send cleaned record to output Kafka topic
        log.info(f"Prepared cleaned record: {record}")

        # Safely build key and headers (don't call .encode on None)
        key_val = record.get("printerId")
        key_bytes = key_val.encode("utf-8") if isinstance(key_val, str) else None

        ts_val = record.get("timestamp")
        headers = []
        if ts_val is not None:
            ts_bytes = ts_val.encode("utf-8") if isinstance(ts_val, str) else str(ts_val).encode("utf-8")
            headers.append(("timestamp", ts_bytes))

        log.info(f"Sending to {KAFKA_TOPIC2}: key={key_val} headers={headers}")
        producer.send(KAFKA_TOPIC2, record, key=key_bytes, headers=headers)
        producer.flush()
        

if __name__ == "__main__":
    process_cleaner()   
