import os
import logging
import pandas as pd
import producer as kafka_producer
import cleaner as data_cleaner

# Kafka settings
KAFKA_TOPIC = "no_spam_topic"
KAFKA_TOPIC2 = "stall_detection_topic"
KAFKA_BOOTSTRAP = "127.0.0.1:9092"  # for host machine
WINDOW_TIME_MINUTES = 1 # Change to 5 minutes later

GROUP_ID = "stall_detection_group"

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
log = logging.getLogger("stall_detection")


def process_cleaner():
    # Create Kafka producer for output topic (alerts)
    producer = kafka_producer.create_producer(KAFKA_BOOTSTRAP)

    # Create Kafka consumer for input topic (cleaned printer status)
    consumer = data_cleaner.create_consumer(KAFKA_TOPIC, KAFKA_BOOTSTRAP, group_id=GROUP_ID)
    log.info("Stall detector started, listening for processed printer status messages...")

    # Per-printer stall state
    stalls = {}

    for message in consumer:
        try:
            log.info(f"Received message: {message.value}")
            message_data = message.value

            # message_data is expected to be the cleaned record (from processedPrinterStatus_topic)
            pid = message_data.get("printerId")
            if pid is None:
                log.warning("Message missing printerId; skipping")
                continue

            ts_raw = message_data.get("timestamp")
            ts = pd.to_datetime(ts_raw, errors="coerce", utc=True)
            if pd.isna(ts):
                ts = pd.Timestamp.utcnow()

            ps = message_data.get("printerStatus") or {}
            err = ps.get("error")
            rl = ps.get("remainingLabels")
            try:
                rl = None if rl is None else int(rl)
            except Exception:
                rl = None

            state = stalls.get(pid)

            if err == 'JAM':
                # Start observing if we have no state
                if state is None:
                    stalls[pid] = {
                        'state': 'observing',
                        'first_seen': ts,
                        'initial_remaining': rl,
                        'last_remaining': rl,
                        'last_ts': ts,
                    }
                    log.info(f"Observed JAM for {pid}: first_seen={stalls[pid]['first_seen']}, remainingLabels={rl}")
                    continue

                # Update last seen
                state['last_ts'] = ts
                state['last_remaining'] = rl

                # If baseline missing and we now have numeric rl, set baseline
                if state['initial_remaining'] is None and rl is not None:
                    state['initial_remaining'] = rl
                    state['first_seen'] = ts
                    log.info(f"Established baseline remainingLabels={rl} for {pid} at {ts}")
                    stalls[pid] = state
                    continue

                # Recovery: remaining decreased
                if state['initial_remaining'] is not None and rl is not None and rl < state['initial_remaining']:
                    log.info(f"Recovery detected for {pid}: remainingLabels decreased {state['initial_remaining']} -> {rl}. Clearing state.")
                    stalls.pop(pid, None)
                    continue

                # If already alerted, do nothing
                if state['state'] == 'alerted':
                    continue

                # Check if 5 minute window elapsed
                elapsed = ts - state['first_seen']
                if elapsed >= pd.Timedelta(minutes=WINDOW_TIME_MINUTES):
                    alert = {
                        'printerId': pid,
                        'alert_type': 'CRITICAL_STALL',
                        'first_seen': state['first_seen'].isoformat(),
                        'detection_time': ts.isoformat(),
                        'initial_remainingLabels': state['initial_remaining'],
                        'last_remainingLabels': rl,
                        'message': 'No decrease in remainingLabels for 5 minutes while error=JAM',
                    }
                    log.critical(f"Emitting stall alert for {pid}: {alert}")

                    producer.send(KAFKA_TOPIC2, alert, key=pid.encode('utf-8'), headers=[
                        ('timestamp', ts.isoformat().encode('utf-8'))
                    ])

                    producer.flush()
                    state['state'] = 'alerted'
                    stalls[pid] = state
                    continue

            else:
                # Clear state if printer not jammed
                if state is not None:
                    log.info(f"Clearing stall state for {pid} because error is {err}")
                    stalls.pop(pid, None)
                continue

        except Exception:
            log.exception("Error in stall detection loop; continuing")
        


if __name__ == "__main__":
    process_cleaner()   
