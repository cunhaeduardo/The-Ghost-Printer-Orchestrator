import os
import logging
from collections import deque
from datetime import datetime, timezone, timedelta
import producer as kafka_producer
import cleaner as data_cleaner

# Kafka settings
KAFKA_TOPIC = "printer_cleaned_topic"
KAFKA_OUT = "no_spam_topic"  # forward non-spam messages here
KAFKA_MAINT = "maintenance_required_topic"  # where to log bursting printers
KAFKA_BOOTSTRAP = "127.0.0.1:9092"  # for host machine
GROUP_ID = "antispam_group"

# Anti-spam thresholds
MAX_MESSAGES_PER_WINDOW = 10  # flag when >= this many messages are seen within WINDOW_SECONDS
WINDOW_SECONDS = 5  # 1 minute window (seconds)
# Duration to keep a printer suppressed after being flagged (seconds)
COOLDOWN_SECONDS = 60

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
log = logging.getLogger("no_spam_detection")


def _parse_timestamp(ts_raw):
    """Parse an ISO timestamp string into a timezone-aware datetime.
    Falls back to current UTC time if parsing fails or value is missing.
    """
    try:
        log.debug(f"Parsing timestamp: {ts_raw}")
        return datetime.fromisoformat(ts_raw.replace("Z", "+00:00"))
    except Exception:
        log.warning(f"Failed to parse timestamp: {ts_raw}")
        return datetime.now(timezone.utc)


def process_cleaner():
    """Buffer messages per printer for 1 minute and apply anti-spam rules.

    Behavior:
    - Buffer incoming messages per printer (dict -> list of (ts, message)).
    - A buffered message is 'ready' when it is at least WINDOW_SECONDS old (based on current time).
    - When a buffered window is ready, count how many messages fall within [oldest_ts, oldest_ts + WINDOW_SECONDS].
      - If count > MAX_MESSAGES_PER_WINDOW: emit one maintenance alert with first timestamp and count, send a single representative
        snapshot to `KAFKA_OUT` (so we still have one cleaned state), drop the buffered messages in that window, and start cooldown.
      - Otherwise: forward the oldest buffered message to `KAFKA_OUT` (messages are forwarded only after they become older than WINDOW_SECONDS).
    """

    producer = kafka_producer.create_producer(KAFKA_BOOTSTRAP)
    consumer = data_cleaner.create_consumer(KAFKA_TOPIC, KAFKA_BOOTSTRAP, group_id=GROUP_ID)
    log.info("Anti-spam processor started, listening for cleaned printer status messages...")

    # buffer: pid -> list of (timestamp: datetime, message)
    buffer = {}
    cooldowns = {}  # pid -> datetime when cooldown ends

    for message in consumer:
        try:
            message_value = message.value if isinstance(message.value, dict) else {}
            pid = message.key.decode('utf-8') if message.key else message_value.get('printerId')
            if not pid:
                log.debug("Skipping message without printerId")
                continue

            # decode headers safely
            headers_dict = dict((k, v.decode('utf-8') if isinstance(v, bytes) else v) for k, v in message.headers) if message.headers else {}
            ts_raw = headers_dict.get('timestamp') or message_value.get('timestamp')
            if not ts_raw:
                log.warning(f"Message for printer {pid} missing timestamp; using current time")
            ts = _parse_timestamp(ts_raw)

            # check cooldown
            if pid in cooldowns:
                if ts < cooldowns[pid]:
                    log.info(f"Printer {pid} in cooldown until {cooldowns[pid].isoformat()}, skipping message")
                    continue
                else:
                    log.info(f"Cooldown expired for printer {pid}, resuming processing")
                    del cooldowns[pid]

            # Append to buffer using dict keyed by ISO timestamp string to match requested structure
            buf = buffer.setdefault(pid, {})
            ts_key = ts.isoformat()
            # ensure uniqueness for keys by appending an index if necessary
            idx = 0
            unique_key = ts_key
            while unique_key in buf:
                idx += 1
                unique_key = f"{ts_key}-{idx}"
            buf[unique_key] = message

            # Now process ready items (oldest timestamps at least WINDOW_SECONDS old)
            now = datetime.now(timezone.utc)

            # helper to get datetime from key
            def _dt_from_key(k):
                # extract the original ISO timestamp even if we appended a '-n' suffix for uniqueness
                if '-' in k:
                    left, right = k.rsplit('-', 1)
                    # suffix is numeric when we added it; otherwise it's part of the timestamp
                    base = left if right.isdigit() else k
                else:
                    base = k
                return _parse_timestamp(base)

            # while there is an oldest item that is ready, process it
            while buf:
                # find oldest key
                oldest_key = min(buf.keys(), key=lambda k: _dt_from_key(k))
                oldest_dt = _dt_from_key(oldest_key)
                if (now - oldest_dt).total_seconds() < WINDOW_SECONDS:
                    # oldest item not old enough yet
                    break

                # define time window
                window_start = oldest_dt
                window_end = window_start + timedelta(seconds=WINDOW_SECONDS)

                # collect items within this window
                window_keys = [k for k in buf.keys() if _dt_from_key(k) >= window_start and _dt_from_key(k) <= window_end]
                count = len(window_keys)

                if count > MAX_MESSAGES_PER_WINDOW:
                    # Burst detected
                    alert = {
                        'printerId': pid,
                        'first_seen': window_start.isoformat(),
                        'reason': 'spamming',
                        'count': count,
                    }
                    log.info(f"Flagging {pid} for spamming: {count} messages between {window_start.isoformat()} and {window_end.isoformat()}")

                    key_bytes = pid.encode('utf-8') if isinstance(pid, str) else None
                    # prefer most recent message in the window as representative
                    latest_key = max(window_keys, key=lambda k: _dt_from_key(k))
                    latest_msg = buf[latest_key]

                    # send alert
                    maint_headers = []
                    latest_hdrs = dict((k, v.decode('utf-8') if isinstance(v, bytes) else v) for k, v in getattr(latest_msg, 'headers', [])) if getattr(latest_msg, 'headers', None) else {}
                    if latest_hdrs.get('timestamp'):
                        maint_headers.append(('timestamp', latest_hdrs.get('timestamp').encode('utf-8')))

                    producer.send(KAFKA_MAINT, alert, key=key_bytes, headers=maint_headers)
                    try:
                        producer.flush(timeout=10)
                    except Exception as e:
                        log.warning(f"Failed to flush maintenance alert: {e}")

                    # send a single snapshot (latest)
                    rep_hdrs = []
                    rep_ts = latest_hdrs.get('timestamp') or (latest_msg.value.get('timestamp') if isinstance(latest_msg.value, dict) else None)
                    if rep_ts:
                        rep_hdrs.append(('timestamp', rep_ts.encode('utf-8') if isinstance(rep_ts, str) else str(rep_ts).encode('utf-8')))
                    try:
                        producer.send(KAFKA_OUT, latest_msg.value, key=key_bytes, headers=rep_hdrs)
                        producer.flush(timeout=10)
                    except Exception as e:
                        log.warning(f"Failed to send representative snapshot to {KAFKA_OUT}: {e}")

                    # drop windowed keys
                    for k in window_keys:
                        buf.pop(k, None)

                    # if buffer empty remove pid
                    if not buf:
                        buffer.pop(pid, None)

                    # set cooldown
                    cooldowns[pid] = datetime.now(timezone.utc) + timedelta(seconds=COOLDOWN_SECONDS)
                    break

                else:
                    # Not a burst: forward the oldest buffered message
                    oldest_msg = buf.pop(oldest_key)
                    out_hdrs = dict((k, v.decode('utf-8') if isinstance(v, bytes) else v) for k, v in getattr(oldest_msg, 'headers', [])) if getattr(oldest_msg, 'headers', None) else {}
                    out_headers = []
                    if out_hdrs.get('timestamp'):
                        out_headers.append(('timestamp', out_hdrs.get('timestamp').encode('utf-8')))

                    try:
                        producer.send(KAFKA_OUT, oldest_msg.value, key=pid.encode('utf-8'), headers=out_headers)
                        producer.flush(timeout=10)
                    except Exception as e:
                        log.warning(f"Failed forwarding buffered message for {pid}: {e}")

                    # if buffer emptied, remove key
                    if not buf:
                        buffer.pop(pid, None)
                        break
        except Exception as e:
            log.error(f"Error in anti-spam processing: {e}", exc_info=True)

if __name__ == "__main__":
    # quick local sanity check
    process_cleaner()
