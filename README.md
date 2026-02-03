# 👻 The Ghost Printer Orchestrator

An event-driven automation engine that ingests **noisy printer telemetry** from a simulator, **cleans & normalizes** it in real time, persists it into a **NoSQL database**, and runs **rule-based alerting with anti-spam protection**.

This project was built for the **Ghost Printer challenge** and demonstrates a practical streaming architecture using **Kafka**, **Python**, and **MongoDB**.

---

## 🧠 What this project does

The simulator produces messy, duplicated, and inconsistent printer status JSON.

This orchestrator:

1. Streams raw events into Kafka
2. Cleans and normalizes inconsistent data
3. Detects and blocks printer spam bursts
4. Persists only valid printer events
5. Stores maintenance signals separately
6. Runs stall detection rules in real time
7. Emits alerts back into Kafka

---

## 🏗️ Architecture Overview

```
PrintSimulator.exe
      │
      ▼
  producer.py
      │
      ▼
Kafka: printer_raw_topic
      │
      ▼
  cleaner.py
      │
      ▼
Kafka: printer_cleaned_topic
      │
      ▼
  antispam.py
      │
      ├──────────────► Kafka: maintenance_required_topic
      │                         │
      │                         ▼
      │              mongodb_maintenance_db.py
      │                         │
      │                         ▼
      │        MongoDB: BloqIt_PrinterDB[maintenance_required]
      │
      ▼
Kafka: no_spam_topic
      │
      ├──────────────► stallDetection.py
      │                         │
      │                         ▼
      │             Kafka: stall_detection_topic (alerts)
      │
      ▼
mongodb_main_db.py
      │
      ▼
MongoDB: BloqIt_PrinterDB[no_spam_detection]
```

---

## 📦 Components

| File                        | Responsibility                                                       |
| --------------------------- | -------------------------------------------------------------------- |
| `producer.py`               | Sends raw simulator data to `printer_raw_topic`                      |
| `cleaner.py`                | Normalizes schema and republishes to `printer_cleaned_topic`         |
| `antispam.py`               | Detects burst spam and routes messages accordingly                   |
| `mongodb_main_db.py`        | Consumes `no_spam_topic` and persists valid events                   |
| `mongodb_maintenance_db.py` | Consumes `maintenance_required_topic` and persists maintenance flags |
| `stallDetection.py`         | Consumes `no_spam_topic` and emits alerts to `stall_detection_topic` |

---

## 🔌 External Services

| Service       | Address                                        |
| ------------- | ---------------------------------------------- |
| Simulator API | `http://127.0.0.1:5000/getPrinterStatus`       |
| Kafka Broker  | `localhost:9092`                               |
| Kafka UI      | [http://localhost:8080](http://localhost:8080) |
| MongoDB       | `mongodb://localhost:27017`                    |

---

## 🔄 Kafka Topics

| Topic                        | Produced by         | Consumed by                               | Purpose              |
| ---------------------------- | ------------------- | ----------------------------------------- | -------------------- |
| `printer_raw_topic`          | `producer.py`       | `cleaner.py`                              | Raw simulator output |
| `printer_cleaned_topic`      | `cleaner.py`        | `antispam.py`                             | Normalized data      |
| `no_spam_topic`              | `antispam.py`       | `mongodb_main_db.py`, `stallDetection.py` | Valid printer events |
| `maintenance_required_topic` | `antispam.py`       | `mongodb_maintenance_db.py`               | Spam burst detection |
| `stall_detection_topic`      | `stallDetection.py` | Alert/monitoring systems                  | Critical alerts      |

---

## 🧹 Data Normalization

`cleaner.py` enforces a consistent structure:

* Converts numeric strings → integers
* Extracts nested error fields
* Guarantees consistent keys:

  * `printerId`
  * `timestamp`
  * `status`
  * `remainingLabels`
  * `error`

This ensures downstream processors operate on deterministic data.

---

## 🛑 Anti-Spam Logic

Implemented in `antispam.py`.

If a printer sends **more than 10 messages within 1 second**:

* One message forward to main flow, others are dropped
* A record is sent to `maintenance_required_topic`
* Cool down system is activated per X seconds.

Otherwise, messages continue normally to `no_spam_topic`.

---

## ⏳ Data Retention Strategy (Kafka, not MongoDB)

This project **does not use MongoDB TTL indexes**.

Instead, **Kafka topics themselves are configured** to automatically delete old data after **1 day** using topic-level configuration:

* `cleanup.policy=delete`
* `retention.ms=86400000` (24 hours)

This means Kafka acts as the **time-bounded event store**, and consumers only process recent data.

This configuration follows Kafka’s official topic configuration guidelines:
[https://kafka.apache.org/30/generated/topic_config.html](https://kafka.apache.org/30/generated/topic_config.html)

---

## 🚨 Stall Detection Rule

`stallDetection.py` consumes messages directly from:

```
no_spam_topic
```

Rule example:

> If `error == "JAM"` and `remainingLabels` does not decrease for X minutes → emit alert

The alert is published to:

```
stall_detection_topic
```

MongoDB is **not** the source for this rule — Kafka is.

---

## ⚙️ How to run locally

### 1️⃣ Start Kafka stack

```bash
docker-compose up -d
```

Kafka UI: [http://localhost:8080](http://localhost:8080)

---

### 2️⃣ Start the Printer Simulator (Windows)

```
PrintSimulator\PrintSimulator.exe
```

Test:

```
http://127.0.0.1:5000/getPrinterStatus
```

---

### 3️⃣ Install dependencies

```bash
pip install -r requirements.txt
```

---

## ▶️ Start the processors (important order)

Open **separate terminals** and start **everything except the producer**:

```bash
python cleaner.py
python antispam.py
python mongodb_main_db.py
python mongodb_maintenance_db.py
python stallDetection.py
```

These will wait for messages.

---

## ▶️ Start the data flow (last step)

Finally, start the producer:

```bash
python producer.py
```

You will now see the full pipeline operating through Kafka and MongoDB.

---

## 🧪 Debugging Tips

* Use Kafka UI to inspect topics and message flow
* If no messages appear:

  * Check simulator is running
  * Check producer logs
* Restarting processors is safe (idempotent consumers)

---

## 🛠️ Tech Stack

* Python
* Kafka + Zookeeper (Docker)
* MongoDB
* Requests / Kafka-Python / Pandas

---

## ✍️ Author

Eduardo Cunha
