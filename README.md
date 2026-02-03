# 👻 The Ghost Printer Orchestrator

An event-driven automation engine that ingests **noisy printer telemetry** from a simulator, **cleans & normalizes** it in real time, persists it into a **NoSQL database**, and runs **rule-based alerting with anti-spam protection**.

This project was built for the **Ghost Printer challenge** and demonstrates a practical streaming architecture using **Kafka**, **Python**, and **MongoDB**.

---

## 🧠 What this project does

The simulator produces messy, duplicated, and inconsistent printer status JSON.

This orchestrator:

1. Polls the simulator API
2. Streams raw events into Kafka
3. Consumes the stream and **cleans / normalizes** the data
4. Deduplicates noisy messages
5. Stores time-bounded logs with TTL
6. Applies **anti-spam protection**
7. Runs **dynamic alert rules** (e.g., printer jam detection)
8. Persists alerts and maintenance signals

---

## 🏗️ Architecture Overview

```
PrintSimulator.exe  -->  producer.py  -->  Kafka (printSimulatorAPI topic)
                                           |
                                           v
                                     consumer.py
                                           |
                     -----------------------------------------
                     |                   |                   |
               Normalization        Anti-spam logic      Rule engine
                     |                   |                   |
                     -----------------------------------------
                                           |
                                           v
                                       MongoDB
```

---

## 📦 Components

| File                      | Responsibility                                                       |
| ------------------------- | -------------------------------------------------------------------- |
| `producer.py`             | Polls the simulator API and sends messages to Kafka                  |
| `consumer.py`             | Consumes Kafka topic, cleans data, dedupes, runs rules, writes to DB |
| `main.py`                 | Example streaming structure using `quixstreams`                      |
| `docker-compose.yml`      | Kafka, Zookeeper and Kafka-UI setup                                  |
| `PrintSimulator/`         | Windows executable exposing `/getPrinterStatus`                      |
| *(suggested)* `db.py`     | MongoDB operations, TTL index, upserts                               |
| *(suggested)* `alerts.py` | Rule evaluation and alert emission                                   |

---

## 🔌 External Services

| Service       | Address                                        |
| ------------- | ---------------------------------------------- |
| Simulator API | `http://127.0.0.1:5000/getPrinterStatus`       |
| Kafka Broker  | `localhost:9092`                               |
| Kafka UI      | [http://localhost:8080](http://localhost:8080) |
| MongoDB       | `mongodb://localhost:27017`                    |

---

## ⚙️ How to run locally

### 1️⃣ Start Kafka stack

```bash
docker-compose up -d
```

Kafka UI will be available at:

```
http://localhost:8080
```

---

### 2️⃣ Start the Printer Simulator (Windows)

Run:

```
PrintSimulator\PrintSimulator.exe
```

Verify the API:

```
http://127.0.0.1:5000/getPrinterStatus
```

---

### 3️⃣ Install dependencies

```bash
pip install -r requirements.txt
```

---

### 4️⃣ Run the Producer

```bash
python producer.py
```

You should now see messages arriving in the `printSimulatorAPI` topic via Kafka UI.

---

### 5️⃣ Run the Consumer / Processor

```bash
python consumer.py
```

This is where the core logic happens:

* Cleaning
* Deduplication
* Anti-spam
* Persistence
* Rule evaluation

---

## 🧹 Data Normalization Rules

The simulator sends inconsistent values. The processor enforces:

* Numeric strings → integers
* Consistent schema:

  * `printerId`
  * `timestamp`
  * `status`
  * `remainingLabels`
  * `error`
* Composite dedupe key: `{printerId, timestamp}`

Example:

```python
record['remainingLabels'] = int(record.get('remainingLabels') or 0)
record['error'] = record.get('printerStatus', {}).get('error')
```

---

## 🛑 Anti-Spam Protection

If a single printer sends **>10 messages within 1 second**:

* The burst is dropped
* A record is inserted into `maintenance_required` collection

```
{ printerId, first_seen }
```

---

## 🚨 Rule Engine & Alerts

Rules are **data-driven** and stored in MongoDB so they can be changed without code updates.

### Example rule: Stall Detection

If:

* `error == 'JAM'`
* `remainingLabels` does not decrease for **5 minutes**

Then:

→ Emit `critical_alert`

Example rule document:

```json
{
  "name": "stall_detection",
  "condition": {"error":"JAM"},
  "window_seconds": 300,
  "check": "remainingLabels_not_decreasing"
}
```

Alerts are written to the `alerts` collection.

---

## 🗃️ MongoDB Collections

| Collection             | Purpose                                           |
| ---------------------- | ------------------------------------------------- |
| `printer_logs`         | Cleaned, normalized printer history (TTL enabled) |
| `maintenance_required` | Anti-spam signals                                 |
| `alerts`               | Rule-triggered alerts                             |

TTL example:

```python
collection.create_index('timestamp', expireAfterSeconds=604800)
```

(7 days retention)

---

## 🧪 Debugging Tips

* Use Kafka UI to inspect raw messages
* If no data appears:

  * Check simulator is running
  * Check producer logs for API errors
* Verify MongoDB TTL index exists
* Reprocessing the same Kafka message should **not** create duplicates (idempotent design)

---

## 🧭 Design Principles

* Deterministic transformations
* Idempotent processing
* Explicit type normalization
* Data-driven rules
* Clear separation between ingestion, processing, and alerting

---

## 🛠️ Tech Stack

* Python
* Kafka + Zookeeper (Docker)
* MongoDB
* QuixStreams (stream processing pattern)
* Requests / Kafka-Python / Pandas

---

## ✅ What this demonstrates

This project showcases:

* Real-time stream processing
* Handling dirty telemetry data
* Event deduplication
* Sliding window anti-spam logic
* Dynamic rule engine design
* TTL-based log retention
* Clean, production-style streaming architecture

---

## 📎 Topic Name

```
printSimulatorAPI
```

---

## ✍️ Author

Eduardo Cunha
