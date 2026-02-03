"""create_printer_collection.py (simplified)

Creates a MongoDB collection named `PrinterStatus_db` with a JSON Schema validator that
ensures the following fields and types (collection left empty — no inserts):

- printerId: string (required)
- timestamp: date (required)
- firmware: string or object (optional)
- printerStatus: string or object (optional)
- error: string or null (optional)
- remainingLabels: int or null (optional)

Usage: python create_printer_collection.py [--uri URI] [--db DB] [--collection NAME]
"""

import os
from numpy import record
import pymongo
import logging
import cleaner as data_cleaner
import antispam as no_spam

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
log = logging.getLogger("db_setup")

# MongoDB settings
DEFAULT_URI = os.getenv("MONGO_URI", "mongodb://nraboy:password1234@localhost:27017")
DB_NAME = "BloqIt_PrinterDB"
COL_MAINTENANCE = "maintenance_required_test"

# Kafka settings
KAFKA_TOPIC = "maintenance_required_topic"
KAFKA_BOOTSTRAP = "127.0.0.1:9092"  # for host machine
GROUP_ID = "maintenance_db_group"

def create_db(uri, db_name):
    pymongo_client = pymongo.MongoClient(uri)
    db = pymongo_client[db_name]
    return db

def check_db_exists(pymongo_client, db_name):
    dblist = pymongo_client.list_database_names()
    if db_name in dblist:
        return True
    return False    

def run_db():

    consumer = data_cleaner.create_consumer(KAFKA_TOPIC, KAFKA_BOOTSTRAP, group_id=GROUP_ID)
    log.info("Starting customer, listening for cleaned printer status messages...")

    for message in consumer:
        log.info(f"Received message for DB insertion: {message.value}")
        message_data = message.value
        pid = message.key.decode('utf-8') if message.key else message_data.get('printerId')
        if not pid:
            log.debug("Skipping message without printerId")
            continue
        ts_raw = message_data.get("timestamp")
        ts = no_spam._parse_timestamp(ts_raw)  
        ps = message_data.get("printerStatus") or {}
        err = ps.get("error")
        rl = ps.get("remainingLabels")
        try:
            rl = None if rl is None else int(rl)
        except Exception:
            rl = None
        record = {
            "printerId": pid,
            "timestamp": ts,
            "error": err,
            "remainingLabels": rl,
        }   

        pymongo_client = pymongo.MongoClient(DEFAULT_URI)
        log.info("MongoDB Setter processor started, listening for cleaned printer status messages...")

        # Create maintenance_required database if it doesn't exist
        if not check_db_exists(pymongo_client, DB_NAME):
            log.info(f"Creating database: {DB_NAME}")
            db = create_db(DEFAULT_URI, DB_NAME)
            print(db.list_collection_names())
        else:
            log.info(f"Database {DB_NAME} already exists.")

        # Insert into DB_MAINTENANCE
        db = create_db(DEFAULT_URI, DB_NAME)
        col_maintenance = db[COL_MAINTENANCE]
        #col_maintenance.insert_one(record)
        col_maintenance.insert_one(message.value)
        log.info(f"Inserted record into {COL_MAINTENANCE}: {message.value}")

        for x in col_maintenance.find():
            log.info(x)





if __name__ == "__main__":
    run_db() 