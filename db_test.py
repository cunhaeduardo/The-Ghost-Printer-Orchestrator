"""db_test.py

Simple MongoDB connection tester.

Usage:
  - Default URI is embedded from your request: mongodb://nraboy:password1234@localhost:27017
  - You can override it with the env var MONGO_URI or --uri CLI option.

This script attempts to connect and prints success or the error returned.
"""

import os
import sys
import argparse
from urllib.parse import urlparse, urlunparse

try:
    from pymongo import MongoClient, errors
except Exception as e:
    print("pymongo is required. Install with: pip install pymongo")
    raise


def mask_uri(uri: str) -> str:
    try:
        p = urlparse(uri)
        if p.password:
            netloc = f"{p.username}:***@{p.hostname}"
            if p.port:
                netloc += f":{p.port}"
            return urlunparse((p.scheme, netloc, p.path, p.params, p.query, p.fragment))
    except Exception:
        pass
    return uri


def test_connection(uri: str) -> int:
    print(f"Attempting connection to: {mask_uri(uri)}")
    try:
        client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        info = client.server_info()  # forces a connection
        print("✅ Connected to MongoDB server.")
        print(f"Server version: {info.get('version')}")
        try:
            dbs = client.list_database_names()
            print(f"Databases: {dbs}")
        except Exception:
            print("(Could not list databases; permission or auth issue.)")
        return 0
    except errors.ServerSelectionTimeoutError as e:
        print("⚠️ Could not connect to MongoDB (timeout):", e)
        return 2
    except Exception as e:
        print("❌ Error connecting to MongoDB:", e)
        return 1


def main(argv=None):
    parser = argparse.ArgumentParser(description="MongoDB connection tester")
    parser.add_argument("--uri", help="MongoDB URI to use (overrides MONGO_URI env)")
    args = parser.parse_args(argv)

    default_uri = os.getenv("MONGO_URI", "mongodb://nraboy:password1234@localhost:27017")
    uri = args.uri or default_uri
    return test_connection(uri)


if __name__ == "__main__":
    sys.exit(main())
