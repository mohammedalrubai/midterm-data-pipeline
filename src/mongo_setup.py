"""
mongo_setup.py - MongoDB connection, collection creation, and index management.
Separates database logic from loading and cleaning logic (as required).
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pymongo import MongoClient, ASCENDING
from pymongo.errors import ConnectionFailure
from config.settings import (
    MONGO_URI, MONGO_DB,
    RAW_COLLECTION, VALIDATED_COLLECTION, QUARANTINE_COLLECTION,
)


def get_client():
    """Create and return a MongoClient. Uses try/finally pattern externally."""
    client = MongoClient(MONGO_URI)
    try:
        client.admin.command("ping")
        print(f"[MongoDB] Connected to {MONGO_URI}")
    except ConnectionFailure:
        print(f"[MongoDB] ERROR: Cannot connect to {MONGO_URI}")
        raise
    return client


def get_database(client=None):
    """Return the project database."""
    if client is None:
        client = get_client()
    return client[MONGO_DB]


def setup_collections(db):
    """Create collections and indexes if they don't exist."""
    existing = db.list_collection_names()

    for col_name in [RAW_COLLECTION, VALIDATED_COLLECTION, QUARANTINE_COLLECTION]:
        if col_name not in existing:
            db.create_collection(col_name)
            print(f"[MongoDB] Created collection: {col_name}")

    # Unique index on order_id in orders_validated (required for Upsert/Idempotency)
    db[VALIDATED_COLLECTION].create_index(
        [("order_id", ASCENDING)],
        unique=True,
        name="unique_order_id",
    )

    # Index on run_id for efficient querying per run
    db[RAW_COLLECTION].create_index(
        [("run_id", ASCENDING)],
        name="idx_run_id",
    )

    db[QUARANTINE_COLLECTION].create_index(
        [("run_id", ASCENDING)],
        name="idx_quarantine_run_id",
    )

    print(f"[MongoDB] Collections and indexes ready in '{MONGO_DB}'")


def drop_collections(db):
    """Drop all project collections (for clean re-runs)."""
    for col_name in [RAW_COLLECTION, VALIDATED_COLLECTION, QUARANTINE_COLLECTION]:
        db[col_name].drop()
    print(f"[MongoDB] All collections dropped in '{MONGO_DB}'")


if __name__ == "__main__":
    client = get_client()
    try:
        db = get_database(client)
        setup_collections(db)
        for col in [RAW_COLLECTION, VALIDATED_COLLECTION, QUARANTINE_COLLECTION]:
            count = db[col].count_documents({})
            print(f"  {col}: {count:,} documents")
    finally:
        client.close()
