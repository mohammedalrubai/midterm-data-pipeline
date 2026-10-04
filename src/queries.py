"""
queries.py - Phase 2: MongoDB Queries, Indexes, and Explain analysis.

Provides:
  - 5 practical queries on orders_validated / orders_quarantine
  - 3 indexes (including 1 Compound Index)
  - explain("executionStats") before and after index creation
"""
import sys
import os
import json

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pymongo import ASCENDING, DESCENDING
from config.settings import VALIDATED_COLLECTION, QUARANTINE_COLLECTION


# ══════════════════════════════════════════════════════════════
# INDEX DEFINITIONS (3 indexes, including 1 compound)
# ══════════════════════════════════════════════════════════════
INDEX_DEFINITIONS = [
    {
        "name": "idx_city",
        "collection": VALIDATED_COLLECTION,
        "keys": [("city", ASCENDING)],
        "reason": "Speeds up queries filtering or grouping by city (sales_by_city, orders_in_city).",
    },
    {
        "name": "idx_status",
        "collection": VALIDATED_COLLECTION,
        "keys": [("status", ASCENDING)],
        "reason": "Speeds up queries filtering by order status (order_status_distribution).",
    },
    {
        "name": "idx_city_status_compound",
        "collection": VALIDATED_COLLECTION,
        "keys": [("city", ASCENDING), ("status", ASCENDING)],
        "reason": "Compound index for queries that filter by both city AND status simultaneously.",
    },
]


def create_indexes(db):
    """Create all Phase 2 indexes. Returns list of created index info."""
    results = []
    for idx_def in INDEX_DEFINITIONS:
        col = db[idx_def["collection"]]
        col.create_index(idx_def["keys"], name=idx_def["name"])
        results.append({
            "name": idx_def["name"],
            "collection": idx_def["collection"],
            "keys": str(idx_def["keys"]),
            "reason": idx_def["reason"],
            "status": "created",
        })
        print(f"[Indexes] Created: {idx_def['name']} on {idx_def['collection']}")
    return results


def drop_phase2_indexes(db):
    """Drop Phase 2 indexes (for before/after comparison)."""
    for idx_def in INDEX_DEFINITIONS:
        col = db[idx_def["collection"]]
        try:
            col.drop_index(idx_def["name"])
        except Exception:
            pass


# ══════════════════════════════════════════════════════════════
# 5 QUERY DEFINITIONS
# ══════════════════════════════════════════════════════════════
def _query_orders_in_city(db, city=None):
    """Query 1: Count orders in a specific city."""
    col = db[VALIDATED_COLLECTION]
    # Pick a city dynamically if none provided
    if city is None:
        sample = col.find_one({}, {"city": 1})
        city = sample.get("city", "") if sample else ""
    pipeline = [
        {"$match": {"city": city}},
        {"$count": "total_orders"},
    ]
    result = list(col.aggregate(pipeline))
    return {"query": "orders_in_city", "city": city, "result": result}


def _query_orders_by_status(db, status=None):
    """Query 2: Find orders with a specific status."""
    col = db[VALIDATED_COLLECTION]
    if status is None:
        sample = col.find_one({}, {"status": 1})
        status = sample.get("status", "") if sample else ""
    cursor = col.find({"status": status}, {"order_id": 1, "city": 1, "total_amount": 1, "_id": 0}).limit(10)
    return {"query": "orders_by_status", "status": status, "result": list(cursor)}


def _query_customer_total(db, customer_id=None):
    """Query 3: Total spending for a specific customer."""
    col = db[VALIDATED_COLLECTION]
    if customer_id is None:
        sample = col.find_one({}, {"customer_id": 1})
        customer_id = sample.get("customer_id", "") if sample else ""
    pipeline = [
        {"$match": {"customer_id": customer_id}},
        {"$addFields": {"amount_num": {"$toDouble": {"$ifNull": ["$total_amount", "0"]}}}},
        {"$group": {"_id": "$customer_id", "total_spent": {"$sum": "$amount_num"}, "order_count": {"$sum": 1}}},
    ]
    result = list(col.aggregate(pipeline))
    return {"query": "customer_total_spending", "customer_id": customer_id, "result": result}


def _query_quarantine_by_error(db):
    """Query 4: Count quarantined records by error code."""
    col = db[QUARANTINE_COLLECTION]
    pipeline = [
        {"$unwind": "$error_codes"},
        {"$group": {"_id": "$error_codes", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
    ]
    result = list(col.aggregate(pipeline))
    return {"query": "quarantine_by_error_code", "result": result}


def _query_city_status_compound(db, city=None, status=None):
    """Query 5: Orders filtered by both city AND status (uses compound index)."""
    col = db[VALIDATED_COLLECTION]
    if city is None:
        sample = col.find_one({}, {"city": 1, "status": 1})
        city = sample.get("city", "") if sample else ""
        status = sample.get("status", "") if sample else ""
    pipeline = [
        {"$match": {"city": city, "status": status}},
        {"$count": "total_orders"},
    ]
    result = list(col.aggregate(pipeline))
    return {"query": "city_status_compound", "city": city, "status": status, "result": result}


# Registry: name -> function
QUERY_REGISTRY = {
    "orders_in_city": _query_orders_in_city,
    "orders_by_status": _query_orders_by_status,
    "customer_total_spending": _query_customer_total,
    "quarantine_by_error_code": _query_quarantine_by_error,
    "city_status_compound": _query_city_status_compound,
}


def run_query(db, name):
    """Run a query by name. Returns dict with results."""
    if name not in QUERY_REGISTRY:
        return {"error": f"Unknown query: {name}", "available": list(QUERY_REGISTRY.keys())}
    return QUERY_REGISTRY[name](db)


def list_queries():
    """List all available query names."""
    return list(QUERY_REGISTRY.keys())


# ══════════════════════════════════════════════════════════════
# EXPLAIN ANALYSIS (before and after indexes)
# ══════════════════════════════════════════════════════════════
def run_explain(db):
    """Run explain on 3 queries before and after creating indexes."""
    col = db[VALIDATED_COLLECTION]
    sample = col.find_one({}, {"city": 1, "status": 1})
    city = sample.get("city", "") if sample else ""
    status = sample.get("status", "") if sample else ""

    explain_queries = [
        {"name": "find_by_city", "filter": {"city": city}},
        {"name": "find_by_status", "filter": {"status": status}},
        {"name": "find_by_city_and_status", "filter": {"city": city, "status": status}},
    ]

    results = []

    # ── BEFORE indexes ──
    drop_phase2_indexes(db)
    print("\n[Explain] === BEFORE Indexes ===")
    before_results = []
    for q in explain_queries:
        explain = col.find(q["filter"]).explain()
        stats = explain.get("executionStats", {})
        info = {
            "query": q["name"],
            "filter": q["filter"],
            "executionTimeMillis": stats.get("executionTimeMillis", "N/A"),
            "totalDocsExamined": stats.get("totalDocsExamined", "N/A"),
            "totalKeysExamined": stats.get("totalKeysExamined", "N/A"),
            "nReturned": stats.get("nReturned", "N/A"),
        }
        before_results.append(info)
        print(f"  {q['name']}: docs_examined={info['totalDocsExamined']}, "
              f"keys_examined={info['totalKeysExamined']}, time={info['executionTimeMillis']}ms")

    # ── CREATE indexes ──
    create_indexes(db)

    # ── AFTER indexes ──
    print("\n[Explain] === AFTER Indexes ===")
    after_results = []
    for q in explain_queries:
        explain = col.find(q["filter"]).explain()
        stats = explain.get("executionStats", {})
        info = {
            "query": q["name"],
            "filter": q["filter"],
            "executionTimeMillis": stats.get("executionTimeMillis", "N/A"),
            "totalDocsExamined": stats.get("totalDocsExamined", "N/A"),
            "totalKeysExamined": stats.get("totalKeysExamined", "N/A"),
            "nReturned": stats.get("nReturned", "N/A"),
        }
        after_results.append(info)
        print(f"  {q['name']}: docs_examined={info['totalDocsExamined']}, "
              f"keys_examined={info['totalKeysExamined']}, time={info['executionTimeMillis']}ms")

    return {"before_indexes": before_results, "after_indexes": after_results}


if __name__ == "__main__":
    from src.mongo_setup import get_client, get_database
    client = get_client()
    try:
        db = get_database(client)
        print("\n=== EXPLAIN ANALYSIS ===")
        explain_results = run_explain(db)
        print("\n=== RUNNING ALL QUERIES ===")
        for name in list_queries():
            result = run_query(db, name)
            print(f"\n[{name}]")
            print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    finally:
        client.close()
