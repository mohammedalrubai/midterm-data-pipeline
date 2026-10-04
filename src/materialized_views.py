"""
materialized_views.py - Phase 2: Materialized Views with Incremental Update.

Two materialized views:
  1. daily_sales_summary  - Sales aggregated by date + city
  2. top_products_summary - Product ranking by quantity and revenue

Each view supports incremental refresh: only processes new data since
the last refresh, without rebuilding from scratch.
"""
import sys
import os
import json
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import (
    VALIDATED_COLLECTION, MV_DAILY_SALES, MV_TOP_PRODUCTS,
)


def _get_last_refresh(db, mv_name):
    """Get the timestamp of the last refresh for a materialized view."""
    meta = db["mv_metadata"].find_one({"_id": mv_name})
    if meta:
        return meta.get("last_refresh")
    return None


def _set_last_refresh(db, mv_name, ts):
    """Update the last refresh timestamp."""
    db["mv_metadata"].replace_one(
        {"_id": mv_name},
        {"_id": mv_name, "last_refresh": ts, "updated_at": datetime.now(timezone.utc)},
        upsert=True,
    )


def refresh_daily_sales(db, full=False):
    """
    Refresh daily_sales_summary materialized view.

    Incremental: only processes records validated after last refresh.
    Full: rebuilds entirely.
    """
    col = db[VALIDATED_COLLECTION]
    mv_col = db[MV_DAILY_SALES]
    now = datetime.now(timezone.utc)

    match_stage = {}
    if not full:
        last_refresh = _get_last_refresh(db, MV_DAILY_SALES)
        if last_refresh:
            match_stage = {"validated_at": {"$gt": last_refresh}}
            print(f"[MV] daily_sales_summary: incremental since {last_refresh}")
        else:
            print("[MV] daily_sales_summary: full build (first run)")
    else:
        mv_col.drop()
        print("[MV] daily_sales_summary: full rebuild")

    pipeline = [
        {"$addFields": {
            "amount_num": {"$toDouble": {"$ifNull": ["$total_amount", "0"]}},
            "order_day": {"$substr": ["$order_date", 0, 10]},  # "YYYY-MM-DD"
        }},
        {"$group": {
            "_id": {"date": "$order_day", "city": "$city"},
            "total_sales": {"$sum": "$amount_num"},
            "order_count": {"$sum": 1},
            "avg_order_value": {"$avg": "$amount_num"},
        }},
    ]
    if match_stage:
        pipeline.insert(0, {"$match": match_stage})

    results = list(col.aggregate(pipeline, allowDiskUse=True))
    updated = 0

    for row in results:
        key = row["_id"]
        # Upsert: merge with existing data
        existing = mv_col.find_one({"_id": key})
        if existing:
            new_total = existing["total_sales"] + row["total_sales"]
            new_count = existing["order_count"] + row["order_count"]
            new_avg = new_total / new_count if new_count > 0 else 0
            mv_col.update_one(
                {"_id": key},
                {"$set": {
                    "total_sales": new_total,
                    "order_count": new_count,
                    "avg_order_value": round(new_avg, 2),
                    "refreshed_at": now,
                }},
            )
        else:
            mv_col.insert_one({
                "_id": key,
                "date": key["date"],
                "city": key["city"],
                "total_sales": row["total_sales"],
                "order_count": row["order_count"],
                "avg_order_value": round(row.get("avg_order_value", 0), 2),
                "refreshed_at": now,
            })
        updated += 1

    _set_last_refresh(db, MV_DAILY_SALES, now)
    total_docs = mv_col.count_documents({})
    print(f"[MV] daily_sales_summary: {updated} entries updated, {total_docs} total")
    return {"view": MV_DAILY_SALES, "entries_updated": updated, "total_docs": total_docs}


def refresh_top_products(db, full=False, limit=50):
    """
    Refresh top_products_summary materialized view.

    Incremental: only processes records validated after last refresh.
    """
    col = db[VALIDATED_COLLECTION]
    mv_col = db[MV_TOP_PRODUCTS]
    now = datetime.now(timezone.utc)

    match_stage = {}
    if not full:
        last_refresh = _get_last_refresh(db, MV_TOP_PRODUCTS)
        if last_refresh:
            match_stage = {"validated_at": {"$gt": last_refresh}}
            print(f"[MV] top_products_summary: incremental since {last_refresh}")
        else:
            print("[MV] top_products_summary: full build (first run)")
    else:
        mv_col.drop()
        print("[MV] top_products_summary: full rebuild")

    # Process with Python (reliable, works without server-side JS)
    query = match_stage if match_stage else {}
    cursor = col.find(query, {"items_json": 1, "order_date": 1})

    product_stats = {}
    for doc in cursor:
        try:
            items_raw = doc.get("items_json", "")
            items = json.loads(items_raw) if isinstance(items_raw, str) and items_raw else []
            if not isinstance(items, list):
                continue
            for item in items:
                name = item.get("name", item.get("sku", "unknown"))
                qty = float(item.get("qty", 0))
                revenue = float(item.get("total", 0))
                if name not in product_stats:
                    product_stats[name] = {"total_qty": 0, "total_revenue": 0, "order_count": 0}
                product_stats[name]["total_qty"] += qty
                product_stats[name]["total_revenue"] += revenue
                product_stats[name]["order_count"] += 1
        except (json.JSONDecodeError, ValueError, TypeError):
            continue

    updated = 0
    for product_name, stats in product_stats.items():
        existing = mv_col.find_one({"_id": product_name})
        if existing:
            mv_col.update_one(
                {"_id": product_name},
                {"$inc": {
                    "total_qty": stats["total_qty"],
                    "total_revenue": stats["total_revenue"],
                    "order_count": stats["order_count"],
                }, "$set": {"refreshed_at": now}},
            )
        else:
            mv_col.insert_one({
                "_id": product_name,
                "product": product_name,
                "total_qty": stats["total_qty"],
                "total_revenue": stats["total_revenue"],
                "order_count": stats["order_count"],
                "refreshed_at": now,
            })
        updated += 1

    _set_last_refresh(db, MV_TOP_PRODUCTS, now)
    total_docs = mv_col.count_documents({})
    print(f"[MV] top_products_summary: {updated} products updated, {total_docs} total")
    return {"view": MV_TOP_PRODUCTS, "entries_updated": updated, "total_docs": total_docs}


def refresh_all(db, full=False):
    """Refresh all materialized views."""
    results = []
    results.append(refresh_daily_sales(db, full=full))
    results.append(refresh_top_products(db, full=full))
    return results


def get_view_data(db, view_name, limit=20):
    """Read data from a materialized view."""
    if view_name == MV_DAILY_SALES:
        col = db[MV_DAILY_SALES]
        data = list(col.find({}, {"_id": 0}).sort("total_sales", -1).limit(limit))
        return {"view": MV_DAILY_SALES, "count": col.count_documents({}), "sample": data}
    elif view_name == MV_TOP_PRODUCTS:
        col = db[MV_TOP_PRODUCTS]
        data = list(col.find({}, {"_id": 0}).sort("total_qty", -1).limit(limit))
        return {"view": MV_TOP_PRODUCTS, "count": col.count_documents({}), "sample": data}
    return {"error": f"Unknown view: {view_name}"}


if __name__ == "__main__":
    from src.mongo_setup import get_client, get_database
    client = get_client()
    try:
        db = get_database(client)
        print("=== Refreshing Materialized Views ===")
        results = refresh_all(db, full=True)
        for r in results:
            print(json.dumps(r, ensure_ascii=False, indent=2, default=str))
    finally:
        client.close()
