"""
aggregations.py - Phase 2: Five Aggregation Reports.

Each report uses MongoDB Aggregation Pipeline on orders_validated.
All values are dynamic - no hardcoded data values.
"""
import sys
import os
import json

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import VALIDATED_COLLECTION


def _agg_sales_by_city(db):
    """Report 1: Total sales grouped by city."""
    col = db[VALIDATED_COLLECTION]
    pipeline = [
        {"$addFields": {"amount_num": {"$toDouble": {"$ifNull": ["$total_amount", "0"]}}}},
        {"$group": {
            "_id": "$city",
            "total_sales": {"$sum": "$amount_num"},
            "order_count": {"$sum": 1},
        }},
        {"$sort": {"total_sales": -1}},
        {"$project": {"city": "$_id", "total_sales": 1, "order_count": 1, "_id": 0}},
    ]
    return {"report": "sales_by_city", "result": list(col.aggregate(pipeline))}


def _agg_top_products(db, limit=10):
    """Report 2: Top selling products by quantity."""
    col = db[VALIDATED_COLLECTION]
    pipeline = [
        {"$addFields": {"items": {"$cond": {
            "if": {"$eq": [{"$type": "$items_json"}, "string"]},
            "then": {"$cond": {
                "if": {"$eq": ["$items_json", ""]},
                "then": [],
                "else": {"$function": {
                    "body": "function(s) { try { return JSON.parse(s); } catch(e) { return []; } }",
                    "args": ["$items_json"],
                    "lang": "js",
                }}
            }},
            "else": {"$ifNull": ["$items_json", []]},
        }}}},
        {"$unwind": {"path": "$items", "preserveNullAndEmptyArrays": False}},
        {"$group": {
            "_id": {"$ifNull": ["$items.name", "$items.sku"]},
            "total_qty": {"$sum": {"$toDouble": {"$ifNull": ["$items.qty", "0"]}}},
            "total_revenue": {"$sum": {"$toDouble": {"$ifNull": ["$items.total", "0"]}}},
            "order_count": {"$sum": 1},
        }},
        {"$sort": {"total_qty": -1}},
        {"$limit": limit},
        {"$project": {"product": "$_id", "total_qty": 1, "total_revenue": 1, "order_count": 1, "_id": 0}},
    ]
    try:
        return {"report": "top_products", "result": list(col.aggregate(pipeline, allowDiskUse=True))}
    except Exception:
        # Fallback without $function if JS is disabled
        return _agg_top_products_fallback(db, limit)


def _agg_top_products_fallback(db, limit=10):
    """Fallback: top products using find + Python processing."""
    col = db[VALIDATED_COLLECTION]
    import json as json_mod
    product_stats = {}
    for doc in col.find({"items_json": {"$exists": True}}, {"items_json": 1}).limit(50000):
        try:
            items = json_mod.loads(doc.get("items_json", "[]")) if isinstance(doc.get("items_json"), str) else []
            for item in items:
                name = item.get("name", item.get("sku", "unknown"))
                qty = float(item.get("qty", 0))
                total = float(item.get("total", 0))
                if name not in product_stats:
                    product_stats[name] = {"total_qty": 0, "total_revenue": 0, "order_count": 0}
                product_stats[name]["total_qty"] += qty
                product_stats[name]["total_revenue"] += total
                product_stats[name]["order_count"] += 1
        except (json_mod.JSONDecodeError, ValueError, TypeError):
            continue
    sorted_products = sorted(product_stats.items(), key=lambda x: x[1]["total_qty"], reverse=True)[:limit]
    result = [{"product": k, **v} for k, v in sorted_products]
    return {"report": "top_products", "result": result}


def _agg_top_customers(db, limit=10):
    """Report 3: Top customers by total spending."""
    col = db[VALIDATED_COLLECTION]
    pipeline = [
        {"$addFields": {"amount_num": {"$toDouble": {"$ifNull": ["$total_amount", "0"]}}}},
        {"$group": {
            "_id": "$customer_id",
            "customer_name": {"$first": "$customer_name"},
            "total_spent": {"$sum": "$amount_num"},
            "order_count": {"$sum": 1},
        }},
        {"$sort": {"total_spent": -1}},
        {"$limit": limit},
        {"$project": {
            "customer_id": "$_id", "customer_name": 1,
            "total_spent": 1, "order_count": 1, "_id": 0,
        }},
    ]
    return {"report": "top_customers", "result": list(col.aggregate(pipeline, allowDiskUse=True))}


def _agg_sales_by_period(db):
    """Report 4: Sales aggregated by month."""
    col = db[VALIDATED_COLLECTION]
    pipeline = [
        {"$addFields": {
            "amount_num": {"$toDouble": {"$ifNull": ["$total_amount", "0"]}},
            "order_month": {"$substr": ["$order_date", 0, 7]},  # "YYYY-MM"
        }},
        {"$group": {
            "_id": "$order_month",
            "total_sales": {"$sum": "$amount_num"},
            "order_count": {"$sum": 1},
        }},
        {"$sort": {"_id": 1}},
        {"$project": {"period": "$_id", "total_sales": 1, "order_count": 1, "_id": 0}},
    ]
    return {"report": "sales_by_period", "result": list(col.aggregate(pipeline, allowDiskUse=True))}


def _agg_order_status_distribution(db):
    """Report 5: Distribution of orders by status."""
    col = db[VALIDATED_COLLECTION]
    total = col.count_documents({})
    pipeline = [
        {"$group": {
            "_id": "$status",
            "count": {"$sum": 1},
        }},
        {"$addFields": {
            "percentage": {"$round": [{"$multiply": [{"$divide": ["$count", max(total, 1)]}, 100]}, 2]},
        }},
        {"$sort": {"count": -1}},
        {"$project": {"status": "$_id", "count": 1, "percentage": 1, "_id": 0}},
    ]
    return {"report": "order_status_distribution", "total_orders": total, "result": list(col.aggregate(pipeline))}


# Registry
AGGREGATION_REGISTRY = {
    "sales_by_city": _agg_sales_by_city,
    "top_products": _agg_top_products,
    "top_customers": _agg_top_customers,
    "sales_by_period": _agg_sales_by_period,
    "order_status_distribution": _agg_order_status_distribution,
}


def run_aggregation(db, name):
    """Run an aggregation report by name."""
    if name not in AGGREGATION_REGISTRY:
        return {"error": f"Unknown aggregation: {name}", "available": list(AGGREGATION_REGISTRY.keys())}
    return AGGREGATION_REGISTRY[name](db)


def list_aggregations():
    """List all available aggregation names."""
    return list(AGGREGATION_REGISTRY.keys())


if __name__ == "__main__":
    from src.mongo_setup import get_client, get_database
    client = get_client()
    try:
        db = get_database(client)
        for name in list_aggregations():
            print(f"\n=== {name} ===")
            result = run_aggregation(db, name)
            # Print first 5 items for readability
            items = result.get("result", [])[:5]
            print(json.dumps({"report": result.get("report"), "top_5": items},
                             ensure_ascii=False, indent=2, default=str))
    finally:
        client.close()
