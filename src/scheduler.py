"""
scheduler.py - Phase 2: Scheduled Jobs with logging.

Two scheduled jobs:
  1. refresh_views - Refresh all Materialized Views
  2. daily_report  - Generate a daily summary report

Each job logs: start_time, end_time, status (success/failed), result.
Jobs can run on a schedule or manually.
"""
import sys
import os
import time
import json
import threading
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import (
    JOB_LOGS_COLLECTION, VALIDATED_COLLECTION,
    QUARANTINE_COLLECTION, SCHEDULER_INTERVAL_MINUTES,
)


def _log_job(db, job_name, start_time, end_time, status, result):
    """Log a job execution to job_logs collection."""
    db[JOB_LOGS_COLLECTION].insert_one({
        "job_name": job_name,
        "start_time": start_time,
        "end_time": end_time,
        "duration_seconds": round((end_time - start_time).total_seconds(), 2),
        "status": status,
        "result": result,
    })


def job_refresh_views(db):
    """Job 1: Refresh all materialized views (incremental)."""
    from src.materialized_views import refresh_all

    start = datetime.now(timezone.utc)
    print(f"\n[Job] refresh_views started at {start.isoformat()}")

    try:
        result = refresh_all(db, full=False)
        end = datetime.now(timezone.utc)
        _log_job(db, "refresh_views", start, end, "success",
                 json.dumps(result, default=str))
        print(f"[Job] refresh_views completed successfully in "
              f"{(end - start).total_seconds():.2f}s")
        return {"job": "refresh_views", "status": "success", "result": result,
                "start": start.isoformat(), "end": end.isoformat()}
    except Exception as e:
        end = datetime.now(timezone.utc)
        _log_job(db, "refresh_views", start, end, "failed", str(e))
        print(f"[Job] refresh_views FAILED: {e}")
        return {"job": "refresh_views", "status": "failed", "error": str(e)}


def job_daily_report(db):
    """Job 2: Generate a daily summary report."""
    start = datetime.now(timezone.utc)
    print(f"\n[Job] daily_report started at {start.isoformat()}")

    try:
        validated_col = db[VALIDATED_COLLECTION]
        quarantine_col = db[QUARANTINE_COLLECTION]

        total_validated = validated_col.count_documents({})
        total_quarantine = quarantine_col.count_documents({})
        total_valid = validated_col.count_documents({"quality_status": "valid"})
        total_corrected = validated_col.count_documents({"quality_status": "corrected"})

        # Sales summary
        sales_pipeline = [
            {"$addFields": {"amount_num": {"$toDouble": {"$ifNull": ["$total_amount", "0"]}}}},
            {"$group": {
                "_id": None,
                "total_revenue": {"$sum": "$amount_num"},
                "avg_order": {"$avg": "$amount_num"},
                "max_order": {"$max": "$amount_num"},
            }},
        ]
        sales_result = list(validated_col.aggregate(sales_pipeline))
        sales_info = sales_result[0] if sales_result else {}

        # Top city
        city_pipeline = [
            {"$addFields": {"amount_num": {"$toDouble": {"$ifNull": ["$total_amount", "0"]}}}},
            {"$group": {"_id": "$city", "total": {"$sum": "$amount_num"}}},
            {"$sort": {"total": -1}},
            {"$limit": 1},
        ]
        city_result = list(validated_col.aggregate(city_pipeline))
        top_city = city_result[0] if city_result else {}

        report = {
            "report_date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
            "total_validated": total_validated,
            "total_quarantine": total_quarantine,
            "valid_count": total_valid,
            "corrected_count": total_corrected,
            "total_revenue": round(sales_info.get("total_revenue", 0), 2),
            "avg_order_value": round(sales_info.get("avg_order", 0), 2),
            "max_order_value": round(sales_info.get("max_order", 0), 2),
            "top_city": top_city.get("_id", "N/A"),
            "top_city_revenue": round(top_city.get("total", 0), 2),
        }

        end = datetime.now(timezone.utc)
        _log_job(db, "daily_report", start, end, "success",
                 json.dumps(report, default=str, ensure_ascii=False))

        print(f"[Job] daily_report completed in {(end - start).total_seconds():.2f}s")
        print(f"  Validated: {total_validated:,} | Quarantine: {total_quarantine:,}")
        print(f"  Revenue: {report['total_revenue']:,.2f} | Top City: {report['top_city']}")

        return {"job": "daily_report", "status": "success", "result": report,
                "start": start.isoformat(), "end": end.isoformat()}

    except Exception as e:
        end = datetime.now(timezone.utc)
        _log_job(db, "daily_report", start, end, "failed", str(e))
        print(f"[Job] daily_report FAILED: {e}")
        return {"job": "daily_report", "status": "failed", "error": str(e)}


# Registry
JOB_REGISTRY = {
    "refresh_views": job_refresh_views,
    "daily_report": job_daily_report,
}


def run_job(db, name):
    """Run a job by name. Returns result dict."""
    if name not in JOB_REGISTRY:
        return {"error": f"Unknown job: {name}", "available": list(JOB_REGISTRY.keys())}
    return JOB_REGISTRY[name](db)


def list_jobs(db=None):
    """List all jobs with their last execution info."""
    jobs = []
    for name in JOB_REGISTRY:
        info = {"name": name, "description": JOB_REGISTRY[name].__doc__.strip()}
        if db is not None:
            last = db[JOB_LOGS_COLLECTION].find_one(
                {"job_name": name}, sort=[("end_time", -1)]
            )
            if last:
                end_time = last.get("end_time", "")
                info["last_run"] = end_time.isoformat() if hasattr(end_time, "isoformat") else str(end_time)
                info["last_status"] = last.get("status")
                info["last_duration"] = last.get("duration_seconds")
        jobs.append(info)
    return jobs


def get_job_logs(db, job_name=None, limit=10):
    """Get recent job execution logs."""
    query = {"job_name": job_name} if job_name else {}
    logs = list(
        db[JOB_LOGS_COLLECTION]
        .find(query, {"_id": 0})
        .sort("end_time", -1)
        .limit(limit)
    )
    return logs


def start_scheduler(db, interval_minutes=None):
    """Start the scheduler (runs jobs periodically in background)."""
    if interval_minutes is None:
        interval_minutes = SCHEDULER_INTERVAL_MINUTES

    def _run_loop():
        while True:
            print(f"\n[Scheduler] Running scheduled jobs...")
            for name in JOB_REGISTRY:
                run_job(db, name)
            print(f"[Scheduler] Next run in {interval_minutes} minutes")
            time.sleep(interval_minutes * 60)

    thread = threading.Thread(target=_run_loop, daemon=True)
    thread.start()
    print(f"[Scheduler] Started (interval: {interval_minutes} min)")
    return thread


if __name__ == "__main__":
    from src.mongo_setup import get_client, get_database
    client = get_client()
    try:
        db = get_database(client)
        print("=== Running All Jobs ===")
        for name in JOB_REGISTRY:
            result = run_job(db, name)
            print(json.dumps(result, ensure_ascii=False, indent=2, default=str))

        print("\n=== Job Logs ===")
        logs = get_job_logs(db)
        for log in logs:
            print(json.dumps(log, ensure_ascii=False, indent=2, default=str))
    finally:
        client.close()
