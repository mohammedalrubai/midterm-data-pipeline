"""
api.py - Phase 2: Unified FastAPI interface for running and testing the pipeline.

Provides 10 endpoints for all project features via Swagger (/docs).
This is NOT a standalone backend - it's a unified interface for existing functions.
"""
import sys
import os
import json
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Optional

from config.settings import (
    MONGO_URI, MONGO_DB, API_HOST, API_PORT,
    VALIDATED_COLLECTION, QUARANTINE_COLLECTION, RAW_COLLECTION,
    MV_DAILY_SALES, MV_TOP_PRODUCTS,
)
from src.mongo_setup import get_client, get_database
from src.queries import list_queries, run_query, create_indexes, run_explain
from src.aggregations import list_aggregations, run_aggregation
from src.materialized_views import refresh_all, get_view_data
from src.scheduler import list_jobs, run_job, get_job_logs

# ══════════════════════════════════════════════════════════════
# App + Database
# ══════════════════════════════════════════════════════════════
app = FastAPI(
    title="Midterm Data Pipeline API",
    description="Unified API for the Hybrid ELT Data Pipeline - Phase 2",
    version="2.0.0",
)

# Global client/db (created at startup)
_client = None
_db = None


@app.on_event("startup")
def startup():
    global _client, _db
    _client = get_client()
    _db = get_database(_client)


@app.on_event("shutdown")
def shutdown():
    global _client
    if _client:
        _client.close()


def _serialize(obj):
    """JSON-safe serialization for MongoDB results."""
    if isinstance(obj, datetime):
        return obj.isoformat()
    if type(obj).__name__ == 'ObjectId':
        return str(obj)
    if isinstance(obj, bytes):
        return obj.decode('utf-8', errors='replace')
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


def _clean_result(data):
    """Remove ObjectId and convert datetime for JSON serialization."""
    if isinstance(data, dict):
        return {k: _clean_result(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [_clean_result(item) for item in data]
    elif isinstance(data, datetime):
        return data.isoformat()
    elif type(data).__name__ == 'ObjectId':
        return str(data)
    elif isinstance(data, bytes):
        return data.decode('utf-8', errors='replace')
    return data


# ══════════════════════════════════════════════════════════════
# 1. GET /health
# ══════════════════════════════════════════════════════════════
@app.get("/health", tags=["System"])
def health_check():
    """Check system health: MongoDB connection and collection counts."""
    try:
        _client.admin.command("ping")
        counts = {
            RAW_COLLECTION: _db[RAW_COLLECTION].count_documents({}),
            VALIDATED_COLLECTION: _db[VALIDATED_COLLECTION].count_documents({}),
            QUARANTINE_COLLECTION: _db[QUARANTINE_COLLECTION].count_documents({}),
        }
        return {
            "status": "healthy",
            "mongodb": MONGO_URI,
            "database": MONGO_DB,
            "collections": counts,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
    except Exception as e:
        return JSONResponse(status_code=503, content={"status": "unhealthy", "error": str(e)})


# ══════════════════════════════════════════════════════════════
# 2. POST /ingest
# ══════════════════════════════════════════════════════════════
class IngestRequest(BaseModel):
    file_path: str
    reset: bool = False


@app.post("/ingest", tags=["Pipeline"])
def ingest(req: IngestRequest, background_tasks: BackgroundTasks):
    """
    Trigger the data ingestion pipeline.
    Uses the same Pipeline from Phase 1 (main.py).
    """
    file_path = req.file_path
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail=f"File not found: {file_path}")

    def _run_pipeline():
        import subprocess
        cmd = [sys.executable, os.path.join(os.path.dirname(__file__), "main.py"), file_path]
        if req.reset:
            cmd.append("--reset")
        subprocess.run(cmd, cwd=os.path.dirname(os.path.dirname(__file__)))

    background_tasks.add_task(_run_pipeline)

    return {
        "status": "started",
        "file": file_path,
        "reset": req.reset,
        "message": "Pipeline started in background. Check /health for collection counts.",
    }


# ══════════════════════════════════════════════════════════════
# 3. POST /indexes
# ══════════════════════════════════════════════════════════════
@app.post("/indexes", tags=["Queries & Indexes"])
def post_indexes():
    """Create Phase 2 indexes and return explain analysis."""
    try:
        explain_results = run_explain(_db)
        return _clean_result({
            "status": "success",
            "explain": explain_results,
        })
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ══════════════════════════════════════════════════════════════
# 4. GET /queries
# ══════════════════════════════════════════════════════════════
@app.get("/queries", tags=["Queries & Indexes"])
def get_queries():
    """List all available queries."""
    return {"queries": list_queries()}


# ══════════════════════════════════════════════════════════════
# 5. GET /queries/{name}
# ══════════════════════════════════════════════════════════════
@app.get("/queries/{name}", tags=["Queries & Indexes"])
def get_query_by_name(name: str):
    """Run a specific query by name."""
    result = run_query(_db, name)
    if "error" in result:
        raise HTTPException(status_code=404, detail=result)
    return _clean_result(result)


# ══════════════════════════════════════════════════════════════
# 6. GET /aggregations
# ══════════════════════════════════════════════════════════════
@app.get("/aggregations", tags=["Aggregations"])
def get_aggregations():
    """List all available aggregation reports."""
    return {"aggregations": list_aggregations()}


# ══════════════════════════════════════════════════════════════
# 7. GET /aggregations/{name}
# ══════════════════════════════════════════════════════════════
@app.get("/aggregations/{name}", tags=["Aggregations"])
def get_aggregation_by_name(name: str):
    """Run a specific aggregation report by name."""
    result = run_aggregation(_db, name)
    if "error" in result:
        raise HTTPException(status_code=404, detail=result)
    return _clean_result(result)


# ══════════════════════════════════════════════════════════════
# 8. POST /refresh-mv
# ══════════════════════════════════════════════════════════════
@app.post("/refresh-mv", tags=["Materialized Views"])
def post_refresh_mv(full: bool = False):
    """Refresh all materialized views. Use full=true for complete rebuild."""
    try:
        results = refresh_all(_db, full=full)
        return _clean_result({
            "status": "success",
            "mode": "full" if full else "incremental",
            "results": results,
        })
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ══════════════════════════════════════════════════════════════
# 9. GET /jobs
# ══════════════════════════════════════════════════════════════
@app.get("/jobs", tags=["Scheduled Jobs"])
def get_jobs():
    """List all scheduled jobs with their last execution info."""
    jobs = list_jobs(_db)
    return _clean_result({"jobs": jobs})


# ══════════════════════════════════════════════════════════════
# 10. POST /jobs/{name}/run
# ══════════════════════════════════════════════════════════════
@app.post("/jobs/{name}/run", tags=["Scheduled Jobs"])
def post_run_job(name: str):
    """Manually trigger a scheduled job by name."""
    result = run_job(_db, name)
    if "error" in result:
        raise HTTPException(status_code=404, detail=result)
    return _clean_result(result)


# ══════════════════════════════════════════════════════════════
# Run with: python src/api.py
# ══════════════════════════════════════════════════════════════
if __name__ == "__main__":
    import uvicorn
    print("Starting API server...")
    print(f"Swagger UI: http://localhost:{API_PORT}/docs")
    uvicorn.run(app, host=API_HOST, port=API_PORT)
