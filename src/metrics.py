"""
metrics.py - Collect, store, and display pipeline run metrics.
All metrics are saved to reports/results.json as required.
"""
import json
import os
import sys
import time
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import RESULTS_FILE, REPORTS_DIR


class MetricsCollector:
    """Collects metrics throughout the pipeline run."""

    def __init__(self, run_id, file_name, file_size_mb, engine_used):
        self.data = {
            "run_id": run_id,
            "file_name": file_name,
            "file_size_mb": round(file_size_mb, 2),
            "engine_used": engine_used,
            "started_at": datetime.now(timezone.utc).isoformat(),
            "rows_read": 0,
            "raw_loaded": 0,
            "valid_count": 0,
            "corrected_count": 0,
            "quarantine_count": 0,
            "elapsed_seconds": 0,
            "throughput": 0,
            "batch_size": 0,
            "partitions": 0,
            "error_case_counts": {},
            "inserted_count": 0,
            "updated_count": 0,
            "unchanged_count": 0,
        }
        self._start_time = time.time()

    def update(self, **kwargs):
        """Update one or more metrics."""
        for key, value in kwargs.items():
            if key in self.data:
                self.data[key] = value

    def increment_error(self, error_code):
        """Increment count for a specific error type."""
        counts = self.data["error_case_counts"]
        counts[error_code] = counts.get(error_code, 0) + 1

    def finalize(self):
        """Calculate final metrics (elapsed time, throughput)."""
        elapsed = time.time() - self._start_time
        self.data["elapsed_seconds"] = round(elapsed, 2)
        raw = self.data["raw_loaded"]
        self.data["throughput"] = round(raw / elapsed, 2) if elapsed > 0 else 0
        self.data["finished_at"] = datetime.now(timezone.utc).isoformat()

    def save(self):
        """Save metrics to reports/results.json."""
        os.makedirs(REPORTS_DIR, exist_ok=True)

        # Load existing results if any
        results = []
        if os.path.exists(RESULTS_FILE):
            with open(RESULTS_FILE, "r", encoding="utf-8") as f:
                try:
                    results = json.load(f)
                    if not isinstance(results, list):
                        results = [results]
                except json.JSONDecodeError:
                    results = []

        results.append(self.data)

        with open(RESULTS_FILE, "w", encoding="utf-8") as f:
            json.dump(results, f, indent=2, ensure_ascii=False)

        print(f"[Metrics] Saved to {RESULTS_FILE}")

    def print_summary(self):
        """Print a human-readable summary of the run."""
        d = self.data
        print("\n" + "=" * 60)
        print("  PIPELINE RUN SUMMARY")
        print("=" * 60)
        print(f"  Run ID:          {d['run_id']}")
        print(f"  File:            {d['file_name']} ({d['file_size_mb']} MB)")
        print(f"  Engine:          {d['engine_used']}")
        print(f"  Rows Read:       {d['rows_read']:,}")
        print(f"  Raw Loaded:      {d['raw_loaded']:,}")
        print(f"  Valid:           {d['valid_count']:,}")
        print(f"  Corrected:       {d['corrected_count']:,}")
        print(f"  Quarantined:     {d['quarantine_count']:,}")
        print(f"  Inserted (new):  {d['inserted_count']:,}")
        print(f"  Updated:         {d['updated_count']:,}")
        print(f"  Unchanged:       {d['unchanged_count']:,}")
        print(f"  Elapsed:         {d['elapsed_seconds']:.2f}s")
        print(f"  Throughput:      {d['throughput']:,.2f} records/s")

        # Consistency check
        total_classified = d["valid_count"] + d["corrected_count"] + d["quarantine_count"]
        consistent = total_classified == d["raw_loaded"]
        status = "PASS" if consistent else "FAIL"
        print(f"\n  Consistency:     raw({d['raw_loaded']:,}) = "
              f"valid({d['valid_count']:,}) + corrected({d['corrected_count']:,}) + "
              f"quarantine({d['quarantine_count']:,}) = {total_classified:,}  [{status}]")

        if d["error_case_counts"]:
            print(f"\n  Error Breakdown:")
            for code, count in sorted(d["error_case_counts"].items()):
                print(f"    {code}: {count:,}")

        print("=" * 60 + "\n")
