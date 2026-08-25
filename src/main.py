"""
main.py - Single entry point for the entire pipeline.

Usage:
    python src/main.py <path_to_csv> [--reset]

The pipeline:
  1. Routes the file to Python Batch or PySpark (based on size)
  2. Loads raw data into orders_raw (ELT: load before cleaning)
  3. Transforms, cleans, classifies records
  4. Writes to orders_validated (Upsert) or orders_quarantine
  5. Saves metrics to reports/results.json

Flags:
  --reset  Drop all collections before running (clean start)
"""
import argparse
import os
import sys
import uuid

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import BATCH_SIZE
from src.mongo_setup import get_client, get_database, setup_collections, drop_collections
from src.file_router import route_file
from src.batch_loader import load_batch
from src.spark_loader import load_spark
from src.elt_pipeline import process_raw_to_validated
from src.metrics import MetricsCollector


def main(file_path, reset=False):
    """Run the complete data pipeline."""

    print("=" * 60)
    print("  MIDTERM DATA PIPELINE — Hybrid ELT")
    print("=" * 60)

    # Validate input file
    if not os.path.exists(file_path):
        print(f"[ERROR] File not found: {file_path}")
        sys.exit(1)

    # ──── Step 1: Connect to MongoDB ────
    client = get_client()
    try:
        db = get_database(client)

        if reset:
            print("\n[Reset] Dropping all collections...")
            drop_collections(db)

        setup_collections(db)

        # ──── Step 2: Route file to engine ────
        print()
        engine, file_size_mb = route_file(file_path)

        # ──── Step 3: Generate run_id ────
        run_id = str(uuid.uuid4())[:8]
        print(f"\n[Pipeline] Run ID: {run_id}")

        # ──── Step 4: Initialize metrics ────
        metrics = MetricsCollector(
            run_id=run_id,
            file_name=os.path.basename(file_path),
            file_size_mb=file_size_mb,
            engine_used=engine,
        )
        metrics.update(batch_size=BATCH_SIZE)

        # ──── Step 5: Raw Load (ELT: load BEFORE cleaning) ────
        if engine == "python_batch":
            load_result = load_batch(file_path, db, run_id)
        else:
            load_result = load_spark(file_path, db, run_id)

        metrics.update(
            rows_read=load_result["rows_read"],
            raw_loaded=load_result["raw_loaded"],
            batch_size=load_result.get("batch_size", 0),
            partitions=load_result.get("partitions", 0),
        )

        # ──── Step 6: ELT Transform & Classify ────
        elt_result = process_raw_to_validated(db, run_id)

        metrics.update(
            valid_count=elt_result["valid_count"],
            corrected_count=elt_result["corrected_count"],
            quarantine_count=elt_result["quarantine_count"],
            inserted_count=elt_result["inserted_count"],
            updated_count=elt_result["updated_count"],
            unchanged_count=elt_result["unchanged_count"],
            error_case_counts=elt_result["error_case_counts"],
        )

        # ──── Step 7: Finalize and save metrics ────
        metrics.finalize()
        metrics.save()
        metrics.print_summary()

        print("[Pipeline] DONE - SUCCESS")

    finally:
        client.close()
        print("[MongoDB] Connection closed.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Midterm Data Pipeline - Hybrid ELT System"
    )
    parser.add_argument(
        "file",
        help="Path to the input CSV file"
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Drop all collections before running (clean start)"
    )

    args = parser.parse_args()
    main(args.file, reset=args.reset)
