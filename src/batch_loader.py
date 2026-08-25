"""
batch_loader.py - Python Batch Loader for small files.

Uses csv.reader in streaming mode (never loads full file into memory).
Does NOT use list(reader) or Pandas.
Collects records in configurable batches, inserts via insert_many.
Prints progress: batch number, record count, time, insertion rate.
"""
import csv
import os
import sys
import time
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import BATCH_SIZE, RAW_COLLECTION


def load_batch(file_path, db, run_id):
    """
    Load a CSV file into orders_raw using Python streaming batch insertion.

    Args:
        file_path: Path to the CSV file
        db: MongoDB database object
        run_id: Unique identifier for this run

    Returns:
        dict with loading metrics
    """
    collection = db[RAW_COLLECTION]
    source_file = os.path.basename(file_path)

    total_loaded = 0
    total_rows = 0
    batch_num = 0
    batch_buffer = []
    start_time = time.time()

    print(f"\n[Batch Loader] Starting Python Batch load...")
    print(f"[Batch Loader] File: {source_file}")
    print(f"[Batch Loader] Batch size: {BATCH_SIZE:,}")
    print()

    try:
        with open(file_path, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            header = next(reader)  # Read header row

            for row in reader:  # Streaming - one row at a time
                total_rows += 1
                row_num = total_rows

                # Build raw document
                raw_record = {}
                for i, col_name in enumerate(header):
                    raw_record[col_name] = row[i] if i < len(row) else ""

                doc = {
                    "run_id": run_id,
                    "source_file": source_file,
                    "source_row_number": row_num,
                    "ingested_at": datetime.now(timezone.utc),
                    "engine_used": "python_batch",
                    "raw_record": raw_record,
                }

                batch_buffer.append(doc)

                # Insert when batch is full
                if len(batch_buffer) >= BATCH_SIZE:
                    batch_num += 1
                    batch_start = time.time()

                    try:
                        collection.insert_many(batch_buffer, ordered=False)
                        total_loaded += len(batch_buffer)
                    except Exception as e:
                        # Log error but don't hide the cause
                        print(f"  [ERROR] Batch {batch_num}: {type(e).__name__}: {e}")
                        # Try inserting one by one to save what we can
                        for doc in batch_buffer:
                            try:
                                collection.insert_one(doc)
                                total_loaded += 1
                            except Exception:
                                pass

                    batch_elapsed = time.time() - batch_start
                    rate = len(batch_buffer) / batch_elapsed if batch_elapsed > 0 else 0
                    print(
                        f"  Batch {batch_num:,}: "
                        f"{total_loaded:,} records loaded | "
                        f"time: {batch_elapsed:.2f}s | "
                        f"rate: {rate:,.0f} rec/s"
                    )

                    batch_buffer = []

            # Insert remaining records
            if batch_buffer:
                batch_num += 1
                batch_start = time.time()

                try:
                    collection.insert_many(batch_buffer, ordered=False)
                    total_loaded += len(batch_buffer)
                except Exception as e:
                    print(f"  [ERROR] Batch {batch_num}: {type(e).__name__}: {e}")
                    for doc in batch_buffer:
                        try:
                            collection.insert_one(doc)
                            total_loaded += 1
                        except Exception:
                            pass

                batch_elapsed = time.time() - batch_start
                rate = len(batch_buffer) / batch_elapsed if batch_elapsed > 0 else 0
                print(
                    f"  Batch {batch_num:,} (final): "
                    f"{total_loaded:,} records loaded | "
                    f"time: {batch_elapsed:.2f}s | "
                    f"rate: {rate:,.0f} rec/s"
                )

    except Exception as e:
        print(f"[Batch Loader] FATAL ERROR: {e}")
        raise

    elapsed = time.time() - start_time
    overall_rate = total_loaded / elapsed if elapsed > 0 else 0

    print(f"\n[Batch Loader] Complete!")
    print(f"  Total rows read: {total_rows:,}")
    print(f"  Total loaded to raw: {total_loaded:,}")
    print(f"  Batches: {batch_num:,}")
    print(f"  Total time: {elapsed:.2f}s")
    print(f"  Overall rate: {overall_rate:,.0f} rec/s")

    return {
        "rows_read": total_rows,
        "raw_loaded": total_loaded,
        "batch_size": BATCH_SIZE,
        "partitions": 0,
    }
