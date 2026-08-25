"""
elt_pipeline.py - ELT Transform & Classify stage.

Reads from orders_raw (already loaded), applies cleaning rules,
classifies each record as Valid/Corrected/Quarantine, then writes to
orders_validated (via Upsert) or orders_quarantine.

ELT principle: data is loaded RAW first, then transformed HERE.
No record is silently dropped - every record ends up in Validated or Quarantine.

Idempotency: Uses Upsert (replace_one with upsert=True) keyed on order_id.
Re-running the same data does NOT create duplicates.
"""
import os
import sys
import time
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import (
    RAW_COLLECTION, VALIDATED_COLLECTION, QUARANTINE_COLLECTION,
)
from src.quality_rules import apply_cleaning_rules, check_quarantine


def process_raw_to_validated(db, run_id):
    """
    Process all raw records for a given run_id.
    Applies cleaning → quarantine check → Upsert to validated or quarantine.

    Returns:
        dict with classification and upsert metrics
    """
    raw_col = db[RAW_COLLECTION]
    valid_col = db[VALIDATED_COLLECTION]
    quarantine_col = db[QUARANTINE_COLLECTION]

    # Count raw records for this run
    raw_count = raw_col.count_documents({"run_id": run_id})
    print(f"\n[ELT Pipeline] Processing {raw_count:,} raw records (run_id={run_id})...")

    valid_count = 0
    corrected_count = 0
    quarantine_count = 0
    inserted_count = 0
    updated_count = 0
    unchanged_count = 0
    error_counts = {}
    duplicate_tracker = {}  # Track order_id duplicates within this run

    start_time = time.time()
    processed = 0

    cursor = raw_col.find({"run_id": run_id})

    for raw_doc in cursor:
        processed += 1

        # Extract the raw record
        raw_record = raw_doc.get("raw_record", {})
        if not raw_record:
            # If Spark loader stored fields at top level
            raw_record = {k: raw_doc.get(k, "") for k in [
                "order_id", "order_date", "status", "customer_id",
                "customer_name", "customer_phone", "customer_email",
                "city", "district", "delivery_type", "delivery_cost",
                "payment_method", "payment_status", "payment_amount",
                "currency", "total_amount", "items_json",
            ]}

        # ──── Step 1: Apply cleaning rules ────
        cleaned, corrections = apply_cleaning_rules(raw_record)

        # ──── Step 2: Check for quarantine conditions ────
        error_codes, error_details = check_quarantine(cleaned)

        # ──── Step 3: Check for duplicate order_id within this run ────
        order_id = str(cleaned.get("order_id", "")).strip()
        if order_id and order_id in duplicate_tracker:
            if "DUPLICATE_ORDER_ID" not in error_codes:
                error_codes.append("DUPLICATE_ORDER_ID")
                error_details.append({
                    "code": "DUPLICATE_ORDER_ID",
                    "message": f"Duplicate order_id '{order_id}' within this run",
                })
        if order_id:
            duplicate_tracker[order_id] = duplicate_tracker.get(order_id, 0) + 1

        # ──── Step 4: Route to Quarantine or Validated ────
        if error_codes:
            # ▶ QUARANTINE
            quarantine_doc = {
                "order_id": order_id if order_id else None,
                "error_codes": error_codes,
                "error_details": error_details,
                "raw_record": raw_record,
                "run_id": run_id,
                "quarantined_at": datetime.now(timezone.utc),
                "source_row_number": raw_doc.get("source_row_number"),
            }
            quarantine_col.insert_one(quarantine_doc)
            quarantine_count += 1

            # Track error types
            for code in error_codes:
                error_counts[code] = error_counts.get(code, 0) + 1

        else:
            # ▶ VALIDATED (via Upsert)
            if corrections:
                quality_status = "corrected"
                corrected_count += 1
            else:
                quality_status = "valid"
                valid_count += 1

            validated_doc = dict(cleaned)
            validated_doc["quality_status"] = quality_status
            validated_doc["corrections"] = corrections
            validated_doc["run_id"] = run_id
            validated_doc["validated_at"] = datetime.now(timezone.utc)

            # Upsert keyed on order_id (Idempotent)
            result = valid_col.replace_one(
                {"order_id": order_id},  # filter
                validated_doc,           # replacement
                upsert=True,             # insert if not exists
            )

            if result.upserted_id:
                inserted_count += 1
            elif result.modified_count > 0:
                updated_count += 1
            else:
                unchanged_count += 1

        # Progress reporting
        if processed % 10000 == 0:
            elapsed = time.time() - start_time
            rate = processed / elapsed if elapsed > 0 else 0
            print(
                f"  Processed: {processed:,}/{raw_count:,} | "
                f"Valid: {valid_count:,} | Corrected: {corrected_count:,} | "
                f"Quarantine: {quarantine_count:,} | Rate: {rate:,.0f} rec/s"
            )

    elapsed = time.time() - start_time

    print(f"\n[ELT Pipeline] Complete!")
    print(f"  Processed:     {processed:,}")
    print(f"  Valid:          {valid_count:,}")
    print(f"  Corrected:     {corrected_count:,}")
    print(f"  Quarantined:   {quarantine_count:,}")
    print(f"  Inserted:      {inserted_count:,}")
    print(f"  Updated:       {updated_count:,}")
    print(f"  Unchanged:     {unchanged_count:,}")
    print(f"  Time:          {elapsed:.2f}s")

    # Consistency check
    total = valid_count + corrected_count + quarantine_count
    status = "PASS" if total == raw_count else "FAIL"
    print(f"  Consistency:   raw({raw_count:,}) = valid({valid_count:,}) + "
          f"corrected({corrected_count:,}) + quarantine({quarantine_count:,}) "
          f"= {total:,}  [{status}]")

    return {
        "valid_count": valid_count,
        "corrected_count": corrected_count,
        "quarantine_count": quarantine_count,
        "inserted_count": inserted_count,
        "updated_count": updated_count,
        "unchanged_count": unchanged_count,
        "error_case_counts": error_counts,
    }
