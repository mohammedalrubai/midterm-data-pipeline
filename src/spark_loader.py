"""
spark_loader.py - PySpark Loader for large files.

Uses SparkSession with a fixed String schema (no inferSchema).
Reads data using Spark DataFrame API, then writes to MongoDB via PyMongo
using foreachPartition for distributed writing.
Does NOT use Pandas. Preserves dirty values by reading all fields as String.
"""
import os
import sys
import time
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# ── Hadoop setup for Windows (required by PySpark) ──
if sys.platform == "win32" and not os.environ.get("HADOOP_HOME"):
    hadoop_home = r"C:\hadoop"
    if os.path.exists(os.path.join(hadoop_home, "bin", "winutils.exe")):
        os.environ["HADOOP_HOME"] = hadoop_home
        os.environ["PATH"] = os.path.join(hadoop_home, "bin") + ";" + os.environ.get("PATH", "")

from config.settings import (
    SPARK_APP_NAME,
    MONGO_URI, MONGO_DB, RAW_COLLECTION, CSV_COLUMNS, BATCH_SIZE,
)


def _build_schema():
    """Build a StructType with ALL columns as StringType (preserve dirty values)."""
    from pyspark.sql.types import StructType, StructField, StringType
    fields = [StructField(col, StringType(), True) for col in CSV_COLUMNS]
    return StructType(fields)


def _write_partition_to_mongo(partition_iter, run_id, source_file, mongo_uri, mongo_db, raw_col, batch_size):
    """
    Write a Spark partition to MongoDB using PyMongo.
    Called via foreachPartition - runs inside each Spark executor.
    """
    from pymongo import MongoClient
    from datetime import datetime, timezone

    client = MongoClient(mongo_uri)
    db = client[mongo_db]
    collection = db[raw_col]

    batch = []
    count = 0

    for row in partition_iter:
        row_dict = row.asDict()

        # Separate metadata from raw data
        raw_record = {}
        for col in CSV_COLUMNS:
            raw_record[col] = row_dict.get(col, "")

        doc = {
            "run_id": run_id,
            "source_file": source_file,
            "source_row_number": row_dict.get("source_row_number", count),
            "ingested_at": datetime.now(timezone.utc),
            "engine_used": "pyspark",
            "raw_record": raw_record,
        }
        batch.append(doc)
        count += 1

        if len(batch) >= batch_size:
            collection.insert_many(batch, ordered=False)
            batch = []

    if batch:
        collection.insert_many(batch, ordered=False)

    client.close()


def load_spark(file_path, db, run_id):
    """
    Load a large CSV file into orders_raw using PySpark.

    Uses PySpark DataFrame API for reading (fixed String schema),
    then writes to MongoDB via PyMongo foreachPartition.

    Args:
        file_path: Path to the CSV file
        db: MongoDB database object
        run_id: Unique identifier for this run

    Returns:
        dict with loading metrics
    """
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import lit, monotonically_increasing_id

    source_file = os.path.basename(file_path)
    start_time = time.time()

    print(f"\n[Spark Loader] Starting PySpark load...")
    print(f"[Spark Loader] File: {source_file}")

    # Build SparkSession (no external connectors needed)
    spark = SparkSession.builder \
        .appName(SPARK_APP_NAME) \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()

    try:
        schema = _build_schema()

        # Read CSV with fixed schema - all String types (no inferSchema!)
        print("[Spark Loader] Reading CSV with fixed String schema...")
        df = spark.read \
            .option("header", "true") \
            .option("multiLine", "true") \
            .option("escape", '"') \
            .schema(schema) \
            .csv(file_path)

        # Add row number for source tracking
        df = df.withColumn("source_row_number", monotonically_increasing_id())

        total_rows = df.count()
        num_partitions = df.rdd.getNumPartitions()

        print(f"[Spark Loader] Rows read: {total_rows:,}")
        print(f"[Spark Loader] Partitions: {num_partitions}")

        # Write to MongoDB via PyMongo foreachPartition
        print("[Spark Loader] Writing to MongoDB via PyMongo (foreachPartition)...")
        write_start = time.time()

        # Broadcast parameters to executors
        b_run_id = run_id
        b_source = source_file
        b_uri = MONGO_URI
        b_db = MONGO_DB
        b_col = RAW_COLLECTION
        b_batch = BATCH_SIZE

        df.foreachPartition(
            lambda partition: _write_partition_to_mongo(
                partition, b_run_id, b_source, b_uri, b_db, b_col, b_batch
            )
        )

        write_elapsed = time.time() - write_start
        print(f"[Spark Loader] MongoDB write time: {write_elapsed:.2f}s")

        # Verify loaded count
        raw_count = db[RAW_COLLECTION].count_documents({"run_id": run_id})

        elapsed = time.time() - start_time
        rate = raw_count / elapsed if elapsed > 0 else 0

        print(f"\n[Spark Loader] Complete!")
        print(f"  Total rows read: {total_rows:,}")
        print(f"  Loaded to raw: {raw_count:,}")
        print(f"  Partitions: {num_partitions}")
        print(f"  Total time: {elapsed:.2f}s")
        print(f"  Rate: {rate:,.0f} rec/s")

        # Show Spark UI info
        sc = spark.sparkContext
        print(f"\n[Spark Loader] Spark UI: http://localhost:4040")
        print(f"[Spark Loader] App ID: {sc.applicationId}")

        return {
            "rows_read": total_rows,
            "raw_loaded": raw_count,
            "batch_size": 0,
            "partitions": num_partitions,
        }

    finally:
        spark.stop()
        print("[Spark Loader] SparkSession stopped.")
