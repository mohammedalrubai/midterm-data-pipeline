"""
settings.py - Centralized project configuration.
All important settings are here or read from environment variables.
No hardcoded values inside the source code.
"""
import os

# ──────────────────────────────────────────────
# MongoDB Connection
# ──────────────────────────────────────────────
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.environ.get("MONGO_DB", "midterm_pipeline")

# ──────────────────────────────────────────────
# Collection Names
# ──────────────────────────────────────────────
RAW_COLLECTION = "orders_raw"
VALIDATED_COLLECTION = "orders_validated"
QUARANTINE_COLLECTION = "orders_quarantine"

# ──────────────────────────────────────────────
# File Router – Threshold (MB)
# Files <= threshold → Python Batch
# Files >  threshold → PySpark
# ──────────────────────────────────────────────
SMALL_FILE_THRESHOLD_MB = int(os.environ.get("SMALL_FILE_THRESHOLD_MB", "200"))

# ──────────────────────────────────────────────
# Python Batch Loader
# ──────────────────────────────────────────────
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "5000"))

# ──────────────────────────────────────────────
# PySpark
# ──────────────────────────────────────────────
SPARK_APP_NAME = "MidtermDataPipeline"
# MongoDB Spark Connector package (auto-downloaded by Spark)
SPARK_MONGO_CONNECTOR = "org.mongodb.spark:mongo-spark-connector_2.12:10.4.0"

# ──────────────────────────────────────────────
# Sample Generator
# ──────────────────────────────────────────────
DEFAULT_SAMPLE_ROWS = int(os.environ.get("DEFAULT_SAMPLE_ROWS", "100000"))

# ──────────────────────────────────────────────
# Paths (relative to project root)
# ──────────────────────────────────────────────
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.environ.get(
    "DATA_DIR",
    os.path.join(os.path.dirname(PROJECT_ROOT), "data")  # ../data/ where professor's files are
)
REPORTS_DIR = os.path.join(PROJECT_ROOT, "reports")
RESULTS_FILE = os.path.join(REPORTS_DIR, "results.json")

# ──────────────────────────────────────────────
# CSV Columns (as defined by the professor's dataset)
# ──────────────────────────────────────────────
CSV_COLUMNS = [
    "order_id", "order_date", "status", "customer_id",
    "customer_name", "customer_phone", "customer_email",
    "city", "district", "delivery_type", "delivery_cost",
    "payment_method", "payment_status", "payment_amount",
    "currency", "total_amount", "items_json",
]

# ──────────────────────────────────────────────
# Valid Statuses (canonical values)
# ──────────────────────────────────────────────
VALID_STATUSES = ["مؤكد", "قيد الانتظار", "قيد الشحن", "تم التسليم", "مرتجع", "ملغي"]
