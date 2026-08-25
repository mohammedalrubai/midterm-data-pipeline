"""
file_router.py - Routes input files to the correct processing engine.
Checks file size against the configurable threshold.
Single entry point; no separate programs for each engine.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import SMALL_FILE_THRESHOLD_MB


def route_file(file_path):
    """
    Determine which engine to use based on file size.

    Returns:
        engine: "python_batch" or "pyspark"
        file_size_mb: float
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Data file not found: {file_path}")

    file_size_bytes = os.path.getsize(file_path)
    file_size_mb = file_size_bytes / (1024 * 1024)

    if file_size_mb <= SMALL_FILE_THRESHOLD_MB:
        engine = "python_batch"
    else:
        engine = "pyspark"

    print(f"[Router] File: {os.path.basename(file_path)}")
    print(f"[Router] Size: {file_size_mb:.2f} MB")
    print(f"[Router] Threshold: {SMALL_FILE_THRESHOLD_MB} MB")
    print(f"[Router] Engine: {engine}")
    print(f"[Router] Reason: File {'<=' if engine == 'python_batch' else '>'} threshold")

    return engine, file_size_mb
