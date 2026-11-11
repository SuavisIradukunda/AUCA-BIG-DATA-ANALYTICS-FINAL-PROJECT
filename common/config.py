"""Shared configuration for every part of the project.

All settings can be overridden with environment variables so the same code runs
on a laptop, in Docker, or on a cluster.
"""
import os

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Raw dataset produced by data_generation/dataset_generator.py --format jsonl
DATA_DIR = os.environ.get("DATA_DIR", os.path.join(ROOT, "data"))
# Cleaned Parquet produced by part2_spark/spark_clean.py
PARQUET_DIR = os.environ.get("PARQUET_DIR", os.path.join(DATA_DIR, "parquet"))

OUTPUT_DIR = os.path.join(ROOT, "outputs")
RESULTS_DIR = os.path.join(OUTPUT_DIR, "results")
FIGURES_DIR = os.path.join(OUTPUT_DIR, "figures")

# MongoDB
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://127.0.0.1:27017")
MONGO_DB = os.environ.get("MONGO_DB", "ecommerce")

# HBase (Thrift server, used by happybase). If unreachable, a local emulator is used.
HBASE_HOST = os.environ.get("HBASE_HOST", "127.0.0.1")
HBASE_PORT = int(os.environ.get("HBASE_PORT", "9090"))
HBASE_EMULATOR_PATH = os.environ.get("HBASE_EMULATOR_PATH",
                                     os.path.join(DATA_DIR, "hbase_emulator.sqlite"))

# Size of the HBase user-session sample: all sessions of this many users
HBASE_SAMPLE_USERS = int(os.environ.get("HBASE_SAMPLE_USERS", "2000"))

# Spark
SPARK_MASTER = os.environ.get("SPARK_MASTER", "local[*]")
SPARK_DRIVER_MEMORY = os.environ.get("SPARK_DRIVER_MEMORY", "4g")

for d in (RESULTS_DIR, FIGURES_DIR):
    os.makedirs(d, exist_ok=True)


def raw_path(name):
    """Path of a raw JSONL file, e.g. raw_path('users') or raw_path('sessions_*')."""
    return os.path.join(DATA_DIR, f"{name}.jsonl")
