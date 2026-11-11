#!/usr/bin/env bash
# Reproduce every result of the project end to end.
# Prerequisites: Python 3.10+, Java 17+ (for Spark), MongoDB on :27017,
# HBase Thrift on :9090 (docker compose up -d) - without HBase the local emulator is used.
set -euo pipefail
cd "$(dirname "$0")"
REF_DATE="2026-10-31T23:59:59"   # fixed end of the 90-day window -> identical data on every run

echo "== 0. Generate dataset (JSON Lines) =="
python data_generation/dataset_generator.py --format jsonl --output-dir data --reference-date "$REF_DATE"

echo "== 1a. MongoDB: load + aggregation pipelines =="
python -m part1_mongodb.load_mongodb --drop
python -m part1_mongodb.aggregations

echo "== 1b. HBase: create tables, load, sample queries =="
python -m part1_hbase.load_hbase --recreate
python -m part1_hbase.query_hbase

echo "== 2. Spark: clean, recommendations, cohort, Spark SQL =="
python -m part2_spark.spark_clean
python -m part2_spark.spark_recommendations
python -m part2_spark.spark_cohort
python -m part2_spark.spark_sql_analytics

echo "== 3. Integrated CLV (MongoDB + HBase + Spark) =="
python -m part3_integration.integrated_clv

echo "== 4. Visualisations =="
python -m part4_visualization.visualize

echo "Done. Results: outputs/results  Figures: outputs/figures"
