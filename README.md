# Distributed Multi-Model Analytics for E-commerce Data

AUCA Big Data Analytics, final project. MongoDB (document model), HBase (wide-column
model) and Apache Spark (distributed processing) on a synthetic e-commerce dataset of
10,000 users, 5,000 products, 2,000,000 sessions and 500,000 transactions over 90 days.

## Repository layout

| Path | Content |
|---|---|
| `data_generation/dataset_generator.py` | Corrected dataset generator (changes listed in its docstring) |
| `data_generation/validate_dataset.py` | Checks every relationship rule of the brief: `python -I data_generation/validate_dataset.py data` |
| `part1_mongodb/load_mongodb.py` | Document-model loader: catalogue, users with embedded purchase summary, transactions with enriched line items, indexes |
| `part1_mongodb/aggregations.py` | 3 aggregation pipelines: revenue & margin by category/month, top products, `$facet` user segmentation |
| `part1_hbase/schema.hbase` | HBase shell DDL (`user_sessions`, `product_metrics`) with row-key design notes |
| `part1_hbase/load_hbase.py` | Loads sessions of a 2,000-user sample + daily metrics for all products |
| `part1_hbase/query_hbase.py`, `sample_queries.hbase` | User session retrieval (latest N, time range, journey), product conversion scans |
| `part2_spark/spark_clean.py` | Cleaning & normalisation to Parquet (explicit schemas, dedupe, nulls, exploded tables) |
| `part2_spark/spark_recommendations.py` | Bought-together / viewed-together (support, confidence, lift) + blended top-5 recommendations |
| `part2_spark/spark_cohort.py` | Registration-month cohort analysis, month-over-month retention |
| `part2_spark/spark_sql_analytics.py` | 10 Spark SQL queries incl. joins on data loaded from MongoDB and HBase |
| `part3_integration/integrated_clv.py` | Integrated CLV: MongoDB value + HBase engagement joined in Spark |
| `part4_visualization/visualize.py` | 7 static figures (`outputs/figures`) |
| `outputs/results` | Every result table as CSV, the Mongo pipelines (JSON) and Spark SQL (`.sql`) |
| `report/` | Technical report (PDF + Word) |
| `common/` | Configuration, Spark session, HBase connection factory |

## Reproduce

```bash
pip install -r requirements.txt        # Java 17+ is needed for Spark
docker compose up -d                   # MongoDB :27017 and HBase Thrift :9090
./run_all.sh                           # ~15 min on a 2-core / 8 GB machine
```

Each step can also be run on its own, e.g. `python -m part1_mongodb.aggregations`.
Settings (Mongo URI, HBase host, sample size, Spark master) are environment variables,
see `common/config.py`. A fixed `--reference-date` makes the dataset identical on
every run. For a quick test use `python data_generation/dataset_generator.py --scale 0.01 --format jsonl`.

**HBase note.** `common/hbase_client.py` connects to HBase through Thrift (happybase).
If no Thrift server is reachable it falls back to `LocalHBaseEmulator`, a single-file
stand-in implementing the happybase calls used here (sorted row keys, families,
prefix/range scans) so the code can be tested without HBase. The results in the report
were produced with the emulator because no HBase server could be installed in the build
environment; MongoDB 8.0 and Spark 4 ran for real. The HBase code is unchanged between
both modes; run it against `docker compose` HBase to reproduce on real HBase.

## Key results

| | |
|---|---|
| Revenue (90 days) | $387.7M from 500k orders; 80% via web sessions, 20% direct |
| Funnel | 2.0M sessions → 65% add to cart → 29% reach checkout → 20% purchase; 69% of carts abandoned |
| Stock-outs | 1,927 products sold out; they earned $51k each vs $102k for in-stock products |
| Margin | category margin rates 18–36% on similar revenue ($13–18M) |
| CLV | top 25% of customers = 31% of projected 12-month value; cart abandonment r = −0.56 with CLV |
