"""Part 3 - Integrated analytics: Customer Lifetime Value (CLV) with engagement.

Business question
  "Which customers are worth the most over the next 12 months, and which browsing
   behaviours distinguish them, so that marketing can protect the top tier and
   nurture the next one?"

Data sources (each stored where it fits best)
  MongoDB  users         profile: age group, state, registration date (document model)
  MongoDB  transactions  orders with embedded, margin-enriched line items:
                         a $group pipeline returns per-user orders, revenue, gross margin,
                         first/last purchase (aggregation pushed down to the database)
  HBase    user_sessions engagement stream: one prefix scan per user, reading only the
                         's' family, gives sessions, time on site, pages, abandonment,
                         recency (row key user_id#reverse_ts -> contiguous, newest first)
  Spark                  joins the three sources, computes CLV and segments, correlations

CLV model (simple, transparent, margin-based)
  monthly_margin   = gross margin over the 90-day window / 3
  r                = month-over-month buyer retention measured in the data (Spark cohort job)
  d                = 1% monthly discount rate
  CLV_12m          = monthly_margin * sum_{t=1..12} (r^t / (1+d)^t)
  Users are split into four CLV tiers (quartiles) and tiers are profiled with engagement.

Usage:  python -m part3_integration.integrated_clv
"""
import os
import time

import pandas as pd
from pymongo import MongoClient
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from common import config
from common.hbase_client import get_connection
from common.spark_utils import get_spark, save_csv
from part1_hbase.query_hbase import user_engagement

OBS_MONTHS = 3          # 90-day observation window
DISCOUNT = 0.01         # monthly discount rate
HORIZON = 12            # months
REF_DATE = "2026-10-31 23:59:59"


def mongo_customer_value():
    """Per-user value metrics computed inside MongoDB (aggregation pushdown)."""
    db = MongoClient(config.MONGO_URI)[config.MONGO_DB]
    pipeline = [
        {"$addFields": {"net_ratio": {"$cond": [{"$gt": ["$subtotal", 0]},
                                                {"$divide": ["$total", "$subtotal"]}, 1]}}},
        {"$addFields": {"margin": {"$sum": {"$map": {
            "input": "$items", "as": "i",
            "in": {"$multiply": ["$$i.subtotal", "$net_ratio", "$$i.profit_margin"]}}}}}},
        {"$group": {"_id": "$user_id",
                    "orders": {"$sum": 1},
                    "revenue": {"$sum": "$total"},
                    "gross_margin": {"$sum": "$margin"},
                    "discount_total": {"$sum": "$discount"},
                    "web_orders": {"$sum": {"$cond": [{"$eq": ["$channel", "web_session"]}, 1, 0]}},
                    "first_purchase": {"$min": "$timestamp"},
                    "last_purchase": {"$max": "$timestamp"}}},
    ]
    t = time.time()
    value = pd.DataFrame(db.transactions.aggregate(pipeline, allowDiskUse=True)).rename(columns={"_id": "user_id"})
    profiles = pd.DataFrame(db.users.find({}, {"age_group": 1, "geo_data.state": 1,
                                               "registration_date": 1}))
    profiles["state"] = profiles["geo_data"].map(lambda g: g["state"])
    profiles = profiles.rename(columns={"_id": "user_id"}).drop(columns=["geo_data"])
    print(f"MongoDB: {len(value):,} customers with orders, {len(profiles):,} profiles "
          f"({time.time() - t:.1f}s)")
    return profiles, value


def hbase_engagement():
    """Per-user engagement from HBase user_sessions (one prefix scan per sampled user)."""
    conn = get_connection()
    tbl = conn.table("user_sessions")
    t = time.time()
    # distinct users present in the table: jump from user to user using the row-key prefix
    users, start = [], b""
    while True:
        nxt = next(tbl.scan(row_start=start, columns=[b"s:start"], limit=1), None)
        if nxt is None:
            break
        uid = nxt[0].split(b"#")[0]
        users.append(uid.decode())
        start = uid + b"$"            # '$' sorts right after '#': skip to the next user
    rows = [user_engagement(tbl, u) for u in users]
    print(f"HBase: engagement for {len(rows):,} users ({time.time() - t:.1f}s)")
    return pd.DataFrame(rows)


def main():
    t0 = time.time()
    profiles_pd, value_pd = mongo_customer_value()
    engagement_pd = hbase_engagement()

    spark = get_spark("05-integrated-clv")
    profiles = spark.createDataFrame(profiles_pd)
    value = spark.createDataFrame(value_pd)
    engagement = spark.createDataFrame(engagement_pd.where(pd.notnull(engagement_pd), None))

    # retention r measured from the order data (share of month-m buyers who buy in m+1)
    retention = pd.read_csv(os.path.join(config.RESULTS_DIR, "spark_monthly_retention.csv"))
    r = min(float(retention["retention_pct"].mean()) / 100, 0.99)
    factor = sum((r ** t) / ((1 + DISCOUNT) ** t) for t in range(1, HORIZON + 1))
    print(f"monthly retention r = {r:.4f}  ->  12-month multiplier = {factor:.2f}")

    # ---- Spark join: HBase engagement sample x MongoDB value x MongoDB profile ----
    df = (engagement.join(value, "user_id", "left").join(profiles, "user_id", "left")
          .fillna({"orders": 0, "revenue": 0.0, "gross_margin": 0.0, "web_orders": 0})
          .withColumn("aov", F.when(F.col("orders") > 0, F.col("revenue") / F.col("orders")))
          .withColumn("margin_rate", F.when(F.col("revenue") > 0, F.col("gross_margin") / F.col("revenue")))
          .withColumn("orders_per_month", F.col("orders") / OBS_MONTHS)
          .withColumn("monthly_margin", F.col("gross_margin") / OBS_MONTHS)
          .withColumn("clv_12m", F.round(F.col("monthly_margin") * F.lit(factor), 2))
          .withColumn("sessions_per_week", F.col("sessions") / F.lit(90 / 7))
          .withColumn("days_since_last_session",
                      F.datediff(F.lit(REF_DATE).cast("timestamp"), F.to_timestamp("last_session")))
          .withColumn("session_to_order_pct",
                      F.when(F.col("sessions") > 0, 100 * F.col("web_orders") / F.col("sessions"))))
    df = df.withColumn("clv_tier", F.ntile(4).over(Window.orderBy("clv_12m")))
    df = df.withColumn("clv_tier", F.concat(F.lit("T"), (5 - F.col("clv_tier")).cast("string")))  # T1 = top
    df.cache()
    df.createOrReplaceTempView("customer_360")

    per_user = df.select("user_id", "age_group", "state", "orders", "revenue", "gross_margin",
                         "aov", "margin_rate", "clv_12m", "clv_tier", "sessions",
                         "sessions_per_week", "avg_session_sec", "avg_pages",
                         "cart_abandonment_rate", "session_to_order_pct", "days_since_last_session")
    save_csv(per_user, "integrated_clv_per_user")

    tiers = spark.sql("""
        SELECT clv_tier,
               COUNT(*)                                   AS customers,
               ROUND(AVG(clv_12m), 0)                     AS avg_clv_12m,
               ROUND(SUM(clv_12m) / SUM(SUM(clv_12m)) OVER () * 100, 1) AS pct_of_total_clv,
               ROUND(AVG(orders), 1)                      AS avg_orders_90d,
               ROUND(AVG(aov), 2)                         AS avg_order_value,
               ROUND(100 * AVG(margin_rate), 2)           AS margin_rate_pct,
               ROUND(AVG(sessions_per_week), 2)           AS sessions_per_week,
               ROUND(AVG(avg_session_sec) / 60, 1)        AS avg_session_min,
               ROUND(AVG(avg_pages), 2)                   AS avg_pages,
               ROUND(100 * AVG(cart_abandonment_rate), 1) AS cart_abandonment_pct,
               ROUND(AVG(session_to_order_pct), 1)        AS session_to_order_pct
        FROM customer_360 GROUP BY clv_tier ORDER BY clv_tier
    """)
    save_csv(tiers, "integrated_clv_tiers")
    tiers.show(truncate=False)

    # which engagement signals move with CLV? (Pearson correlation)
    metrics = ["sessions", "avg_session_sec", "avg_pages", "cart_abandonment_rate",
               "session_to_order_pct", "aov", "orders", "margin_rate"]
    corr = spark.createDataFrame([(m, float(df.stat.corr("clv_12m", m))) for m in metrics],
                                 ["metric", "corr_with_clv"]).orderBy(F.desc(F.abs("corr_with_clv")))
    save_csv(corr, "integrated_clv_correlations")
    corr.show(truncate=False)

    by_age = spark.sql("""
        SELECT age_group, COUNT(*) AS customers, ROUND(AVG(clv_12m), 0) AS avg_clv_12m,
               ROUND(100 * AVG(CASE WHEN clv_tier = 'T1' THEN 1 ELSE 0 END), 1) AS pct_top_tier
        FROM customer_360 GROUP BY age_group ORDER BY age_group""")
    save_csv(by_age, "integrated_clv_by_age")
    by_age.show()

    pd.DataFrame([{"retention_r": r, "multiplier_12m": factor, "discount_rate": DISCOUNT,
                   "customers": per_user.count()}]).to_csv(
        os.path.join(config.RESULTS_DIR, "integrated_clv_model.csv"), index=False)
    print(f"done in {time.time() - t0:.0f}s")
    spark.stop()


if __name__ == "__main__":
    main()
