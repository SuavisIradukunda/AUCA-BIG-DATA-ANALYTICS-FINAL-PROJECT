"""Part 2 - Spark batch job 3: cohort analysis of purchasing patterns.

Cohort  = users grouped by registration month (yyyy-MM).
Period  = months since registration of each purchase (0 = registration month).
Metrics per (cohort, period): active buyers, % of cohort active, revenue,
revenue per cohort member and per active buyer, orders per buyer.

A calendar-month view (cohort x transaction month) is also produced, because the
activity window covers only 90 days (Aug-Oct 2026) while registrations span
Feb-Aug 2026: each cohort is observed over a different range of "months since
registration".

Usage:  python -m part2_spark.spark_cohort
"""
import time

from pyspark.sql import functions as F

from common.spark_utils import get_spark, parquet, save_csv


def main():
    spark = get_spark("03-cohort")
    t0 = time.time()
    users = spark.read.parquet(parquet("users")).select("user_id", "registration_date",
                                                        "registration_month")
    txns = spark.read.parquet(parquet("transactions")).select("transaction_id", "user_id",
                                                              "timestamp", "total", "txn_month")

    cohort_size = users.groupBy("registration_month").agg(F.count("*").alias("cohort_size"))

    tx = (txns.join(users, "user_id")
          .withColumn("months_since_registration",
                      F.floor(F.months_between(F.trunc("timestamp", "month"),
                                               F.trunc("registration_date", "month"))).cast("int")))

    cohort = (tx.groupBy("registration_month", "months_since_registration")
              .agg(F.countDistinct("user_id").alias("active_buyers"),
                   F.count("transaction_id").alias("orders"),
                   F.round(F.sum("total"), 2).alias("revenue"))
              .join(cohort_size, "registration_month")
              .withColumn("pct_active", F.round(100 * F.col("active_buyers") / F.col("cohort_size"), 1))
              .withColumn("revenue_per_member", F.round(F.col("revenue") / F.col("cohort_size"), 2))
              .withColumn("revenue_per_buyer", F.round(F.col("revenue") / F.col("active_buyers"), 2))
              .withColumn("orders_per_buyer", F.round(F.col("orders") / F.col("active_buyers"), 2))
              .orderBy("registration_month", "months_since_registration"))
    save_csv(cohort, "spark_cohort_months_since_registration")
    cohort.show(60)

    calendar = (tx.groupBy("registration_month", "txn_month")
                .agg(F.countDistinct("user_id").alias("active_buyers"),
                     F.round(F.sum("total"), 2).alias("revenue"))
                .join(cohort_size, "registration_month")
                .withColumn("revenue_per_member", F.round(F.col("revenue") / F.col("cohort_size"), 2))
                .orderBy("registration_month", "txn_month"))
    save_csv(calendar, "spark_cohort_calendar_month")

    # month-over-month buyer retention (bought in month m AND in month m+1)
    monthly = txns.select("user_id", "txn_month").distinct()
    nxt = monthly.select("user_id", F.date_format(F.add_months(F.to_date(F.concat("txn_month", F.lit("-01"))), -1),
                                                  "yyyy-MM").alias("txn_month")).withColumn("next", F.lit(1))
    retention = (monthly.join(nxt, ["user_id", "txn_month"], "left")
                 .groupBy("txn_month").agg(F.count("*").alias("buyers"),
                                           F.sum(F.coalesce("next", F.lit(0))).alias("retained_next_month"))
                 .withColumn("retention_pct", F.round(100 * F.col("retained_next_month") / F.col("buyers"), 2))
                 .filter(F.col("txn_month") < F.lit("2026-10"))
                 .orderBy("txn_month"))
    save_csv(retention, "spark_monthly_retention")
    retention.show()
    print(f"done in {time.time() - t0:.0f}s")
    spark.stop()


if __name__ == "__main__":
    main()
