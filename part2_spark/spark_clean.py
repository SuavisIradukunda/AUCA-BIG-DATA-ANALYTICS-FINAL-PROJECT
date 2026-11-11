"""Part 2 - Spark batch job 1: clean and normalise the raw JSON dataset.

Input : data/*.jsonl (JSON Lines, one record per line -> splittable, read in parallel)
Output: data/parquet/<table>  (columnar, compressed, typed; read by every later job)

Pipeline
  1. Read with EXPLICIT schemas (no schema inference pass over 4 GB; wrong types fail fast).
  2. Standardise formats: ISO strings -> TimestampType, trimmed/upper-cased codes,
     lower-cased enums, rounded money.
  3. Handle missing / invalid values: drop rows without keys, de-duplicate on primary
     keys, fill missing optional fields, clamp impossible values (negative stock/price),
     flag records that reference unknown users/products.
  4. Normalise nested structures into analysis tables:
       sessions -> sessions (1 row/session), page_views (1 row/view), cart_items
       transactions -> transactions, transaction_items (discount allocated per item)
       categories -> subcategories, products -> product_price_history
  5. Write Parquet; page_views / sessions are partitioned by date for pruning.
  6. Write a data-quality report (rows in / out per table).

Usage:  python -m part2_spark.spark_clean
"""
import glob
import time

from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

from common import config
from common.spark_utils import get_spark, parquet

# ---------------------------------------------------------------------------
# Explicit schemas
# ---------------------------------------------------------------------------
GEO = T.StructType([T.StructField("city", T.StringType()), T.StructField("state", T.StringType()),
                    T.StructField("country", T.StringType()),
                    T.StructField("ip_address", T.StringType())])

USER_SCHEMA = T.StructType([
    T.StructField("user_id", T.StringType()), T.StructField("age", T.IntegerType()),
    T.StructField("geo_data", GEO), T.StructField("registration_date", T.StringType()),
    T.StructField("last_active", T.StringType())])

CATEGORY_SCHEMA = T.StructType([
    T.StructField("category_id", T.StringType()), T.StructField("name", T.StringType()),
    T.StructField("subcategories", T.ArrayType(T.StructType([
        T.StructField("subcategory_id", T.StringType()), T.StructField("name", T.StringType()),
        T.StructField("profit_margin", T.DoubleType())])))])

PRODUCT_SCHEMA = T.StructType([
    T.StructField("product_id", T.StringType()), T.StructField("name", T.StringType()),
    T.StructField("category_id", T.StringType()), T.StructField("subcategory_id", T.StringType()),
    T.StructField("base_price", T.DoubleType()), T.StructField("current_stock", T.IntegerType()),
    T.StructField("is_active", T.BooleanType()),
    T.StructField("price_history", T.ArrayType(T.StructType([
        T.StructField("price", T.DoubleType()), T.StructField("date", T.StringType())]))),
    T.StructField("creation_date", T.StringType())])

SESSION_SCHEMA = T.StructType([
    T.StructField("session_id", T.StringType()), T.StructField("user_id", T.StringType()),
    T.StructField("start_time", T.StringType()), T.StructField("end_time", T.StringType()),
    T.StructField("duration_seconds", T.IntegerType()), T.StructField("geo_data", GEO),
    T.StructField("device_profile", T.StructType([
        T.StructField("type", T.StringType()), T.StructField("os", T.StringType()),
        T.StructField("browser", T.StringType())])),
    T.StructField("viewed_products", T.ArrayType(T.StringType())),
    T.StructField("page_views", T.ArrayType(T.StructType([
        T.StructField("timestamp", T.StringType()), T.StructField("page_type", T.StringType()),
        T.StructField("product_id", T.StringType()), T.StructField("category_id", T.StringType()),
        T.StructField("view_duration", T.IntegerType())]))),
    # cart_contents is an object keyed by product_id -> MapType
    T.StructField("cart_contents", T.MapType(T.StringType(), T.StructType([
        T.StructField("quantity", T.IntegerType()), T.StructField("price", T.DoubleType())]))),
    T.StructField("conversion_status", T.StringType()), T.StructField("referrer", T.StringType())])

TRANSACTION_SCHEMA = T.StructType([
    T.StructField("transaction_id", T.StringType()), T.StructField("session_id", T.StringType()),
    T.StructField("user_id", T.StringType()), T.StructField("timestamp", T.StringType()),
    T.StructField("items", T.ArrayType(T.StructType([
        T.StructField("product_id", T.StringType()), T.StructField("quantity", T.IntegerType()),
        T.StructField("unit_price", T.DoubleType()), T.StructField("subtotal", T.DoubleType())]))),
    T.StructField("subtotal", T.DoubleType()), T.StructField("discount", T.DoubleType()),
    T.StructField("total", T.DoubleType()), T.StructField("payment_method", T.StringType()),
    T.StructField("status", T.StringType())])


def ts(col):
    """ISO-8601 string -> timestamp (unparseable values become null)."""
    return F.to_timestamp(F.col(col))


def dedupe(df, key, order_col=None):
    """Keep one row per key (the latest by order_col when given)."""
    if order_col is None:
        return df.dropDuplicates([key])
    w = Window.partitionBy(key).orderBy(F.col(order_col).desc_nulls_last())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def main():
    spark = get_spark("01-clean-normalise")
    t0 = time.time()
    quality = []

    def report(name, raw_count, df):
        n = df.count()
        quality.append((name, raw_count, n, raw_count - n))
        print(f"{name:24s} raw={raw_count:>10,}  clean={n:>10,}  removed={raw_count - n:,}")

    # ---------------- users ----------------
    raw = spark.read.schema(USER_SCHEMA).json(config.raw_path("users"))
    raw_n = raw.count()
    median_age = raw.approxQuantile("age", [0.5], 0.01)[0] if raw.filter("age IS NOT NULL").count() else 35
    users = (raw.filter(F.col("user_id").isNotNull())
             .select(
                 "user_id",
                 F.coalesce("age", F.lit(int(median_age))).alias("age"),      # impute missing age
                 F.initcap(F.trim("geo_data.city")).alias("city"),
                 F.upper(F.trim("geo_data.state")).alias("state"),
                 F.coalesce(F.upper(F.trim("geo_data.country")), F.lit("US")).alias("country"),
                 ts("registration_date").alias("registration_date"),
                 ts("last_active").alias("last_active"))
             .filter(F.col("registration_date").isNotNull())
             .withColumn("age_group", F.when(F.col("age") < 25, "18-24").when(F.col("age") < 35, "25-34")
                         .when(F.col("age") < 45, "35-44").when(F.col("age") < 55, "45-54")
                         .otherwise("55+"))
             .withColumn("registration_month", F.date_format("registration_date", "yyyy-MM")))
    users = dedupe(users, "user_id", "last_active").cache()
    report("users", raw_n, users)
    users.write.mode("overwrite").parquet(parquet("users"))

    # ---------------- categories / subcategories ----------------
    raw = spark.read.schema(CATEGORY_SCHEMA).json(config.raw_path("categories"))
    categories = dedupe(raw.filter("category_id IS NOT NULL"), "category_id") \
        .select("category_id", F.trim("name").alias("category_name"), "subcategories")
    subcats = (categories.select("category_id", "category_name", F.explode("subcategories").alias("s"))
               .select("category_id", "category_name",
                       F.col("s.subcategory_id").alias("subcategory_id"),
                       F.initcap(F.trim("s.name")).alias("subcategory_name"),
                       F.coalesce("s.profit_margin", F.lit(0.0)).alias("profit_margin")))
    report("categories", raw.count(), categories)
    categories.drop("subcategories").write.mode("overwrite").parquet(parquet("categories"))
    subcats.write.mode("overwrite").parquet(parquet("subcategories"))

    # ---------------- products ----------------
    raw = spark.read.schema(PRODUCT_SCHEMA).json(config.raw_path("products"))
    raw_n = raw.count()
    products = (raw.filter(F.col("product_id").isNotNull())
                .withColumn("base_price", F.round(F.abs("base_price"), 2))
                .withColumn("current_stock", F.greatest(F.coalesce("current_stock", F.lit(0)), F.lit(0)))
                # an out-of-stock product cannot be sold -> treat as inactive
                .withColumn("is_active", F.coalesce("is_active", F.lit(False)) & (F.col("current_stock") > 0))
                .withColumn("creation_date", ts("creation_date"))
                .withColumn("n_price_changes", F.size("price_history") - 1))
    products = dedupe(products, "product_id")
    products = (products.join(F.broadcast(subcats.select("subcategory_id", "category_name",
                                                         "subcategory_name", "profit_margin")),
                              "subcategory_id", "left")
                .withColumn("profit_margin", F.coalesce("profit_margin", F.lit(0.0))))
    price_history = (products.select("product_id", F.posexplode("price_history").alias("pos", "h"))
                     .select("product_id", "pos", F.col("h.price").alias("price"),
                             F.to_timestamp("h.date").alias("effective_from")))
    products = products.drop("price_history").cache()
    report("products", raw_n, products)
    products.write.mode("overwrite").parquet(parquet("products"))
    price_history.write.mode("overwrite").parquet(parquet("product_price_history"))

    # ---------------- sessions ----------------
    raw = spark.read.schema(SESSION_SCHEMA).json(sorted(glob.glob(config.raw_path("sessions_*"))))
    raw_n = raw.count()
    known_users = users.select("user_id")
    sessions = (raw.filter(F.col("session_id").isNotNull() & F.col("user_id").isNotNull())
                .withColumn("start_time", ts("start_time"))
                .withColumn("end_time", ts("end_time"))
                .filter(F.col("start_time").isNotNull() & (F.col("duration_seconds") > 0))
                .withColumn("referrer", F.lower(F.coalesce("referrer", F.lit("unknown"))))
                .withColumn("conversion_status", F.lower("conversion_status"))
                .withColumn("page_views", F.coalesce("page_views", F.array()))
                .withColumn("viewed_products", F.coalesce("viewed_products", F.array().cast("array<string>"))))
    sessions = dedupe(sessions, "session_id")
    sessions = sessions.join(known_users, "user_id", "left_semi")      # referential integrity
    sessions = sessions.withColumn("session_date", F.to_date("start_time")).cache()

    sessions_flat = sessions.select(
        "session_id", "user_id", "start_time", "end_time", "duration_seconds", "session_date",
        F.hour("start_time").alias("start_hour"),
        F.dayofweek("start_time").alias("day_of_week"),
        F.col("geo_data.state").alias("state"), F.col("geo_data.ip_address").alias("ip_address"),
        F.lower("device_profile.type").alias("device_type"),
        F.col("device_profile.os").alias("os"), F.col("device_profile.browser").alias("browser"),
        "referrer", "conversion_status", "viewed_products",
        F.size("page_views").alias("n_page_views"),
        F.size("viewed_products").alias("n_products_viewed"),
        F.size(F.map_keys(F.coalesce("cart_contents", F.create_map()))).alias("n_cart_items"),
        F.exists("page_views", lambda p: p.page_type == "checkout").alias("reached_checkout"))
    report("sessions", raw_n, sessions_flat)
    sessions_flat.repartition("session_date").write.mode("overwrite") \
        .partitionBy("session_date").parquet(parquet("sessions"))

    page_views = (sessions.select("session_id", "user_id", "session_date",
                                  F.posexplode("page_views").alias("position", "pv"))
                  .select("session_id", "user_id", "session_date", "position",
                          F.to_timestamp("pv.timestamp").alias("view_time"),
                          F.lower(F.trim("pv.page_type")).alias("page_type"),
                          "pv.product_id", "pv.category_id",
                          F.coalesce("pv.view_duration", F.lit(0)).alias("view_duration")))
    page_views.repartition("session_date").write.mode("overwrite") \
        .partitionBy("session_date").parquet(parquet("page_views"))
    print(f"{'page_views':24s} rows={spark.read.parquet(parquet('page_views')).count():>10,}")

    cart_items = (sessions.select("session_id", "user_id", "session_date", "conversion_status",
                                  F.explode("cart_contents").alias("product_id", "c"))
                  .select("session_id", "user_id", "session_date", "conversion_status", "product_id",
                          F.col("c.quantity").alias("quantity"), F.col("c.price").alias("unit_price"))
                  .filter("quantity > 0"))
    cart_items.write.mode("overwrite").parquet(parquet("cart_items"))

    # ---------------- transactions ----------------
    raw = spark.read.schema(TRANSACTION_SCHEMA).json(config.raw_path("transactions"))
    raw_n = raw.count()
    txns = (raw.filter(F.col("transaction_id").isNotNull() & F.col("user_id").isNotNull())
            .withColumn("timestamp", ts("timestamp"))
            .filter(F.col("timestamp").isNotNull() & (F.size("items") > 0))
            .withColumn("discount", F.round(F.coalesce("discount", F.lit(0.0)), 2))
            .withColumn("subtotal", F.round("subtotal", 2))
            .withColumn("total", F.round("total", 2))
            .withColumn("payment_method", F.lower("payment_method"))
            .withColumn("status", F.lower("status"))
            .withColumn("channel", F.when(F.col("session_id").isNull(), "direct").otherwise("web_session"))
            .withColumn("txn_date", F.to_date("timestamp"))
            .withColumn("txn_month", F.date_format("timestamp", "yyyy-MM")))
    txns = dedupe(txns, "transaction_id").join(known_users, "user_id", "left_semi").cache()
    report("transactions", raw_n, txns)

    items = (txns.select("transaction_id", "user_id", "session_id", "timestamp", "txn_date",
                         "txn_month", "channel", "status",
                         (F.col("total") / F.col("subtotal")).alias("net_ratio"),
                         F.explode("items").alias("i"))
             .select("transaction_id", "user_id", "session_id", "timestamp", "txn_date", "txn_month",
                     "channel", "status", F.col("i.product_id").alias("product_id"),
                     F.col("i.quantity").alias("quantity"), F.col("i.unit_price").alias("unit_price"),
                     F.col("i.subtotal").alias("gross_amount"),
                     # order-level discount allocated proportionally to each line item
                     F.round(F.col("i.subtotal") * F.col("net_ratio"), 2).alias("net_amount"))
             .filter("quantity > 0 AND unit_price >= 0")
             .join(F.broadcast(products.select("product_id", "category_id", "category_name",
                                               "subcategory_id", "profit_margin")),
                   "product_id", "left")
             .withColumn("gross_margin", F.round(F.col("net_amount") * F.col("profit_margin"), 2)))
    txns.drop("items").write.mode("overwrite").parquet(parquet("transactions"))
    items.write.mode("overwrite").parquet(parquet("transaction_items"))
    print(f"{'transaction_items':24s} rows={spark.read.parquet(parquet('transaction_items')).count():>10,}")

    # ---------------- data quality report ----------------
    dq = spark.createDataFrame(quality, ["table", "raw_rows", "clean_rows", "removed_rows"])
    dq.toPandas().to_csv(f"{config.RESULTS_DIR}/spark_data_quality.csv", index=False)
    print(f"done in {time.time() - t0:.0f}s")
    spark.stop()


if __name__ == "__main__":
    main()
