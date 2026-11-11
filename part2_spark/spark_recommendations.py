"""Part 2 - Spark batch job 2: product recommendation indicators.

  A. "Frequently bought together" (market-basket analysis on transaction_items)
       support(A,B)    = baskets containing A and B / all baskets
       confidence(A->B)= baskets(A,B) / baskets(A)
       lift(A,B)       = confidence(A->B) / P(B)   (>1 = bought together more than chance)
  B. "Frequently viewed together" (co-views inside the same session, from viewed_products)
  C. Blended top-5 recommendations per product: normalised co-purchase and co-view
     scores, restricted to products that are still active and in stock.

Optimisation choices
  * pairs are generated with a self-join on basket id restricted to a < b, which halves
    the shuffle and removes self-pairs;
  * per-product counts are broadcast (5k rows) instead of shuffled;
  * a minimum pair count prunes the long tail before the expensive window ranking.

Usage:  python -m part2_spark.spark_recommendations
"""
import time

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from common.spark_utils import get_spark, parquet, save_csv

MIN_PAIR_COUNT = 3


def pair_stats(baskets, basket_col, min_count):
    """baskets: (basket_id, product_id) distinct rows -> pair counts with support/confidence/lift."""
    n_baskets = baskets.select(basket_col).distinct().count()
    item_counts = baskets.groupBy("product_id").agg(F.countDistinct(basket_col).alias("n"))
    a, b = baskets.alias("a"), baskets.alias("b")
    pairs = (a.join(b, (F.col(f"a.{basket_col}") == F.col(f"b.{basket_col}")) &
                    (F.col("a.product_id") < F.col("b.product_id")))
             .groupBy(F.col("a.product_id").alias("product_a"), F.col("b.product_id").alias("product_b"))
             .agg(F.count("*").alias("pair_count"))
             .filter(F.col("pair_count") >= min_count))
    ic = F.broadcast(item_counts)
    pairs = (pairs.join(ic.withColumnRenamed("product_id", "product_a").withColumnRenamed("n", "n_a"), "product_a")
             .join(ic.withColumnRenamed("product_id", "product_b").withColumnRenamed("n", "n_b"), "product_b")
             .withColumn("support", F.col("pair_count") / F.lit(n_baskets))
             .withColumn("confidence_a_to_b", F.col("pair_count") / F.col("n_a"))
             .withColumn("confidence_b_to_a", F.col("pair_count") / F.col("n_b"))
             .withColumn("lift", F.col("pair_count") * F.lit(n_baskets) / (F.col("n_a") * F.col("n_b"))))
    return pairs, n_baskets


def main():
    spark = get_spark("02-recommendations")
    t0 = time.time()
    items = spark.read.parquet(parquet("transaction_items"))
    sessions = spark.read.parquet(parquet("sessions"))
    products = spark.read.parquet(parquet("products")).select("product_id", "name", "category_name",
                                                              "is_active")

    # ---- A. bought together -------------------------------------------------
    baskets = items.select("transaction_id", "product_id").distinct()
    bought, n_txn = pair_stats(baskets, "transaction_id", MIN_PAIR_COUNT)
    bought = bought.cache()
    print(f"bought-together: {bought.count():,} pairs with >= {MIN_PAIR_COUNT} co-purchases "
          f"(from {n_txn:,} baskets)")
    top_bought = bought.orderBy(F.desc("pair_count"), F.desc("lift")).limit(20)
    save_csv(top_bought, "spark_bought_together_top20")
    top_bought.show(10, truncate=False)

    # ---- B. viewed together -------------------------------------------------
    views = (sessions.select("session_id", F.explode("viewed_products").alias("product_id"))
             .distinct())
    viewed, n_sess = pair_stats(views, "session_id", MIN_PAIR_COUNT)
    viewed = viewed.cache()
    print(f"viewed-together: {viewed.count():,} pairs (from {n_sess:,} sessions with views)")
    save_csv(viewed.orderBy(F.desc("pair_count")).limit(20), "spark_viewed_together_top20")

    # ---- C. blended recommendations ----------------------------------------
    def directed(df, kind):
        """Make pairs directional (A recommends B and B recommends A) with a 0-1 score."""
        mx = df.agg(F.max("pair_count")).first()[0]
        both = (df.select(F.col("product_a").alias("product_id"), F.col("product_b").alias("rec_id"),
                          "pair_count", "lift")
                .union(df.select(F.col("product_b").alias("product_id"),
                                 F.col("product_a").alias("rec_id"), "pair_count", "lift")))
        return both.select("product_id", "rec_id",
                           (F.col("pair_count") / F.lit(mx)).alias(f"{kind}_score"),
                           F.col("lift").alias(f"{kind}_lift"))

    blended = (directed(bought, "bought").join(directed(viewed, "viewed"), ["product_id", "rec_id"], "full")
               .fillna(0.0)
               # purchases are a stronger signal than views
               .withColumn("score", 0.7 * F.col("bought_score") + 0.3 * F.col("viewed_score"))
               .join(F.broadcast(products.filter("is_active")
                                 .select(F.col("product_id").alias("rec_id"))), "rec_id"))  # only sellable
    w = Window.partitionBy("product_id").orderBy(F.desc("score"))
    recs = (blended.withColumn("rank", F.row_number().over(w)).filter("rank <= 5")
            .join(F.broadcast(products.select(F.col("product_id").alias("rec_id"),
                                              F.col("name").alias("rec_name"),
                                              F.col("category_name").alias("rec_category"))), "rec_id"))
    recs.write.mode("overwrite").parquet(parquet("recommendations"))
    sample = recs.filter(F.col("product_id") == "prod_00123").orderBy("rank")
    save_csv(sample, "spark_recommendations_prod_00123")
    sample.show(truncate=False)
    print(f"products with recommendations: {recs.select('product_id').distinct().count():,}")

    # lift distribution: is there real affinity in the data?
    stats = bought.select(F.mean("lift").alias("mean_lift"), F.expr("percentile(lift, 0.5)").alias("median_lift"),
                          F.max("lift").alias("max_lift"), F.count("*").alias("pairs"))
    save_csv(stats, "spark_bought_together_lift_stats")
    stats.show()
    print(f"done in {time.time() - t0:.0f}s")
    spark.stop()


if __name__ == "__main__":
    main()
