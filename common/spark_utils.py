"""SparkSession factory and small helpers shared by the Spark jobs."""
import os

from pyspark.sql import SparkSession

from common import config


def get_spark(app_name):
    spark = (SparkSession.builder
             .appName(app_name)
             .master(config.SPARK_MASTER)
             .config("spark.driver.memory", config.SPARK_DRIVER_MEMORY)
             # small local cluster: fewer shuffle partitions than the default 200
             .config("spark.sql.shuffle.partitions", os.environ.get("SHUFFLE_PARTITIONS", "16"))
             .config("spark.sql.adaptive.enabled", "true")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.ui.showConsoleProgress", "false")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    return spark


def parquet(name):
    return os.path.join(config.PARQUET_DIR, name)


def save_csv(df, name):
    """Write a (small) result DataFrame to outputs/results/<name>.csv via pandas."""
    path = os.path.join(config.RESULTS_DIR, f"{name}.csv")
    df.toPandas().to_csv(path, index=False)
    return path
