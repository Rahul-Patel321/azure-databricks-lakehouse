"""
Silver Layer: Bronze -> Silver (incremental MERGE)
-----------------------------------------------------
Reads only Bronze records newer than Silver's last watermark, cleans and
deduplicates them, and MERGEs into Silver. This avoids reprocessing the
entire Bronze history on every run — the difference between a pipeline
that stays fast and one that slows down as data volume grows.
"""
import os

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

BRONZE_PATH = "delta/bronze/sales"
SILVER_PATH = "delta/silver/sales"
WATERMARK_FILE = "delta/_watermarks/silver_last_ts.txt"


def get_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("silver-merge")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


def read_watermark() -> str:
    if os.path.exists(WATERMARK_FILE):
        with open(WATERMARK_FILE) as f:
            return f.read().strip()
    return "1900-01-01T00:00:00"


def write_watermark(value: str) -> None:
    os.makedirs(os.path.dirname(WATERMARK_FILE), exist_ok=True)
    with open(WATERMARK_FILE, "w") as f:
        f.write(value)


def clean_and_dedupe(df):
    df = df.filter(F.col("order_id").isNotNull() & F.col("customer_id").isNotNull())

    window = Window.partitionBy("order_id").orderBy(F.col("_ingested_ts").desc())
    df = df.withColumn("_rn", F.row_number().over(window)).filter(F.col("_rn") == 1).drop("_rn")

    return df.withColumn("amount", F.col("amount").cast("decimal(12,2)"))


def merge_into_silver(spark, new_df):
    if not DeltaTable.isDeltaTable(spark, SILVER_PATH):
        new_df.write.format("delta").mode("overwrite").save(SILVER_PATH)
        return

    silver = DeltaTable.forPath(spark, SILVER_PATH)
    (
        silver.alias("target")
        .merge(new_df.alias("source"), "target.order_id = source.order_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def main():
    spark = get_spark()
    last_watermark = read_watermark()

    bronze_df = spark.read.format("delta").load(BRONZE_PATH)
    incremental_df = bronze_df.filter(F.col("_ingested_ts") > last_watermark)

    if incremental_df.rdd.isEmpty():
        print("No new Bronze records since last watermark. Nothing to do.")
        return

    silver_df = clean_and_dedupe(incremental_df)
    merge_into_silver(spark, silver_df)

    new_watermark = incremental_df.agg(F.max("_ingested_ts")).collect()[0][0]
    write_watermark(str(new_watermark))

    print(f"Silver merge complete. {silver_df.count()} records processed. "
          f"Watermark advanced to {new_watermark}.")


if __name__ == "__main__":
    main()
