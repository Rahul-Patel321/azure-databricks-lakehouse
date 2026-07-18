"""
Bronze Layer: Raw -> Delta
--------------------------
Reads raw sales/orders files landed by ADF into ADLS Gen2 `raw/`, and writes
them unmodified into a Bronze Delta table. Append-only, schema-flexible.

In a live Databricks workspace, swap the batch read below for Auto Loader
to incrementally discover new files without re-listing the whole directory:

    df = (spark.readStream.format("cloudFiles")
          .option("cloudFiles.format", "csv")
          .option("cloudFiles.schemaLocation", SCHEMA_LOCATION)
          .load(RAW_PATH))

This repo uses a batch read so it runs outside a Databricks cluster.
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

RAW_PATH = "raw/"
BRONZE_PATH = "delta/bronze/sales"


def get_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("bronze-ingestion")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


def main():
    spark = get_spark()

    raw_df = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(RAW_PATH)
        .withColumn("_ingested_ts", F.current_timestamp())
    )

    (
        raw_df.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .save(BRONZE_PATH)
    )

    print(f"Bronze ingestion complete. {raw_df.count()} records written.")


if __name__ == "__main__":
    main()
