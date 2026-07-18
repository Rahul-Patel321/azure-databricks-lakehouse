"""
Gold Layer: Silver -> Gold (aggregation + performance tuning)
------------------------------------------------------------
Builds reporting aggregates from Silver, then runs OPTIMIZE with ZORDER to
compact small files and co-locate data on the columns Power BI/Synapse
queries filter on most (region, sale_date) — directly targets dashboard
query latency, not just pipeline throughput.
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

SILVER_PATH = "delta/silver/sales"
GOLD_PATH = "delta/gold/sales_summary"


def get_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("gold-aggregation")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


def build_gold_summary(df):
    return (
        df.withColumn("sale_date", F.to_date("order_ts"))
        .groupBy("sale_date", "region")
        .agg(
            F.count("*").alias("order_count"),
            F.sum("amount").alias("total_revenue"),
            F.countDistinct("customer_id").alias("unique_customers"),
        )
    )


def main():
    spark = get_spark()
    silver_df = spark.read.format("delta").load(SILVER_PATH)

    gold_df = build_gold_summary(silver_df)
    gold_df.write.format("delta").mode("overwrite").save(GOLD_PATH)

    # OPTIMIZE + ZORDER: compacts small files from incremental writes and
    # co-locates rows by the columns most Power BI filters use.
    spark.sql(f"OPTIMIZE delta.`{GOLD_PATH}` ZORDER BY (region, sale_date)")

    print(f"Gold aggregation complete. {gold_df.count()} region-day rows written and optimized.")


if __name__ == "__main__":
    main()
