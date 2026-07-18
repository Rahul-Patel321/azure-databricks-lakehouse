"""
Unit tests for the Silver dedup/clean logic and Gold aggregation logic.
Runs on local PySpark — no Databricks cluster needed for CI.
"""
import sys
import os
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "notebooks"))

import pytest
from pyspark.sql import SparkSession

import importlib
silver = importlib.import_module("02_silver_transformation")
gold = importlib.import_module("03_gold_aggregation")


@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local[1]").appName("test-lakehouse").getOrCreate()


def test_silver_drops_rows_missing_ids(spark):
    data = [
        ("o1", "CUST1", 100.0, datetime(2026, 7, 1), "North", datetime(2026, 7, 1, 9, 0)),
        (None, "CUST2", 200.0, datetime(2026, 7, 1), "South", datetime(2026, 7, 1, 9, 5)),
    ]
    columns = ["order_id", "customer_id", "amount", "order_ts", "region", "_ingested_ts"]
    df = spark.createDataFrame(data, columns)

    result = silver.clean_and_dedupe(df)
    assert result.count() == 1
    assert result.collect()[0]["order_id"] == "o1"


def test_silver_dedupes_keeping_latest_ingested(spark):
    data = [
        ("o1", "CUST1", 100.0, datetime(2026, 7, 1), "North", datetime(2026, 7, 1, 9, 0)),
        ("o1", "CUST1", 150.0, datetime(2026, 7, 1), "North", datetime(2026, 7, 1, 10, 0)),
    ]
    columns = ["order_id", "customer_id", "amount", "order_ts", "region", "_ingested_ts"]
    df = spark.createDataFrame(data, columns)

    result = silver.clean_and_dedupe(df)
    assert result.count() == 1
    assert float(result.collect()[0]["amount"]) == 150.0


def test_gold_aggregates_by_region_and_date(spark):
    data = [
        ("o1", "CUST1", 100.0, datetime(2026, 7, 1, 9, 0), "North"),
        ("o2", "CUST2", 200.0, datetime(2026, 7, 1, 10, 0), "North"),
        ("o3", "CUST3", 300.0, datetime(2026, 7, 1, 11, 0), "South"),
    ]
    columns = ["order_id", "customer_id", "amount", "order_ts", "region"]
    df = spark.createDataFrame(data, columns)

    result = {r["region"]: r for r in gold.build_gold_summary(df).collect()}
    assert result["North"]["order_count"] == 2
    assert result["North"]["total_revenue"] == 300.0
    assert result["South"]["order_count"] == 1
