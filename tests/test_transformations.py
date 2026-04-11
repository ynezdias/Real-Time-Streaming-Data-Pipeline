import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import sys, os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'spark'))
from transformations import clean_events, enrich_events

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .appName("test") \
        .master("local[1]") \
        .getOrCreate()

@pytest.fixture(scope="session")
def sample_df(spark):
    data = [
        ("u1", "page_view", "2026-01-15T10:00:00", "s1", "mobile", "/home", "US", None),
        ("u2", "purchase",  "2026-01-15T11:00:00", "s2", "desktop", "/cart", "UK", 49.99),
        (None, "page_view", "2026-01-15T12:00:00", "s3", "tablet", "/home", "CA", None),  # bad row
        ("u4", "unknown",   "2026-01-15T13:00:00", "s4", "mobile", "/home", "US", None),  # bad event
    ]
    schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("session_id", StringType(), True),
        StructField("device", StringType(), True),
        StructField("page", StringType(), True),
        StructField("country", StringType(), True),
        StructField("amount", DoubleType(), True),
    ])
    return spark.createDataFrame(data, schema)

def test_clean_removes_null_user_id(spark, sample_df):
    result = clean_events(sample_df)
    assert result.count() == 2  # drops null user_id and unknown event_type

def test_clean_removes_invalid_event_type(spark, sample_df):
    result = clean_events(sample_df)
    event_types = [r.event_type for r in result.collect()]
    assert "unknown" not in event_types

def test_enrich_adds_event_date(spark, sample_df):
    cleaned = clean_events(sample_df)
    enriched = enrich_events(cleaned)
    assert "event_date" in enriched.columns
    assert "event_hour" in enriched.columns
    assert "is_purchase" in enriched.columns

def test_enrich_is_purchase_flag(spark, sample_df):
    cleaned = clean_events(sample_df)
    enriched = enrich_events(cleaned)
    purchases = enriched.filter("is_purchase = true").count()
    assert purchases == 1