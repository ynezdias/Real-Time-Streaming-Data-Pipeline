from pyspark.sql import functions as F
from pyspark.sql.functions import window

def clean_events(df):
    return df.dropna(subset=["user_id", "event_type", "timestamp"]) \
             .filter(F.col("event_type").isin(
                 ["page_view","search","add_to_cart","purchase","logout"]))

def enrich_events(df):
    return df.withColumn("event_date", F.to_date(F.col("timestamp"))) \
             .withColumn("event_hour", F.hour(F.to_timestamp("timestamp"))) \
             .withColumn("is_purchase", F.col("event_type") == "purchase")

def window_aggregations(df):
    return df.groupBy(
        window(F.col("timestamp"), "5 minutes"),
        "event_type",
        "country"
    ).agg(
        F.count("*").alias("event_count"),
        F.countDistinct("user_id").alias("unique_users"),
        F.sum("amount").alias("total_revenue")
    )