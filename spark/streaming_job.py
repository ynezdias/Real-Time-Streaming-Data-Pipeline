import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from spark.schema import event_schema
from spark.transformations import clean_events, enrich_events, window_aggregations
from storage.postgres_writer import write_aggregates
from dotenv import load_dotenv

load_dotenv()

spark = SparkSession.builder \
    .appName("UserActivityStreaming") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")) \
    .option("subscribe", "user-events") \
    .option("startingOffsets", "latest") \
    .load()

parsed_df = raw_df.select(
    F.from_json(F.col("value").cast("string"), event_schema).alias("data")
).select("data.*")

clean_df = clean_events(parsed_df)
enriched_df = enrich_events(clean_df)
windowed_df = window_aggregations(enriched_df)

s3_query = enriched_df.writeStream \
    .format("parquet") \
    .option("path", os.getenv("S3_PATH", "output/events/")) \
    .option("checkpointLocation", "output/checkpoints/") \
    .partitionBy("event_date", "event_type") \
    .outputMode("append") \
    .trigger(processingTime="30 seconds") \
    .start()

def write_batch_to_postgres(batch_df, batch_id):
    rows = batch_df.collect()
    if rows:
        write_aggregates(rows)
        print(f"Batch {batch_id}: wrote {len(rows)} rows to PostgreSQL")

pg_query = windowed_df.writeStream \
    .foreachBatch(write_batch_to_postgres) \
    .outputMode("update") \
    .trigger(processingTime="30 seconds") \
    .start()

print("Streaming queries started. Waiting for data...")
pg_query.awaitTermination()