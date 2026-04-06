from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from schema import event_schema
from transformations import clean_events, enrich_events

spark = SparkSession.builder \
    .appName("UserActivityStreaming") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Read from Kafka
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "user-events") \
    .option("startingOffsets", "latest") \
    .load()

# Parse JSON payload
parsed_df = raw_df.select(
    F.from_json(F.col("value").cast("string"), event_schema).alias("data")
).select("data.*")

# Transform
clean_df = clean_events(parsed_df)
enriched_df = enrich_events(clean_df)

# Write to S3 as partitioned Parquet
query = enriched_df.writeStream \
    .format("parquet") \
    .option("path", "s3a://your-bucket/events/") \
    .option("checkpointLocation", "s3a://your-bucket/checkpoints/") \
    .partitionBy("event_date", "event_type") \
    .outputMode("append") \
    .trigger(processingTime="30 seconds") \
    .start()

query.awaitTermination()