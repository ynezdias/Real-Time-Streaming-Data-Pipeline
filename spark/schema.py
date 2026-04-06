from pyspark.sql.types import *

event_schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("session_id", StringType(), True),
    StructField("device", StringType(), True),
    StructField("page", StringType(), True),
    StructField("country", StringType(), True),
    StructField("amount", DoubleType(), True),
])