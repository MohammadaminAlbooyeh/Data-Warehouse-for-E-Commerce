from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# Spark session
spark = SparkSession.builder \
    .appName("EcommerceOrdersStreaming") \
    .getOrCreate()

# Define schema for orders
schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("order_date", StringType(), True),
])

# Read stream from Kafka
orders_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "orders_topic") \
    .load()

# Parse JSON payload
orders_parsed = orders_stream.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*")

# Write to Bronze layer (append mode)
query = orders_parsed.writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("path", "data/bronze/streaming_orders/") \
    .option("checkpointLocation", "data/bronze/checkpoints/") \
    .start()

query.awaitTermination()
