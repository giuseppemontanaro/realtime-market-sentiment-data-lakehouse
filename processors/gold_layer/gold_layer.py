import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, avg, min, max, count, stddev

AWS_S3_BUCKET_NAME = os.getenv('AWS_S3_BUCKET_NAME')
SILVER_PATH = f"s3a://{AWS_S3_BUCKET_NAME}/medallion/silver/market_data/"
GOLD_PATH = f"s3a://{AWS_S3_BUCKET_NAME}/medallion/gold/ticker_metrics/"
CHECKPOINT_PATH = f"s3a://{AWS_S3_BUCKET_NAME}/checkpoints/gold_layer/"

spark = SparkSession.builder \
    .appName("Medallion-Gold-Aggregations") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

silver_df = spark.readStream \
    .format("delta") \
    .load(SILVER_PATH)

gold_metrics_df = silver_df \
    .withWatermark("timestamp", "2 minutes") \
    .groupBy(
        window(col("timestamp"), "1 minute", "30 seconds"),
        col("ticker")
    ) \
    .agg(
        avg("price").alias("avg_price"),
        min("price").alias("min_price"),
        max("price").alias("max_price"),
        stddev("price").alias("price_volatility"),
        count("*").alias("event_count")
    ) \
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        "ticker",
        "avg_price",
        "min_price",
        "max_price",
        "price_volatility",
        "event_count"
    )

def write_to_gold_delta(batch_df, batch_id):
    batch_df.write \
        .format("delta") \
        .mode("append") \
        .save(GOLD_PATH)

query = gold_metrics_df.writeStream \
    .foreachBatch(write_to_gold_delta) \
    .outputMode("update") \
    .option("checkpointLocation", CHECKPOINT_PATH) \
    .trigger(processingTime="30 seconds") \
    .start()

print(f"Gold Layer is aggregating metrics to {GOLD_PATH}...")
query.awaitTermination()