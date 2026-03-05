import os
import time
import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, avg, stddev, count, sum, coalesce, lit, current_timestamp, expr

AWS_S3_BUCKET_NAME = os.getenv('AWS_S3_BUCKET_NAME')
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_KEY')

BASE_SILVER_PATH = f"s3a://{AWS_S3_BUCKET_NAME}/medallion/silver/"
BASE_GOLD_PATH = f"s3a://{AWS_S3_BUCKET_NAME}/medallion/gold/sentiment_impact/"
CHECKPOINT_PATH = f"s3a://{AWS_S3_BUCKET_NAME}/checkpoints/gold/"

spark = SparkSession.builder \
    .appName("Medallion-Gold-Sentiment-Correlation") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

def wait_for_s3_data(path, bucket):
    s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY)
    print(f"Checking for data in {path}...")
    while True:
        results = s3.list_objects_v2(Bucket=bucket, Prefix=path.split(f"{bucket}/")[1])
        if 'Contents' in results:
            print("Data found! Starting Spark stream...")
            break
        print("Waiting for initial data on S3... (10s)")
        time.sleep(10)

wait_for_s3_data(f"{BASE_SILVER_PATH}market/", AWS_S3_BUCKET_NAME)
wait_for_s3_data(f"{BASE_SILVER_PATH}news/", AWS_S3_BUCKET_NAME)
wait_for_s3_data(f"{BASE_SILVER_PATH}reddit/", AWS_S3_BUCKET_NAME)


market_df = spark.readStream.format("delta").load(f"{BASE_SILVER_PATH}market/")

market_agg = market_df.groupBy(
    window(col("timestamp"), "10 minutes"),
    col("ticker")
).agg(
    avg("price").alias("avg_price"),
    stddev("price").alias("stddev_price")
)

def write_to_delta(batch_df, batch_id):
    print(f"Batch {batch_id}: market rows = {batch_df.count()}", flush=True)

    if batch_df.isEmpty():
        print(f"Batch {batch_id}: empty, skipping.", flush=True)
        return

    # read reddit and news as static snapshots on every batch
    # picks the latest data without join complexity
    reddit_static = spark.read.format("delta").load(f"{BASE_SILVER_PATH}reddit/") \
        .filter(col("timestamp") >= expr("current_timestamp() - interval 2 hours"))

    news_static = spark.read.format("delta").load(f"{BASE_SILVER_PATH}news/") \
        .filter(col("timestamp") >= expr("current_timestamp() - interval 2 hours"))

    reddit_agg = reddit_static.groupBy("ticker").agg(
        (sum(col("sentiment_score") * col("impact_score")) / sum("impact_score")).alias("social_sentiment"),
        count("*").alias("social_volume")
    )

    news_agg = news_static.groupBy("ticker").agg(
        avg("sentiment_score").alias("news_sentiment"),
        count("*").alias("news_volume")
    )

    result = batch_df \
        .join(reddit_agg, "ticker", "left") \
        .join(news_agg, "ticker", "left") \
        .select(
            col("window.start").alias("window_start"),
            "ticker",
            "avg_price",
            coalesce(col("social_sentiment"), lit(0)).alias("social_sentiment"),
            coalesce(col("news_sentiment"), lit(0)).alias("news_sentiment"),
            coalesce(col("social_volume"), lit(0)).alias("social_volume"),
            coalesce(col("news_volume"), lit(0)).alias("news_volume")
        ) \
        .withColumn("combined_sentiment", (col("social_sentiment") + col("news_sentiment")) / 2)

    result.write \
        .format("delta") \
        .mode("append") \
        .save(BASE_GOLD_PATH)

query = market_agg.writeStream \
    .foreachBatch(write_to_delta) \
    .outputMode("update") \
    .option("checkpointLocation", CHECKPOINT_PATH) \
    .trigger(processingTime="1 minute") \
    .start()

query.awaitTermination()