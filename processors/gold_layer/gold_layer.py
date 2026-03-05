import os
import time
import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, avg, stddev, count, sum, coalesce, lit, current_timestamp, expr, when
from delta.tables import DeltaTable


AWS_S3_BUCKET_NAME = os.getenv('AWS_S3_BUCKET_NAME')
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_KEY')

BASE_SILVER_PATH = f"s3a://{AWS_S3_BUCKET_NAME}/medallion/silver/"
BASE_GOLD_PATH = f"s3a://{AWS_S3_BUCKET_NAME}/medallion/gold"
GOLD_LATEST_PATH = f"{BASE_GOLD_PATH}/ticker_latest/"
GOLD_SENTIMENT_PATH = f"{BASE_GOLD_PATH}/sentiment_impact/"
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
    window(col("timestamp"), "5 minutes"),
    col("ticker")
).agg(
    avg("price").alias("avg_price"),
    sum(col("price") * col("volume")).alias("sum_price_volume"),
    sum("volume").alias("sum_volume"),
    stddev("price").alias("volatility")
)

def write_to_delta(batch_df, batch_id):
    if batch_df.isEmpty():
        print(f"Batch {batch_id}: empty, skipping.", flush=True)
        return

    market_static = spark.read.format("delta").load(f"{BASE_SILVER_PATH}market/") \
        .filter(col("timestamp") >= expr("current_timestamp() - interval 24 hours"))

    reddit_static = spark.read.format("delta").load(f"{BASE_SILVER_PATH}reddit/") \
        .filter(col("timestamp") >= expr("current_timestamp() - interval 2 hours"))

    news_static = spark.read.format("delta").load(f"{BASE_SILVER_PATH}news/") \
        .filter(col("timestamp") >= expr("current_timestamp() - interval 2 hours"))
    
    avg_volume_static = market_static.groupBy("ticker").agg(
        avg("volume").alias("historical_avg_volume")
    )

    reddit_agg = reddit_static.groupBy("ticker").agg(
        (sum(col("sentiment_score") * col("impact_score")) / sum("impact_score")).alias("social_sentiment"),
        count("*").alias("social_volume")
    )

    news_agg = news_static.groupBy("ticker").agg(
        avg("sentiment_score").alias("news_sentiment"),
        count("*").alias("news_volume")
    )

    result = batch_df \
        .join(avg_volume_static, "ticker", "left") \
        .join(reddit_agg, "ticker", "left") \
        .join(news_agg, "ticker", "left") \
        .withColumn("vwap", 
            when(col("sum_volume") > 0, col("sum_price_volume") / col("sum_volume"))
            .otherwise(col("avg_price"))
        ) \
        .withColumn("rvol", col("sum_volume") / col("historical_avg_volume")) \
        .select(
            col("window.start").alias("window_start"),
            "ticker",
            "avg_price",
            "vwap",
            "rvol",
            coalesce(col("volatility"), lit(0)).alias("volatility"),
            coalesce(col("social_sentiment"), lit(0)).alias("social_sentiment"),
            coalesce(col("news_sentiment"), lit(0)).alias("news_sentiment"),
            (col("social_sentiment") - col("news_sentiment")).alias("sentiment_gap")
        ) \
        .withColumn("combined_sentiment", (col("social_sentiment") + col("news_sentiment")) / 2)
    
    result.write \
        .format("delta") \
        .mode("append") \
        .save(GOLD_SENTIMENT_PATH)
     
    if not DeltaTable.isDeltaTable(spark, GOLD_LATEST_PATH):
        result.write.format("delta").mode("append").save(GOLD_LATEST_PATH)
    else:
        dt_latest = DeltaTable.forPath(spark, GOLD_LATEST_PATH)
        dt_latest.alias("t").merge(result.alias("u"), "t.ticker = u.ticker") \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()

query = market_agg.writeStream \
    .foreachBatch(write_to_delta) \
    .outputMode("update") \
    .option("checkpointLocation", CHECKPOINT_PATH) \
    .trigger(processingTime="1 minute") \
    .start()

query.awaitTermination()