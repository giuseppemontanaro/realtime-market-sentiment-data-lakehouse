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
    window(col("timestamp"), "1 minute"),
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

    # 1. Prepariamo i dati Silver con la stessa colonna "window_start"
    # Usiamo lo start della finestra per il join
    reddit_static = spark.read.format("delta").load(f"{BASE_SILVER_PATH}reddit/") \
        .withColumn("window_start", window(col("timestamp"), "1 minute").start)

    news_static = spark.read.format("delta").load(f"{BASE_SILVER_PATH}news/") \
        .withColumn("window_start", window(col("timestamp"), "1 minute").start)
    
    avg_volume_static = market_static.groupBy("ticker").agg(
        avg("volume").alias("historical_avg_volume")
    )

    # 2. Aggreghiamo includendo window_start nel GroupBy
    reddit_agg = reddit_static.groupBy("window_start", "ticker").agg(
        (sum(col("sentiment_score") * col("impact_score")) / sum("impact_score")).alias("social_sentiment")
    )

    news_agg = news_static.groupBy("window_start", "ticker").agg(
        avg("sentiment_score").alias("news_sentiment")
    )

    # 3. Allineiamo il batch_df (Market) per il join
    # Estraiamo window_start dalla colonna window strutturata
    batch_with_time = batch_df.withColumn("window_start", col("window.start"))

    result = batch_with_time \
        .join(avg_volume_static, "ticker", "left") \
        .join(reddit_agg, ["window_start", "ticker"], "left") \
        .join(news_agg, ["window_start", "ticker"], "left") \
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
    
    if not DeltaTable.isDeltaTable(spark, GOLD_SENTIMENT_PATH):
        # Se è la prima volta, scrivi normalmente
        result.write.format("delta").mode("overwrite").save(GOLD_SENTIMENT_PATH)
    else:
        # 2. Esegui il MERGE (Upsert)
        dt_gold = DeltaTable.forPath(spark, GOLD_SENTIMENT_PATH)
        
        dt_gold.alias("target").merge(
            result.alias("updates"),
            "target.window_start = updates.window_start AND target.ticker = updates.ticker"
        ).whenMatchedUpdateAll() \
         .whenNotMatchedInsertAll() \
         .execute()
    

query = market_agg.writeStream \
    .foreachBatch(write_to_delta) \
    .outputMode("update") \
    .option("checkpointLocation", CHECKPOINT_PATH) \
    .trigger(processingTime="1 minute") \
    .start()

query.awaitTermination()