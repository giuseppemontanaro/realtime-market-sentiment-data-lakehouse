import os
import re
import time
import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, to_timestamp, explode, udf, when, avg, stddev,
    regexp_replace, window, upper, abs as spark_abs
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, ArrayType, FloatType, LongType, DateType
)
from delta.tables import DeltaTable
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# --- CONFIGURATION ---
AWS_S3_BUCKET_NAME = os.getenv('AWS_S3_BUCKET_NAME')
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_KEY')

BASE_BRONZE_PATH = f"s3a://{AWS_S3_BUCKET_NAME}/medallion/bronze/"
BASE_SILVER_PATH = f"s3a://{AWS_S3_BUCKET_NAME}/medallion/silver/"
BASE_CHECKPOINT_PATH = f"s3a://{AWS_S3_BUCKET_NAME}/checkpoints/silver/"

spark = SparkSession.builder \
    .appName("Medallion-Silver-Delta") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# --- UDF: SENTIMENT AND CLEANING ---
analyzer = SentimentIntensityAnalyzer()

def get_vader_sentiment(text):
    if text is None or text.strip() == "": 
        return 0.0
    scores = analyzer.polarity_scores(text)
    return float(scores['compound'])

sentiment_udf = udf(get_vader_sentiment, FloatType())

def clean_text_func(text):
    if text is None: return ""
    # removes HTML, URL and Emoji
    text = re.sub(r'<[^>]+>', '', text)
    text = re.sub(r'http\S+', '', text)
    text = re.sub(r'[^a-zA-Z0-9\s!?.#]', '', text)
    return text.strip().lower()

clean_text_udf = udf(clean_text_func, StringType())

# --- BRONZE SCHEMES ---
market_schema = StructType([
    StructField("timestamp", StringType()),
    StructField("ticker", StringType()),
    StructField("price", DoubleType()),
    StructField("volume", LongType()),
    StructField("sector", StringType()),
    StructField("name", StringType()),
    StructField("ingestion_date", DateType())
])

news_schema = StructType([
    StructField("timestamp", StringType()),
    StructField("source", StringType()),
    StructField("title", StringType()),
    StructField("description", StringType()),
    StructField("url", StringType()),
    StructField("related_tickers", ArrayType(StringType())),
    StructField("ingestion_date", DateType())
])

reddit_schema = StructType([
    StructField("timestamp", StringType()),
    StructField("author", StringType()),
    StructField("title", StringType()),
    StructField("body", StringType()),
    StructField("score", IntegerType()),
    StructField("num_comments", IntegerType()),
    StructField("upvote_ratio", DoubleType()),
    StructField("related_tickers", ArrayType(StringType())),
    StructField("source", StringType()),
    StructField("ingestion_date", DateType())
])


# --- WAITING IF NO DATA ON S3 ---
def wait_for_s3_data(path, bucket):
    s3 = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
    )
    print(f"Checking for data in {path}...")
    while True:
        results = s3.list_objects_v2(Bucket=bucket, Prefix=path.split(f"{bucket}/")[1])
        if 'Contents' in results:
            print("Data found! Starting Spark stream...")
            break
        print("Waiting for initial data on S3... (10s)")
        time.sleep(10)

wait_for_s3_data(f"{BASE_BRONZE_PATH}market/", AWS_S3_BUCKET_NAME)
wait_for_s3_data(f"{BASE_BRONZE_PATH}news/", AWS_S3_BUCKET_NAME)
wait_for_s3_data(f"{BASE_BRONZE_PATH}reddit/", AWS_S3_BUCKET_NAME)

# --- PROCESSING MARKET (Standardization) ---
market_bronze = spark.readStream.format("parquet").schema(market_schema).load(f"{BASE_BRONZE_PATH}market/")

# Standardization
market_silver = market_bronze \
    .withColumn("timestamp", to_timestamp(col("timestamp"))) \
    .withColumn("ticker", upper(regexp_replace(col("ticker"), "-USD", ""))) \
    .withColumn("event_window", window(col("timestamp"), "1 minute").start)

# --- PROCESSING NEWS (cleaning, standardization and sentiment) ---
news_bronze = spark.readStream.format("parquet").schema(news_schema).load(f"{BASE_BRONZE_PATH}news/")

news_silver = news_bronze \
    .withColumn("timestamp", to_timestamp(col("timestamp"))) \
    .withColumn("ticker", explode(col("related_tickers"))) \
    .withColumn("ticker", upper(regexp_replace(col("ticker"), "-USD", ""))) \
    .withColumn("clean_title", clean_text_udf(col("title"))) \
    .withColumn("sentiment_score", sentiment_udf(col("clean_title"))) \
    .withColumn("event_window", window(col("timestamp"), "1 minute").start) \
    .withColumn("date", to_date(col("timestamp"))) \
    .select("timestamp", "event_window", "date", "ticker", "source", "clean_title", "sentiment_score", "url")

# --- PROCESSING SOCIAL (Sentiment, standardization and Impact Score) ---
reddit_bronze = spark.readStream.format("parquet").schema(reddit_schema).load(f"{BASE_BRONZE_PATH}reddit/")

reddit_silver = reddit_bronze \
    .withColumn("timestamp", to_timestamp(col("timestamp"))) \
    .withColumn("ticker", explode(col("related_tickers"))) \
    .withColumn("ticker", upper(regexp_replace(col("ticker"), "-USD", ""))) \
    .withColumn("clean_body", clean_text_udf(col("body"))) \
    .withColumn("sentiment_score", sentiment_udf(col("clean_body"))) \
    .withColumn("impact_score", (
        (col("score") * col("upvote_ratio")) + 
        (col("num_comments") * 1.5)
    ).cast(FloatType())) \
    .withColumn("impact_score", when(col("impact_score") > 1.0, col("impact_score")).otherwise(1.0)) \
    .withColumn("event_window", window(col("timestamp"), "1 minute").start) \
    .withColumn("date", to_date(col("timestamp"))) \
    .select("timestamp", "event_window", "date", "ticker", "author", "sentiment_score", "impact_score", "clean_body")

# --- UPSERT LOGIC (DELTA) ---
def upsert_to_delta(microBatchDF, batchId, target_path, merge_cols):
    if not DeltaTable.isDeltaTable(spark, target_path):
        microBatchDF.write.format("delta").mode("append").save(target_path)
    else:
        dt = DeltaTable.forPath(spark, target_path)
        dt.alias("t").merge(microBatchDF.alias("u"), merge_cols) \
          .whenMatchedUpdateAll() \
          .whenNotMatchedInsertAll() \
          .execute()

# --- MULTI-SINK WRITING ---
q1 = market_silver.writeStream \
    .foreachBatch(lambda df, id: upsert_to_delta(
        df, 
        id, 
        f"{BASE_SILVER_PATH}/market/", 
        "t.ticker = u.ticker AND t.timestamp = u.timestamp")) \
    .option("checkpointLocation", f"{BASE_CHECKPOINT_PATH}/market/") \
    .start()

q2 = news_silver.writeStream \
    .foreachBatch(lambda df, id: upsert_to_delta(
        df, 
        id, 
        f"{BASE_SILVER_PATH}/news/", 
        "t.ticker = u.ticker AND t.timestamp = u.timestamp AND t.url = u.url")) \
    .option("checkpointLocation", f"{BASE_CHECKPOINT_PATH}/news/") \
    .start()

q3 = reddit_silver.writeStream \
    .foreachBatch( lambda df, id: upsert_to_delta(
        df, 
        id, 
        f"{BASE_SILVER_PATH}/reddit/", 
        "t.ticker = u.ticker AND t.timestamp = u.timestamp AND t.author = u.author")) \
    .option("checkpointLocation", f"{BASE_CHECKPOINT_PATH}/reddit/") \
    .start()

spark.streams.awaitAnyTermination()