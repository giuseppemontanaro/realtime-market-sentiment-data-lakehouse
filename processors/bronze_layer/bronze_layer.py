import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_date
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, ArrayType, LongType

# --- CONFIGURATION ---
KAFKA_SERVER = os.getenv('KAFKA_BOOTSTRAP_SERVER')
KAFKA_TOPIC_MARKET = os.getenv('KAFKA_TOPIC_MARKET')
KAFKA_TOPIC_NEWS = os.getenv('KAFKA_TOPIC_NEWS')
KAFKA_TOPIC_REDDIT = os.getenv('KAFKA_TOPIC_REDDIT')
TOPICS = f"{KAFKA_TOPIC_MARKET},{KAFKA_TOPIC_NEWS},{KAFKA_TOPIC_REDDIT}"

KAFKA_CA_PATH = os.getenv('KAFKA_CA_PATH')
KAFKA_CERT_PATH = os.getenv('KAFKA_CERT_PATH')
KAFKA_KEY_PATH = os.getenv('KAFKA_KEY_PATH')

AWS_S3_BUCKET_NAME = os.getenv('AWS_S3_BUCKET_NAME')
BASE_BRONZE_PATH = f"s3a://{AWS_S3_BUCKET_NAME}/medallion/bronze/"
BASE_CHECKPOINT_PATH = f"s3a://{AWS_S3_BUCKET_NAME}/checkpoints/bronze"


# --- SCHEMES ---
market_schema = StructType([
    StructField("timestamp", StringType()),
    StructField("ticker", StringType()),
    StructField("price", DoubleType()),
    StructField("volume", LongType()),
    StructField("sector", StringType()),
    StructField("name", StringType())
])

news_schema = StructType([
    StructField("timestamp", StringType()),
    StructField("source", StringType()),
    StructField("title", StringType()),
    StructField("description", StringType()),
    StructField("url", StringType()),
    StructField("related_tickers", ArrayType(StringType()))
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
    StructField("source", StringType())
])


def read_file(path):
    with open(path, 'r') as f:
        return f.read()
    

spark = SparkSession.builder \
    .appName("Medallion-Bronze") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_SERVER) \
    .option("subscribe", TOPICS) \
    .option("kafka.security.protocol", "SSL") \
    .option("kafka.ssl.truststore.type", "PEM") \
    .option("kafka.ssl.truststore.certificates", read_file(KAFKA_CA_PATH)) \
    .option("kafka.ssl.keystore.type", "PEM") \
    .option("kafka.ssl.keystore.certificate.chain", read_file(KAFKA_CERT_PATH)) \
    .option("kafka.ssl.keystore.key", read_file(KAFKA_KEY_PATH)) \
    .load()


# --- PROCESSING ---
base_df = raw_df.selectExpr("CAST(value AS STRING) as json_payload", "topic", "timestamp as kafka_arrival_ts")

# market stream
market_df = base_df.filter(col("topic") == f"{KAFKA_TOPIC_MARKET}") \
    .select(from_json(col("json_payload"), market_schema).alias("data"), "kafka_arrival_ts") \
    .select("data.*", "kafka_arrival_ts") \
    .withColumn("ingestion_date", to_date(col("timestamp")))

# news stream
news_df = base_df.filter(col("topic") == f"{KAFKA_TOPIC_NEWS}") \
    .select(from_json(col("json_payload"), news_schema).alias("data"), "kafka_arrival_ts") \
    .select("data.*", "kafka_arrival_ts") \
    .withColumn("ingestion_date", to_date(col("timestamp")))

# reddit stream
reddit_df = base_df.filter(col("topic") == f"{KAFKA_TOPIC_REDDIT}") \
    .select(from_json(col("json_payload"), reddit_schema).alias("data"), "kafka_arrival_ts") \
    .select("data.*", "kafka_arrival_ts") \
    .withColumn("ingestion_date", to_date(col("timestamp")))


# --- MULTI-SINK WRITING ---
q1 = market_df.writeStream \
    .format("parquet") \
    .partitionBy("ingestion_date", "ticker") \
    .option("path", f"{BASE_BRONZE_PATH}/market/") \
    .option("checkpointLocation", f"{BASE_CHECKPOINT_PATH}/market/") \
    .start()

q2 = news_df.writeStream \
    .format("parquet") \
    .partitionBy("ingestion_date") \
    .option("path", f"{BASE_BRONZE_PATH}/news/") \
    .option("checkpointLocation", f"{BASE_CHECKPOINT_PATH}/news/") \
    .start()

q3 = reddit_df.writeStream \
    .format("parquet") \
    .partitionBy("ingestion_date") \
    .option("path", f"{BASE_BRONZE_PATH}/reddit/") \
    .option("checkpointLocation", f"{BASE_CHECKPOINT_PATH}/reddit/") \
    .start()

print(f"Bronze Layer active. Writing to {BASE_BRONZE_PATH}...")
spark.streams.awaitAnyTermination()