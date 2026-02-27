import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

load_dotenv()

KAFKA_SERVER = os.getenv('KAFKA_BOOTSTRAP_SERVER')
TOPIC_NAME = os.getenv('KAFKA_TOPIC')
CA_PATH = os.getenv('KAFKA_CA_PATH')
CERT_PATH = os.getenv('KAFKA_CERT_PATH')
KEY_PATH = os.getenv('KAFKA_KEY_PATH')

spark = SparkSession.builder \
    .appName("MarketDataStreaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")


schema = StructType([
    StructField("timestamp", StringType()),
    StructField("ticker", StringType()),
    StructField("price", DoubleType()),
    StructField("volume", IntegerType()),
    StructField("exchange", StringType())
])


with open(CA_PATH, 'r') as f:
    ca_cert = f.read()
with open(CERT_PATH, 'r') as f:
    client_cert = f.read()
with open(KEY_PATH, 'r') as f:
    client_key = f.read()

raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_SERVER) \
    .option("subscribe", TOPIC_NAME) \
    .option("kafka.security.protocol", "SSL") \
    .option("kafka.ssl.truststore.type", "PEM") \
    .option("kafka.ssl.truststore.certificates", ca_cert) \
    .option("kafka.ssl.keystore.type", "PEM") \
    .option("kafka.ssl.keystore.certificate.chain", client_cert) \
    .option("kafka.ssl.keystore.key", client_key) \
    .option("kafka.ssl.endpoint.identification.algorithm", "") \
    .option("startingOffsets", "latest") \
    .load()


parsed_df = raw_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")


query = parsed_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

print("Spark Stream started! Waiting for data from Aiven...")
query.awaitTermination()