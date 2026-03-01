import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_date
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType


KAFKA_SERVER = os.getenv('KAFKA_BOOTSTRAP_SERVER')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC')
KAFKA_CA_PATH = os.getenv('KAFKA_CA_PATH')
KAFKA_CERT_PATH = os.getenv('KAFKA_CERT_PATH')
KAFKA_KEY_PATH = os.getenv('KAFKA_KEY_PATH')

AWS_S3_BUCKET_NAME = os.getenv('AWS_S3_BUCKET_NAME')
BRONZE_PATH = f"s3a://{AWS_S3_BUCKET_NAME}/medallion/bronze/market_data/"
CHECKPOINT_PATH = f"s3a://{AWS_S3_BUCKET_NAME}/checkpoints/bronze_layer/"


spark = SparkSession.builder \
    .appName("Medallion-Bronze") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

def read_file(path):
    with open(path, 'r') as f:
        return f.read()

raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_SERVER) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("kafka.security.protocol", "SSL") \
    .option("kafka.ssl.truststore.type", "PEM") \
    .option("kafka.ssl.truststore.certificates", read_file(KAFKA_CA_PATH)) \
    .option("kafka.ssl.keystore.type", "PEM") \
    .option("kafka.ssl.keystore.certificate.chain", read_file(KAFKA_CERT_PATH)) \
    .option("kafka.ssl.keystore.key", read_file(KAFKA_KEY_PATH)) \
    .option("kafka.ssl.endpoint.identification.algorithm", "") \
    .option("startingOffsets", "latest") \
    .load()


schema = StructType([
    StructField("timestamp", StringType()),
    StructField("ticker", StringType()),
    StructField("price", DoubleType()),
    StructField("volume", IntegerType()),
    StructField("exchange", StringType())
])

bronze_df = raw_df.selectExpr("CAST(value AS STRING) as raw_payload", "timestamp as kafka_ts") \
    .select(from_json(col("raw_payload"), schema).alias("data"), "kafka_ts") \
    .select("data.*", "kafka_ts") \
    .withColumn("processing_date", to_date(col("timestamp")))


query = bronze_df.writeStream \
    .format("parquet") \
    .partitionBy("processing_date", "ticker") \
    .option("path", BRONZE_PATH) \
    .option("checkpointLocation", CHECKPOINT_PATH) \
    .outputMode("append") \
    .start()


print(f"Bronze Layer active. Writing to {BRONZE_PATH}...")
query.awaitTermination()