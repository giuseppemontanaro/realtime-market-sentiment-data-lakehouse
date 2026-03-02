import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import col, to_date, to_timestamp
from delta.tables import DeltaTable


AWS_S3_BUCKET_NAME = os.getenv('AWS_S3_BUCKET_NAME')
BRONZE_PATH = f"s3a://{AWS_S3_BUCKET_NAME}/medallion/bronze/market_data/"
SILVER_PATH = f"s3a://{AWS_S3_BUCKET_NAME}/medallion/silver/market_data/"
CHECKPOINT_PATH = f"s3a://{AWS_S3_BUCKET_NAME}/checkpoints/silver_layer/"

spark = SparkSession.builder \
    .appName("Medallion-Silver-Delta") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

def upsert_to_delta(microBatchDF, batchId):
    if DeltaTable.isDeltaTable(spark, SILVER_PATH):
        silverTable = DeltaTable.forPath(spark, SILVER_PATH)
        (silverTable.alias("target")
         .merge(microBatchDF.alias("updates"), 
                "target.ticker = updates.ticker AND target.timestamp = updates.timestamp")
         .whenMatchedUpdateAll()
         .whenNotMatchedInsertAll()
         .execute())
    else:
        microBatchDF.write.format("delta").mode("overwrite").save(SILVER_PATH)


bronze_schema = StructType([
    StructField("ticker", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("timestamp", StringType(), True) # Leggiamo come String
])

bronze_df = spark.readStream \
    .format("parquet") \
    .schema(bronze_schema) \
    .load(BRONZE_PATH)

silver_cleaned_df = bronze_df \
    .withColumn("timestamp", to_timestamp(col("timestamp"))) \
    .filter(col("price") > 0) \
    .withColumn("date", to_date(col("timestamp")))

query = silver_cleaned_df.writeStream \
    .foreachBatch(upsert_to_delta) \
    .option("checkpointLocation", CHECKPOINT_PATH) \
    .start()


print(f"Silver Layer (Delta) is streaming and deduplicating to {SILVER_PATH}...")
query.awaitTermination()