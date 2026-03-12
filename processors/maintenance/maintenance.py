import os
from pyspark.sql import SparkSession
from delta.tables import DeltaTable

# --- CONFIGURAZIONE ---
AWS_S3_BUCKET_NAME = os.getenv('AWS_S3_BUCKET_NAME')
BASE_SILVER_PATH = f"s3a://{AWS_S3_BUCKET_NAME}/medallion/silver/"
GOLD_PATH = f"s3a://{AWS_S3_BUCKET_NAME}/medallion/gold/sentiment_impact/"

spark = SparkSession.builder \
    .appName("Medallion-Maintenance-Job") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.databricks.delta.retentionDurationCheck.enabled", "false") \
    .getOrCreate()

tables = {
    "market": f"{BASE_SILVER_PATH}market/",
    "news": f"{BASE_SILVER_PATH}news/",
    "reddit": f"{BASE_SILVER_PATH}reddit/",
    "gold": GOLD_PATH
}

def run_maintenance(table_name, path, retention_days=1):
    print(f"\n--- Starting Maintenance on table: {table_name} ---")
    
    try:
        dt = DeltaTable.forPath(spark, path)
        
        # 1. DELETE
        print(f"[{table_name}] Deleting records older than {retention_days} day(s)...")
        dt.delete(f"timestamp < current_timestamp() - interval {retention_days} days")
        
        # 2. OPTIMIZE
        print(f"[{table_name}] Optimizing file layout (Compaction)...")
        dt.optimize().executeCompaction()
        
        # 3. VACUUM
        print(f"[{table_name}] Running Vacuum to free up S3 space...")
        dt.vacuum(168) 
        
        print(f"[{table_name}] Maintenance completed successfully.")
        
    except Exception as e:
        print(f"[{table_name}] Error during maintenance: {e}")

if __name__ == "__main__":

    for name, path in tables.items():
        days = 7 if name == "gold" else 1
        run_maintenance(name, path, retention_days=days)
    
    print("\nAll maintenance tasks finished.")
    spark.stop()