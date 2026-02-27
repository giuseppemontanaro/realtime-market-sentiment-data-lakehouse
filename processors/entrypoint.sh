#!/bin/bash

CONFIGS="--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
--conf spark.hadoop.fs.s3a.endpoint=s3.eu-south-1.amazonaws.com \
--conf spark.hadoop.fs.s3a.endpoint.region=eu-south-1 \
--conf spark.hadoop.fs.s3a.path.style.access=true \
--conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider
--conf spark.hadoop.fs.s3a.access.key=${AWS_ACCESS_KEY} \
--conf spark.hadoop.fs.s3a.secret.key=${AWS_SECRET_KEY}"

echo "Starting Spark Layer: $1"

# Run spark-submit using the first argument as the script name
exec spark-submit \
    --master "local[*]" \
    $CONFIGS \
    "/app/processor/$1"
