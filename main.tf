provider "aws" {
  region = "eu-central-1"
}

resource "aws_s3_bucket" "data_lake" {
  bucket = "market-sentiment-lakehouse-tuonome"
}

resource "aws_iam_user" "spark_user" {
  name = "databricks-spark-user"
}

resource "aws_iam_policy" "spark_policy" {
  name        = "SparkS3AccessPolicy"
  description = "Permessi per Spark su S3"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action   = ["s3:PutObject", "s3:GetObject", "s3:ListBucket"]
        Effect   = "Allow"
        Resource = [
          "${aws_s3_bucket.data_lake.arn}",
          "${aws_s3_bucket.data_lake.arn}/*"
        ]
      }
    ]
  })
}

resource "aws_iam_user_policy_attachment" "attach_spark" {
  user       = aws_iam_user.spark_user.name
  policy_arn = aws_iam_policy.spark_policy.arn
}