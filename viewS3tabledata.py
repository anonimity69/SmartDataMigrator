from pyspark.sql import SparkSession

import logging

# Suppress Spark's ShutdownHookManager errors
logging.getLogger("org.apache.spark.util.ShutdownHookManager").setLevel(logging.ERROR)
# Initialize Spark Session with Hadoop AWS support

spark = SparkSession.builder \
    .appName("S3ToPostgres") \
    .config("spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .config("spark.cleaner.referenceTracking.cleanCheckpoints", "false") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
    .getOrCreate()

# Set log level to ERROR to avoid excessive logs
spark.sparkContext.setLogLevel("ERROR")

# S3 file path
s3_file_path = "s3a://clientbuckettest2412025/employees.csv"

# Read CSV from S3 into Spark DataFrame
df = spark.read.option("header", "true").csv(s3_file_path)
df = df.repartition(10)
df.show()