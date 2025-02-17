from pyspark.sql import SparkSession
import os
import logging

def load_dataframe_from_S3(spark,s3_file_path):
    #Loading data(csv,json,parquet) from S3 bucket into dataframe using repartitioning
    file_extension = os.path.splitext(s3_file_path)[1].lower()
    if file_extension == ".csv":
        df = spark.read.option("header", "true").csv(s3_file_path)
    elif file_extension == ".json":
        df = spark.read.option("multiline", "true").json(s3_file_path)
    elif file_extension == ".parquet":
        df = spark.read.parquet(s3_file_path)
    df = df.repartition(10)
    df.select(df.columns[:5]).show(10)
    return df

def load_dataframe_to_aws_postgres(aws_postgres_url,aws_postgres_properties,aws_table_name,edit_mode,df):
    #Loading data from dataframe into AWS Postgres SQL Bucket
    df.write \
    .jdbc(aws_postgres_url, aws_table_name, mode=edit_mode, properties=aws_postgres_properties)

def load_dataframe_to_aws_postgres_using_RDD(aws_postgres_url, aws_postgres_properties, aws_table_name, edit_mode,df):
    df.repartition(10).write \
        .jdbc(aws_postgres_url, aws_table_name, mode=edit_mode, properties={"batchsize": "1000", **aws_postgres_properties})

def main():
    # Suppress Spark's ShutdownHookManager errors
    logging.getLogger("org.apache.spark.util.ShutdownHookManager").setLevel(logging.ERROR)
    custom_temp_dir = "D:/Office/Exavalu Office/tmp"
    os.makedirs(custom_temp_dir, exist_ok=True)

    #Location and Variables
    connection_jar_location="Drivers/postgresql-42.7.3.jar"
    s3_file_path = "s3a://clientbuckettest2412025/reward.json" #format: s3a://
    endpoint="mytestdb.ctoskq26y17o.us-east-1.rds.amazonaws.com:5432/postgres"
    # AWS PostgreSQL JDBC URL and Database connection properties
    aws_postgres_url = f"jdbc:postgresql://{endpoint}"
    aws_postgres_properties = {
        "user": "postgres",
        "password": "password12345678",
        "driver": "org.postgresql.Driver",
        "currentSchema": "testing"
    }
    aws_table_name="reward"
    edit_mode="overwrite"

    #    Initialize Spark Session with Hadoop AWS support
    spark = SparkSession.builder \
        .appName("S3ToPostgres") \
        .config("spark.local.dir", custom_temp_dir) \
        .config("spark.jars", connection_jar_location) \
        .config("spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
        .getOrCreate()
    # Set log level to ERROR to avoid excessive logs
    spark.sparkContext.setLogLevel("ERROR")
    df=load_dataframe_from_S3(spark,s3_file_path)
    load_dataframe_to_aws_postgres_using_RDD(aws_postgres_url,aws_postgres_properties,aws_table_name,edit_mode,df)
    print(f"Data transfer from S3 to PostgreSQL RDS complete! Created Table: {aws_table_name}")

# Standard entry point
if __name__ == "__main__":
    main()