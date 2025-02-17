from pyspark.sql import SparkSession
import os
import logging

def load_dataframe_from_MysqlDB(spark, mysql_url, mysql_table_name, mysql_properties):
    #Loading data from MySQL into dataframe using repartitioning
    df = spark.read.jdbc(mysql_url, mysql_table_name, properties=mysql_properties)
    df = df.repartition(10)
    df.show()
    return df

def load_dataframe_to_aws_postgres(aws_postgres_url, aws_postgres_properties, aws_table_name, edit_mode,df):
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
    mysql_jar_location = "Drivers/mysql-connector-j-9.1.0.jar"
    postgres_jar_location = "Drivers/postgresql-42.7.3.jar"
    endpoint="mytestdb.ctoskq26y17o.us-east-1.rds.amazonaws.com:5432/postgres"
    mysql_schema="student"
    mysql_table_name="student_info"
    mysql_url = f"jdbc:mysql://localhost:3306/{mysql_schema}"
    mysql_properties = {
        "user": "root",
        "password": "root",
        "driver": "com.mysql.cj.jdbc.Driver"
    }
    # AWS PostgreSQL JDBC URL and Database connection properties
    aws_postgres_url = f"jdbc:postgresql://{endpoint}"
    aws_postgres_properties = {
        "user": "postgres",
        "password": "password12345678",
        "driver": "org.postgresql.Driver",
        "currentSchema": "testing"
    }
    aws_table_name=f"mysql_{mysql_table_name}"
    edit_mode="overwrite"

    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("MySQLToPostgres") \
        .config("spark.local.dir", custom_temp_dir) \
        .config("spark.jars", f"{mysql_jar_location},{postgres_jar_location}") \
        .getOrCreate()
    # Set log level to ERROR to avoid excessive logs
    spark.sparkContext.setLogLevel("ERROR")
    df=load_dataframe_from_MysqlDB(spark, mysql_url, mysql_table_name, mysql_properties)
    load_dataframe_to_aws_postgres_using_RDD(aws_postgres_url, aws_postgres_properties, aws_table_name, edit_mode, df)
    print(f"Data transfer from MySQL to PostgreSQL RDS complete! Created Table: {aws_table_name}")

# Standard entry point
if __name__ == "__main__":
    main()