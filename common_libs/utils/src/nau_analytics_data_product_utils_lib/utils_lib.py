import os
from pyspark.sql import SparkSession #type:ignore
from .config import Config


    
def get_required_env(env_name:str) -> str:
    env_value = os.getenv(env_name)
    if env_value is None:
        raise ValueError(f"Environment variable {env_name} is not set")
    return env_value

def get_iceberg_spark_session(cfg: Config) -> SparkSession:
    spark = SparkSession.builder \
        .appName(cfg.app_name) \
        .config("spark.jars", 
                "/opt/spark/jars/hadoop-aws-3.3.4.jar," 
                "/opt/spark/jars/aws-java-sdk-bundle-1.12.375.jar," 
                "/opt/spark/jars/mysql-connector-j-8.3.0.jar," 
                "/opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.10.0.jar") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")\
        .config(f"spark.sql.catalog.{cfg.bronze_iceberg_catalog_name}","org.apache.iceberg.spark.SparkCatalog") \
        .config(f"spark.sql.catalog.{cfg.bronze_iceberg_catalog_name}.type","jdbc") \
        .config(f"spark.sql.catalog.{cfg.bronze_iceberg_catalog_name}.uri", cfg.bronze_iceberg_catalog_uri) \
        .config(f"spark.sql.catalog.{cfg.bronze_iceberg_catalog_name}.jdbc.user", cfg.iceberg_catalog_user) \
        .config(f"spark.sql.catalog.{cfg.bronze_iceberg_catalog_name}.jdbc.password", cfg.iceberg_catalog_password)\
        .config(f"spark.sql.catalog.{cfg.bronze_iceberg_catalog_name}.warehouse", cfg.bronze_iceberg_catalog_warehouse) \
        .config(f"spark.sql.catalog.{cfg.silver_iceberg_catalog_name}","org.apache.iceberg.spark.SparkCatalog") \
        .config(f"spark.sql.catalog.{cfg.silver_iceberg_catalog_name}.type","jdbc") \
        .config(f"spark.sql.catalog.{cfg.silver_iceberg_catalog_name}.uri", cfg.silver_iceberg_catalog_uri) \
        .config(f"spark.sql.catalog.{cfg.silver_iceberg_catalog_name}.jdbc.user", cfg.iceberg_catalog_user) \
        .config(f"spark.sql.catalog.{cfg.silver_iceberg_catalog_name}.jdbc.password", cfg.iceberg_catalog_password)\
        .config(f"spark.sql.catalog.{cfg.silver_iceberg_catalog_name}.warehouse", cfg.silver_iceberg_catalog_warehouse) \
        .config(f"spark.sql.catalog.{cfg.gold_iceberg_catalog_name}","org.apache.iceberg.spark.SparkCatalog") \
        .config(f"spark.sql.catalog.{cfg.gold_iceberg_catalog_name}.type","jdbc") \
        .config(f"spark.sql.catalog.{cfg.gold_iceberg_catalog_name}.uri", cfg.gold_iceberg_catalog_uri) \
        .config(f"spark.sql.catalog.{cfg.gold_iceberg_catalog_name}.jdbc.user", cfg.iceberg_catalog_user) \
        .config(f"spark.sql.catalog.{cfg.gold_iceberg_catalog_name}.jdbc.password", cfg.iceberg_catalog_password)\
        .config(f"spark.sql.catalog.{cfg.gold_iceberg_catalog_name}.warehouse", cfg.gold_iceberg_catalog_warehouse) \
        .config("spark.hadoop.fs.s3a.access.key", cfg.s3_access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", cfg.s3_secret_key) \
        .config("spark.hadoop.fs.s3a.endpoint", cfg.s3_endpoint) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()
    return spark 

def get_delta_spark_session(S3_ACCESS_KEY: str,S3_SECRET_KEY: str , S3_ENDPOINT: str) -> SparkSession:
    spark = SparkSession.builder \
        .appName("incremental_table_ingestion") \
        .config("spark.jars", 
                "/opt/spark/jars/hadoop-aws-3.3.4.jar,"
                "/opt/spark/jars/aws-java-sdk-bundle-1.12.375.jar,"
                "/opt/spark/jars/delta-spark_2.12-3.2.1.jar,"
                "/opt/spark/jars/delta-storage-3.2.1.jar,"
                "/opt/spark/jars/delta-kernel-api-3.2.1.jar,"
                "/opt/spark/jars/mysql-connector-j-8.3.0.jar") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")\
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
        .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()
    return spark 
