from .config import Config
from .utils_lib import get_required_env,get_iceberg_spark_session,get_delta_spark_session
from pyspark.sql import SparkSession #type:ignore
import base64

def generate_config_file_from_env(app_name:str) -> Config:


    S3_ACCESS_KEY = get_required_env("S3_ACCESS_KEY")
    S3_SECRET_KEY = get_required_env("S3_SECRET_KEY")
    S3_ENDPOINT = get_required_env("S3_ENDPOINT")

    ICEBERG_CATALOG_USER = get_required_env("ICEBERG_CATALOG_USER")
    ICEBERG_CATALOG_PASSWORD = get_required_env("ICEBERG_CATALOG_PASSWORD")
    ICEBERG_CATALOG_PASSWORD = base64.b64decode(ICEBERG_CATALOG_PASSWORD).decode()
    
    ICEBERG_CATALOG_HOST = get_required_env("ICEBERG_CATALOG_HOST")
    ICEBERG_CATALOG_PORT = get_required_env("ICEBERG_CATALOG_PORT")
    
    BRONZE_ICEBERG_DATABASE_CATALOG_NAME = get_required_env("BRONZE_ICEBERG_DATABASE_CATALOG_NAME")
    BRONZE_ICEBERG_CATALOG_WAREHOUSE = get_required_env("BRONZE_ICEBERG_CATALOG_WAREHOUSE")
    BRONZE_ICEBERG_CATALOG_NAME = get_required_env("BRONZE_ICEBERG_CATALOG_NAME")
    BRONZE_ICEBERG_CATALOG_URI = f"jdbc:mysql://{ICEBERG_CATALOG_HOST}:{ICEBERG_CATALOG_PORT}/{BRONZE_ICEBERG_DATABASE_CATALOG_NAME}"

    SILVER_ICEBERG_DATABASE_CATALOG_NAME = get_required_env("SILVER_ICEBERG_DATABASE_CATALOG_NAME")
    SILVER_ICEBERG_CATALOG_WAREHOUSE = get_required_env("SILVER_ICEBERG_CATALOG_WAREHOUSE")
    SILVER_ICEBERG_CATALOG_NAME = get_required_env("SILVER_ICEBERG_CATALOG_NAME")
    SILVER_ICEBERG_CATALOG_URI = f"jdbc:mysql://{ICEBERG_CATALOG_HOST}:{ICEBERG_CATALOG_PORT}/{SILVER_ICEBERG_DATABASE_CATALOG_NAME}"

    GOLD_ICEBERG_DATABASE_CATALOG_NAME = get_required_env("GOLD_ICEBERG_DATABASE_CATALOG_NAME")
    GOLD_ICEBERG_CATALOG_WAREHOUSE = get_required_env("GOLD_ICEBERG_CATALOG_WAREHOUSE")
    GOLD_ICEBERG_CATALOG_NAME = get_required_env("GOLD_ICEBERG_CATALOG_NAME")
    GOLD_ICEBERG_CATALOG_URI = f"jdbc:mysql://{ICEBERG_CATALOG_HOST}:{ICEBERG_CATALOG_PORT}/{GOLD_ICEBERG_DATABASE_CATALOG_NAME}"

    icerberg_cfg = Config(
        app_name=app_name,
        s3_access_key=S3_ACCESS_KEY,
        s3_endpoint=S3_ENDPOINT,
        s3_secret_key=S3_SECRET_KEY,
        iceberg_catalog_user=ICEBERG_CATALOG_USER,
        iceberg_catalog_password=ICEBERG_CATALOG_PASSWORD,
        bronze_iceberg_catalog_uri=BRONZE_ICEBERG_CATALOG_URI,
        bronze_iceberg_catalog_warehouse=BRONZE_ICEBERG_CATALOG_WAREHOUSE,
        bronze_iceberg_catalog_name = BRONZE_ICEBERG_CATALOG_NAME,
        silver_iceberg_catalog_uri=SILVER_ICEBERG_CATALOG_URI,
        silver_iceberg_catalog_warehouse=SILVER_ICEBERG_CATALOG_WAREHOUSE,
        silver_iceberg_catalog_name=SILVER_ICEBERG_CATALOG_NAME,
        gold_iceberg_catalog_uri=GOLD_ICEBERG_CATALOG_URI,
        gold_iceberg_catalog_warehouse=GOLD_ICEBERG_CATALOG_WAREHOUSE,
        gold_iceberg_catalog_name=GOLD_ICEBERG_CATALOG_NAME
    )
    return icerberg_cfg

def start_iceberg_session(app_name:str) -> SparkSession:
    spark_session_config_settings = generate_config_file_from_env(app_name)
    spark = get_iceberg_spark_session(spark_session_config_settings)
    return spark