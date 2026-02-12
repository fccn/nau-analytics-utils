from dataclasses import dataclass

@dataclass
class Config:
    s3_access_key: str
    s3_secret_key: str
    s3_endpoint: str

    app_name: str
    iceberg_catalog_user: str
    iceberg_catalog_password: str
    bronze_iceberg_catalog_uri: str
    bronze_iceberg_catalog_warehouse: str
    bronze_iceberg_catalog_name: str
    silver_iceberg_catalog_uri: str
    silver_iceberg_catalog_warehouse: str
    silver_iceberg_catalog_name: str
    gold_iceberg_catalog_uri: str
    gold_iceberg_catalog_warehouse: str
    gold_iceberg_catalog_name: str
