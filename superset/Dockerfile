FROM apache/superset:4.1.4-py310

# Dialect Trino + driver PyMySQL para o metastore MySQL 
# Dialect ClickHouse + driver ClickHouse para consultas
RUN pip install --no-cache-dir \
  "trino" "pymysql" \
  "clickhouse-sqlalchemy" \
  "clickhouse-driver"