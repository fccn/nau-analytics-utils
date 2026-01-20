FROM apache/superset:4.1.4-py310

USER root

# Instalar apenas os extras que precisamos, sem permitir que o pip altere deps core do Superset
RUN pip install --no-cache-dir --no-deps "trino" "pymysql" \
 && pip install --no-cache-dir --no-deps "clickhouse-connect[sqlalchemy]"

USER superset