FROM apache/superset:4.1.4-py310

USER root
RUN pip install --no-cache-dir \
  "pymysql" \
  "lz4" \
  "trino[sqlalchemy]" \
  "clickhouse-connect[sqlalchemy]"
USER superset