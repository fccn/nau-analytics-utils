FROM apache/superset:6.0.0-py310

USER root
RUN pip install --no-cache-dir \
  "pymysql" \
  "lz4" \
  "trino[sqlalchemy]" \
  "clickhouse-connect[sqlalchemy]"
USER superset
