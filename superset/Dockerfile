FROM apache/superset:6.0.0-py310

USER root

RUN /app/.venv/bin/python -m ensurepip --upgrade && \
    /app/.venv/bin/python -m pip install --no-cache-dir \
      "pymysql" \
      "lz4" \
      "trino[sqlalchemy]" \
      "clickhouse-connect[sqlalchemy]" \
      "authlib" \
      "PyJWT" \
      "requests"

USER superset
