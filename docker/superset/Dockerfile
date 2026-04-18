FROM apache/superset:latest

USER root
RUN pip install --no-cache-dir --target /app/pythonpath psycopg2-binary
USER superset
