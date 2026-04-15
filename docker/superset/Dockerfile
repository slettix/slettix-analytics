FROM apache/superset:4.1.1

USER root

# DuckDB engine for SQLAlchemy + psycopg2 for potential postgres fallback
RUN pip install --no-cache-dir \
    duckdb==1.1.3 \
    duckdb-engine==0.13.4

# Lagringsmappe for SQLite-metadata og superset-cache
RUN mkdir -p /var/lib/superset && chmod 777 /var/lib/superset

# Portal-konfig og init-skript
COPY superset_config.py  /app/pythonpath/superset_config.py
COPY setup_duckdb.py     /app/pythonpath/setup_duckdb.py
COPY init.sh             /app/superset-init.sh
RUN chmod +x /app/superset-init.sh

USER superset
