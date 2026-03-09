#!/usr/bin/env bash
# docker-entrypoint-initdb.d/init_pg.sh
# Creates the pipeline user and the data warehouse DB, then applies the schema.
# Runs automatically on first Postgres container start.
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE USER pipeline WITH PASSWORD 'pipeline';
    CREATE DATABASE ecommerce_dw;
    GRANT ALL PRIVILEGES ON DATABASE ecommerce_dw TO pipeline;
EOSQL

psql -v ON_ERROR_STOP=1 --username pipeline --dbname ecommerce_dw \
     -f /opt/airflow/sql/01_create_schema.sql

psql -v ON_ERROR_STOP=1 --username pipeline --dbname ecommerce_dw \
     -f /opt/airflow/sql/02_seed_dim_date.sql

echo "✅ ecommerce_dw schema initialised"
