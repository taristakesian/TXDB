#!/usr/bin/env bash
set -euo pipefail

echo ">>> Airflow DB init"
airflow db init

echo ">>> Create admin user"
airflow users create \
  --username "$AIRFLOW_SUPERUSER_USERNAME" \
  --password "$AIRFLOW_SUPERUSER_PASSWORD" \
  --firstname "$AIRFLOW_SUPERUSER_FIRSTNAME" \
  --lastname "$AIRFLOW_SUPERUSER_LASTNAME" \
  --role Admin \
  --email "$AIRFLOW_SUPERUSER_EMAIL"

echo ">>> Add connections"
# S3 / MinIO
airflow connections add minio_s3 --conn-uri "$CONN_MINIO"
# Postgres ODS
airflow connections add ods_postgres --conn-uri "$CONN_PG_ODS"
# Mongo
airflow connections add ods_mongo --conn-uri "$CONN_MONGO"

echo ">>> Done"
