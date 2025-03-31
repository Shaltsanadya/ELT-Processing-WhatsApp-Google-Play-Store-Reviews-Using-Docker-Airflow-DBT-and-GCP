# Menggunakan Apache Airflow sebagai base image
FROM apache/airflow:latest

# Mengatur user menjadi root untuk menginstal dependensi
USER root

# Install git, PostgreSQL client (libpq-dev), dan Python dev tools
RUN apt-get update -y && \
    apt-get install --no-install-recommends -y \
    git libpq-dev python3-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Kembali ke user Airflow untuk keamanan
USER airflow

# Install dbt dan dependencies yang dibutuhkan
RUN pip install --no-cache-dir \
    dbt-core==1.9.3 \
    dbt-bigquery==1.9.1

WORKDIR /opt/airflow/dags


ENV DBT_PROFILES_DIR=/opt/airflow/dags




