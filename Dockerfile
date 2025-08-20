FROM apache/airflow:2.5.1
USER root
RUN pip install --no-cache-dir clickhouse-driver pandas
USER airflow
