from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from clickhouse_driver import Client

def extract_data():
    pass  # здесь можно вызвать генератор или прочитать CSV

def validate_data(file):
    df = pd.read_csv(file)
    df.dropna(subset=["req_id","user_id","campaign_id","creative_id","ip","ts"], inplace=True)
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    df = df.drop_duplicates(subset=["req_id"])
    df.to_csv(file, index=False)

def load_data(table, file):
    client = Client('clickhouse')
    df = pd.read_csv(file)
    client.execute(
        f"INSERT INTO {table} VALUES",
        [tuple(x) for x in df.to_records(index=False)]
    )

def aggregate_ctr():
    query = """
    SELECT campaign_id, count() as clicks
    FROM raw_clicks
    WHERE ts > now() - INTERVAL 30 MINUTE
    GROUP BY campaign_id
    """
    client = Client('clickhouse')
    print(client.execute(query))

def fraud_check():
    client = Client('clickhouse')
    query = """
    INSERT INTO fraud_alerts
    SELECT max(ts), ip, count() AS clicks,
           window_start, window_end
    FROM (
        SELECT ip, ts,
               toStartOfInterval(ts, INTERVAL 10 SECOND) as window_start,
               window_start + INTERVAL 10 SECOND as window_end
        FROM raw_clicks
    )
    GROUP BY ip, window_start, window_end
    HAVING clicks > 5
    """
    client.execute(query)

with DAG(
    dag_id="etl_ads",
    start_date=datetime(2025, 8, 19),
    schedule_interval="* * * * *",
    catchup=False,
    retries=2,
    retry_delay=timedelta(minutes=1)
) as dag:

    t1 = PythonOperator(task_id="extract", python_callable=extract_data)
    t2 = PythonOperator(task_id="validate_impressions", python_callable=lambda: validate_data("/data/impressions.csv"))
    t3 = PythonOperator(task_id="validate_clicks", python_callable=lambda: validate_data("/data/clicks.csv"))
    t4 = PythonOperator(task_id="load_impressions", python_callable=lambda: load_data("raw_impressions", "/data/impressions.csv"))
    t5 = PythonOperator(task_id="load_clicks", python_callable=lambda: load_data("raw_clicks", "/data/clicks.csv"))
    t6 = PythonOperator(task_id="aggregate", python_callable=aggregate_ctr)
    t7 = PythonOperator(task_id="fraud", python_callable=fraud_check)

    t1 >> [t2, t3]
    t2 >> t4 >> t6
    t3 >> t5 >> t6
    t6 >> t7
