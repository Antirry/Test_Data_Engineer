from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import os, pandas as pd
from clickhouse_driver import Client

CLICKHOUSE_HOST = os.getenv('CLICKHOUSE_HOST','clickhouse')
CLICKHOUSE_PORT = int(os.getenv('CLICKHOUSE_PORT','9000'))
CLICKHOUSE_USER = os.getenv('CLICKHOUSE_USER','default')
CLICKHOUSE_PASSWORD = os.getenv('CLICKHOUSE_PASSWORD','')
DATA_DIR = os.getenv('DATA_DIR','/data')

def ch():
    return Client(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT, user=CLICKHOUSE_USER, password=CLICKHOUSE_PASSWORD)

def init_ch():
    c = ch()
    with open('/opt/airflow/scripts/ddl.sql') as f:
        for stmt in f.read().split(';'):
            s = stmt.strip()
            if s:
                c.execute(s)
    c.disconnect()

def extract():
    os.system('python /opt/airflow/scripts/generate_data.py')

def clean(fname):
    p = os.path.join(DATA_DIR, fname)
    df = pd.read_csv(p)
    # обязательные поля
    req_cols = ['req_id','user_id','campaign_id','creative_id','ip','ts']
    df = df.dropna(subset=req_cols)
    # привести время к UTC
    df['ts'] = pd.to_datetime(df['ts'], utc=True)
    # допустим пустой UA, но нормализуем тип
    df['ua'] = df['ua'].fillna('').astype(str)
    # удалить дубли по req_id, оставляя последнюю запись
    df = df.drop_duplicates(subset=['req_id'], keep='last')
    df.to_csv(p, index=False)

def load(table, fname):
    p = os.path.join(DATA_DIR, fname)
    df = pd.read_csv(p, parse_dates=['ts'])
    rows = [tuple(x) for x in df.to_records(index=False)]
    if not rows:
        return
    c = ch()
    c.execute(f'INSERT INTO default.{table} VALUES', rows)
    c.disconnect()

def check_loaded():
    c = ch()
    imps = c.execute('SELECT count() FROM default.raw_impressions')[0][0]
    clks = c.execute('SELECT count() FROM default.raw_clicks')[0][0]
    c.disconnect()
    if imps == 0 or clks == 0:
        raise ValueError(f'No data loaded: imps={imps}, clicks={clks}')

def run_aggs():
    c = ch()
    with open('/opt/airflow/scripts/queries.sql') as f:
        text = f.read()
    for stmt in [s for s in text.split(';') if s.strip()]:
        res = c.execute(stmt)
        print(f'-- Result for: {stmt.splitlines()[0]}')
        for r in res:
            print(r)
    c.disconnect()

def fraud():
    c = ch()
    # Скользящее окно 10 секунд (без дублей в fraud_alerts)
    c.execute("""
    INSERT INTO default.fraud_alerts (ts, ip, clicks, window_start, window_end)
    SELECT cand.ts_ins, cand.ip, cand.clicks, cand.window_start, cand.window_end
    FROM (
      SELECT
        now() AS ts_ins,
        ip,
        count() OVER (
          PARTITION BY ip
          ORDER BY ts
          RANGE BETWEEN INTERVAL 10 SECOND PRECEDING AND CURRENT ROW
        ) AS clicks,
        ts - INTERVAL 10 SECOND AS window_start,
        ts AS window_end
      FROM default.raw_clicks
      WHERE ts >= now() - INTERVAL 10 MINUTE
    ) AS cand
    LEFT ANTI JOIN default.fraud_alerts f
      ON f.ip = cand.ip
     AND f.window_start = cand.window_start
     AND f.window_end = cand.window_end
    WHERE cand.clicks > 5
    """)
    c.disconnect()

default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'sla': timedelta(minutes=2)
}

with DAG(
    dag_id='etl_ads',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval='*/1 * * * *',
    catchup=False,
) as dag:

    t0 = PythonOperator(task_id='init_clickhouse', python_callable=init_ch)
    t1 = PythonOperator(task_id='extract', python_callable=extract)
    t2 = PythonOperator(task_id='validate_impressions', python_callable=clean, op_args=['impressions.csv'])
    t3 = PythonOperator(task_id='validate_clicks', python_callable=clean, op_args=['clicks.csv'])
    t4 = PythonOperator(task_id='load_impressions', python_callable=load, op_args=['raw_impressions','impressions.csv'])
    t5 = PythonOperator(task_id='load_clicks', python_callable=load, op_args=['raw_clicks','clicks.csv'])
    t6 = PythonOperator(task_id='check_loaded', python_callable=check_loaded)
    t7 = PythonOperator(task_id='aggregations', python_callable=run_aggs)
    t8 = PythonOperator(task_id='fraud_check', python_callable=fraud)

    t0 >> t1
    t1 >> [t2, t3]
    [t2, t3] >> [t4, t5]
    [t4, t5] >> t6 >> t7 >> t8
        
