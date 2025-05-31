from airflow import DAG
from airflow.decorators import task
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import requests
import pandas as pd

# BigQuery setup
BQ_PROJECT = 'crypto-etl-project-461506'
BQ_DATASET = 'finpulse_raw'
BQ_TABLE = 'fin_events'

default_args = {
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# ✅ CHANGED: schedule_interval → schedule
with DAG(
    dag_id='fetch_financial_events',
    schedule='@daily',  # ✅ compatible with Airflow 3.x
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['finpulse', 'finnhub', 'bq'],
) as dag:

    @task()
    def extract():
        api_key = Variable.get("FINNHUB_API_KEY")
        url = f"https://finnhub.io/api/v1/calendar/earnings?from={datetime.utcnow().date()}&to={datetime.utcnow().date()}&token={api_key}"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json().get('earningsCalendar', [])
        return data

    @task()
    def transform(raw_events):
        df = pd.DataFrame(raw_events)
        df = df[['symbol', 'date', 'epsEstimate', 'hour', 'revenueEstimate']].fillna("null")
        df['ingested_at'] = pd.Timestamp.utcnow().isoformat()
        return df.to_dict(orient='records')

    @task()
    def build_sql(rows):
        values = ",\n".join([
            f"('{r['symbol']}', DATE('{r['date']}'), {r['epsEstimate']}, '{r['hour']}', {r['revenueEstimate']}, TIMESTAMP('{r['ingested_at']}'))"
            for r in rows
        ])
        return f"""
        INSERT INTO `{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}` 
        (symbol, event_date, eps_estimate, hour, revenue_estimate, ingested_at)
        VALUES
        {values}
        """

    load = BigQueryInsertJobOperator(
        task_id='load_to_bigquery',
        configuration={
            "query": {
                "query": "{{ ti.xcom_pull(task_ids='build_sql') }}",
                "useLegacySql": False,
            }
        },
        location="US",
        gcp_conn_id="google_cloud_default",
    )

    raw_data = extract()
    clean_data = transform(raw_data)
    sql = build_sql(clean_data)
    load << sql
