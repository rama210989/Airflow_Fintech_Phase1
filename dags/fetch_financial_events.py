from airflow import DAG
from airflow.decorators import task
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import requests
import pandas as pd

# BigQuery setup
BQ_PROJECT = 'fintech-project'
BQ_DATASET = 'finpulse_raw'
BQ_TABLE = 'fin_events'

default_args = {
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='fetch_financial_events',
    schedule='@daily',
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['finpulse', 'finnhub', 'bq'],
) as dag:

    @task()
    def extract():
        api_key = Variable.get("FINNHUB_API_KEY")
        today = datetime.utcnow().date()
        url = f"https://finnhub.io/api/v1/calendar/earnings?from={today}&to={today}&token={api_key}"

        response = requests.get(url)
        response.raise_for_status()
        json_data = response.json()

        data = json_data.get('earningsCalendar', [])

        if not data:
            raise ValueError(f"No earnings data returned for {today}. Response was: {json_data}")

        print(f"✅ Extracted {len(data)} records from Finnhub.")
        print("🔎 Sample record:", data[0] if data else "None")

        return data

    @task()
    def transform(raw_events):
        try:
            df = pd.DataFrame(raw_events)

            expected_cols = ['symbol', 'date', 'epsEstimate', 'hour', 'revenueEstimate']
            missing = [col for col in expected_cols if col not in df.columns]

            if missing:
                print(f"⚠️ Warning: Missing columns in response: {missing}")
                print("📦 Raw Data Sample:", df.head().to_dict(orient='records'))
                return []  # Skip insert if data format is unexpected

            df = df[expected_cols].fillna("null")
            df['ingested_at'] = pd.Timestamp.utcnow().isoformat()

            return df.to_dict(orient='records')

        except Exception as e:
            print(f"❌ Transform step failed: {e}")
            print("📦 Raw data input:", raw_events)
            return []

    @task()
    def build_sql(rows):
        if not rows:
            print("⚠️ No valid rows to insert into BigQuery.")
            return "SELECT 1"  # Dummy no-op query

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
