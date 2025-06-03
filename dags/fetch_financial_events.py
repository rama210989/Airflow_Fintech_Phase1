from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from datetime import datetime, timedelta
import requests

from google.cloud import bigquery
from google.api_core.exceptions import NotFound

default_args = {
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='fetch_financial_events_extract_load_bq',
    schedule='@daily',
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['finpulse', 'finnhub'],
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
    def load_to_bigquery(data: list):
        client = bigquery.Client()
        project_id = 'fintech-project'
        dataset_id = 'finpulse_raw'
        table_id = 'earnings_calendar'
        full_table_id = f"{project_id}.{dataset_id}.{table_id}"

        try:
            client.get_table(full_table_id)
            print("✅ Table exists.")
        except NotFound:
            print("⚠️ Table not found. Creating table.")
            sample_record = data[0]
            schema = [
                bigquery.SchemaField(key, "STRING", mode="NULLABLE")
                for key in sample_record.keys()
            ]
            table = bigquery.Table(full_table_id, schema=schema)
            client.create_table(table)
            print("✅ Table created successfully.")

        # Insert data
        errors = client.insert_rows_json(
            table=full_table_id,
            json_rows=data,
            row_ids=[None] * len(data),
            skip_invalid_rows=True,
            ignore_unknown_values=True,
        )

        if errors:
            print(f"❌ Errors during insert: {errors}")
            raise RuntimeError(f"Insert failed: {errors}")
        else:
            print(f"✅ Inserted {len(data)} rows into {full_table_id}")

    # DAG execution sequence
    extracted_data = extract()
    load_to_bigquery(extracted_data)
