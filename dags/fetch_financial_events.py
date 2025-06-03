from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from datetime import datetime, timedelta
import requests

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
        from google.api_core.exceptions import GoogleAPIError

        hook = BigQueryHook(gcp_conn_id='google_cloud_default', use_legacy_sql=False)

        rows_to_insert = [{'json': record} for record in data]  # required format

        try:
            hook.insert_all(
                project_id='fintech-project',
                dataset_id='finpulse_raw',
                table_id='earnings_calendar',
                rows=rows_to_insert,
                ignore_unknown_values=True,
                skip_invalid_rows=True
            )
            print(f"✅ Loaded {len(rows_to_insert)} records to BigQuery.")
        except GoogleAPIError as e:
            print(f"❌ Google API error while inserting rows: {e}")
            raise
        except Exception as e:
            print(f"❌ Unexpected error while inserting rows: {e}")
            raise

    extracted_data = extract()
    load_to_bigquery(extracted_data)
