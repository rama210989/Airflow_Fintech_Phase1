from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from datetime import datetime, timedelta
import requests

default_args = {
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='fetch_financial_events_extract_only',
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

    extract()
