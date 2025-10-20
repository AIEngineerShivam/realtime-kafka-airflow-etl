# dags/weather_etl_dag.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import requests

default_args = {
    "owner": "shivam",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}

def fetch_weather():
    # Delhi coordinates; open-meteo is free and requires no key for basic data
    lat, lon = 28.7041, 77.1025
    url = f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&current_weather=true"
    r = requests.get(url, timeout=10)
    r.raise_for_status()
    j = r.json()
    current = j.get("current_weather") or {}
    # current_weather contains temperature (deg C) and weathercode, windspeed
    temp = float(current.get("temperature", 0.0))
    # map weathercode minimally to human string (simplified)
    code = int(current.get("weathercode", -1)) if current.get("weathercode") is not None else -1
    condition = "Clear"
    if code in (51,53,55,56,57,61,63,65,66,67,80,81,82):
        condition = "Rainy"
    elif code in (71,73,75,77):
        condition = "Snow"
    elif code in (-1, 0):
        condition = "Clear"
    else:
        condition = "Cloudy"
    return {"city": "Delhi", "temperature": temp, "condition": condition}

def process_weather(ti=None):
    row = ti.xcom_pull(task_ids="fetch_weather")
    if not row:
        raise ValueError("No weather data fetched")
    hook = PostgresHook(postgres_conn_id="postgres_default")
    insert_sql = """
      INSERT INTO weather (city, temperature, condition, created_at)
      VALUES (%s, %s, %s, NOW())
    """
    hook.run(insert_sql, parameters=(row["city"], row["temperature"], row["condition"]))
    # return something for logs
    return f"Inserted: {row['city']} {row['temperature']} {row['condition']}"

with DAG(
    dag_id="weather_etl_dag",
    default_args=default_args,
    start_date=datetime(2025,10,19),
    schedule_interval="@hourly",
    catchup=False,
    tags=["weather","etl"],
) as dag:

    t1 = PythonOperator(task_id="fetch_weather", python_callable=fetch_weather)
    t2 = PythonOperator(task_id="process_weather", python_callable=process_weather, provide_context=True)

    t1 >> t2
