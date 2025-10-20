from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import random

def fetch_weather():
    # Simulate fetching weather
    cities = ["Delhi", "Mumbai", "Bangalore", "Chennai"]
    conditions = ["Sunny", "Cloudy", "Rainy", "Windy"]
    data = []
    for city in cities:
        temp = round(random.uniform(20, 40), 2)
        condition = random.choice(conditions)
        data.append((city, temp, condition))
    return data

def process_weather(**context):
    data = context['task_instance'].xcom_pull(task_ids='fetch_weather')
    
    # Insert into weather_db table
    hook = PostgresHook(postgres_conn_id='weather_postgres')
    conn = hook.get_conn()
    cur = conn.cursor()
    
    for city, temp, condition in data:
        cur.execute(
            "INSERT INTO weather (city, temperature, condition) VALUES (%s, %s, %s)",
            (city, temp, condition)
        )
    
    conn.commit()
    cur.close()
    conn.close()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 10, 19),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='weather_etl_dag',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:

    fetch_task = PythonOperator(
        task_id='fetch_weather',
        python_callable=fetch_weather
    )

    process_task = PythonOperator(
        task_id='process_weather',
        python_callable=process_weather,
        provide_context=True
    )

    fetch_task >> process_task
