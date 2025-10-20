from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'shivam',
    'depends_on_past': False,
    'start_date': datetime(2025, 10, 19),
    'retries': 1,
    'retry_delay': timedelta(seconds=10),
}

with DAG(
    'weather_pipeline',
    default_args=default_args,
    description='Weather pipeline using Kafka',
    schedule_interval=None,  # Run manually
    catchup=False,
) as dag:

    # Step 1: Produce weather data
    produce_task = BashOperator(
        task_id='produce_weather_data',
        bash_command='python3 /opt/airflow/kafka_producer/producer.py'
    )

    # Step 2: Consume weather data
    consume_task = BashOperator(
        task_id='consume_weather_data',
        bash_command='python3 /opt/airflow/kafka_producer/consumer.py'
    )

    produce_task >> consume_task
