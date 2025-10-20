# realtime-kafka-airflow-etl

A fully automated real-time data pipeline using Kafka, Airflow, and Postgres. Fetches live data from free APIs, processes with Python ETL tasks, stores in Postgres, and enables instant visualization in Power BI. Ideal for end-to-end analytics automation.

## Features
- Real-time data ingestion via Kafka
- Workflow orchestration with Airflow
- ETL processing in Python/Spark
- Storage in Postgres
- Automatic dashboard updates in Power BI

## Installation
1. Clone repository
2. Setup Docker environment
3. Start Kafka, Postgres, and Airflow containers
4. Configure API keys and run DAGs

## Usage
- Trigger DAGs in Airflow UI
- Monitor logs for ETL
- Connect Postgres to Power BI for dashboards
