from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import sys
import os
from python_script.extract_data import ingest_subscription_data, ingest_session_data


# Add current directory to Python path
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(SCRIPT_DIR)


# Define Airflow default arguments
default_args = {
    "owner": "TX Services",
    "depends_on_past": False,
    "start_date": datetime(2024, 3, 7),
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "daily_data_pipeline",
    default_args=default_args,
    description="A DAG to fetch data from S3 and trigger DBT",
    schedule_interval=timedelta(days=1),
    catchup=False,
)

fetch_subscriptions = PythonOperator(
    task_id="fetch_subscriptions",
    python_callable=ingest_subscription_data,
    dag=dag,
)

fetch_sessions = PythonOperator(
    task_id="fetch_sessions",
    python_callable=ingest_session_data,
    dag=dag,
)

run_dbt_task = BashOperator(
    task_id="run_dbt",
    bash_command="cd /Users/thamyres.vasconcellos/Desktop/tx_services/ && dbt run",
    dag=dag,
)

# Define task dependencies
fetch_subscriptions >> fetch_sessions >> run_dbt_task