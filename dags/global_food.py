from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'retries': 1
}

with DAG(
    dag_id='global_street_food_analysis_dag',
    default_args=default_args,
    description='Analyze global street food data',
    schedule=None,
    catchup=False
) as dag:

    run_spark_job = BashOperator(
        task_id='run_street_food_analysis',
        bash_command='spark-submit /opt/airflow/scripts/food.py'

    )

    run_spark_job

 