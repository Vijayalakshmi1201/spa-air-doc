from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def hello_world():
    print("Hello World from Airflow!")

default_args = {
    'start_date': datetime(2025, 5, 27),
}

with DAG(
    'simple_hello_world',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:

    task_hello = PythonOperator(
        task_id='say_hello',
        python_callable=hello_world,
    )
