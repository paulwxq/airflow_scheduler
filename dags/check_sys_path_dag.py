from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys

def print_sys_path():
    print("=== sys.path 内容如下 ===")
    for path in sys.path:
        print(path)

default_args = {
    'start_date': datetime(2025, 1, 1),
}

with DAG(
    dag_id='check_sys_path_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['debug'],
) as dag:

    task_print_path = PythonOperator(
        task_id='print_sys_path',
        python_callable=print_sys_path
    )
