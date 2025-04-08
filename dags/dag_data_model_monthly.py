from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime
from utils import get_subscribed_tables, get_neo4j_dependencies
import pendulum

def process_monthly_model(table_name, execution_mode):
    deps = get_neo4j_dependencies(table_name)
    print(f"Processing monthly model for {table_name} with dependencies: {deps}, mode: {execution_mode}")

def is_first_day():
    return pendulum.now().day == 1

with DAG("dag_data_model_monthly", start_date=datetime(2024, 1, 1), schedule_interval="@daily", catchup=False) as dag:
    wait_for_weekly = ExternalTaskSensor(
        task_id="wait_for_weekly_model",
        external_dag_id="dag_data_model_weekly",
        external_task_id=None,
        mode="poke",
        timeout=3600,
        poke_interval=30
    )

    if is_first_day():
        monthly_tables = get_subscribed_tables('monthly')
        for item in monthly_tables:
            t = PythonOperator(
                task_id=f"process_monthly_{item['table_name']}",
                python_callable=process_monthly_model,
                op_kwargs={"table_name": item['table_name'], "execution_mode": item['execution_mode']},
            )
            wait_for_weekly >> t