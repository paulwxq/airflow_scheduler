from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime
from utils import get_subscribed_tables, get_neo4j_dependencies
import pendulum

def process_weekly_model(table_name, execution_mode):
    deps = get_neo4j_dependencies(table_name)
    print(f"Processing weekly model for {table_name} with dependencies: {deps}, mode: {execution_mode}")

def is_monday():
    return pendulum.now().day_of_week == 0

with DAG("dag_data_model_weekly", start_date=datetime(2024, 1, 1), schedule_interval="@daily", catchup=False) as dag:
    wait_for_daily = ExternalTaskSensor(
        task_id="wait_for_daily_model",
        external_dag_id="dag_data_model_daily",
        external_task_id=None,
        mode="poke",
        timeout=3600,
        poke_interval=30
    )

    if is_monday():
        weekly_tables = get_subscribed_tables('weekly')
        for item in weekly_tables:
            t = PythonOperator(
                task_id=f"process_weekly_{item['table_name']}",
                python_callable=process_weekly_model,
                op_kwargs={"table_name": item['table_name'], "execution_mode": item['execution_mode']},
            )
            wait_for_daily >> t