from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable 
from datetime import datetime

def test_pg_conn():
    hook = PostgresHook(postgres_conn_id="pg_dataops")
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT 1;")
    result = cursor.fetchone()
    print(f"Query result: {result}")
    cursor.close()
    conn.close()

        # 获取并打印 Airflow Variables
    upload_base_path = Variable.get("STRUCTURE_UPLOAD_BASE_PATH", default_var="Not Set")
    archive_base_path = Variable.get("STRUCTURE_UPLOAD_ARCHIVE_BASE_PATH", default_var="Not Set")

    print(f"STRUCTURE_UPLOAD_BASE_PATH: {upload_base_path}")
    print(f"STRUCTURE_UPLOAD_ARCHIVE_BASE_PATH: {archive_base_path}")

with DAG(
    dag_id="test_pg_hook_connection",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["postgres", "test"],
) as dag:

    run_test = PythonOperator(
        task_id="test_pg_conn",
        python_callable=test_pg_conn,
    )