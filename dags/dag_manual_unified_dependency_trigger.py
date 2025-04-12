from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import task
from datetime import datetime, timedelta
from config import NEO4J_CONFIG
from utils import execute_script
import logging
from neo4j import GraphDatabase
import networkx as nx

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def build_dependency_chain_nx(start_table, dependency_level="resource"):
    uri = NEO4J_CONFIG['uri']
    auth = (NEO4J_CONFIG['user'], NEO4J_CONFIG['password'])
    driver = GraphDatabase.driver(uri, auth=auth)
    logger.info(f"构建表 {start_table} 的依赖链（层级: {dependency_level}）")

    G = nx.DiGraph()
    node_info = {}

    with driver.session() as session:
        query = f"""
        MATCH path=(target:Table {{en_name: $start_table}})<-[:DERIVED_FROM*0..]-(source)
        WHERE (all(n in nodes(path) WHERE n:DataModel OR n:DataResource))
        WITH collect(DISTINCT source.en_name) + '{start_table}' AS tables
        UNWIND tables AS tname
        MATCH (t:Table {{en_name: tname}})
        OPTIONAL MATCH (t)-[r:DERIVED_FROM]->(up)
        RETURN t.en_name AS table_name,
               labels(t) AS labels,
               r.script_name AS script_name,
               r.script_exec_mode AS script_exec_mode,
               up.en_name AS upstream
        """
        records = session.run(query, start_table=start_table)
        for record in records:
            name = record['table_name']
            script_name = record.get('script_name')
            script_exec_mode = record.get('script_exec_mode') or 'append'
            upstream = record.get('upstream')
            node_type = None
            for label in record.get('labels', []):
                if label in ['DataModel', 'DataResource']:
                    node_type = label

            if name not in node_info:
                node_info[name] = {
                    'table_name': name,
                    'script_name': script_name or f"{name}.py",
                    'execution_mode': script_exec_mode,
                    'table_type': node_type,
                    'upstream_tables': []
                }
            if upstream:
                G.add_edge(upstream, name)
                node_info[name]['upstream_tables'].append(upstream)

    driver.close()
    execution_order = list(nx.topological_sort(G))
    logger.info(f"拓扑排序执行顺序: {execution_order}")

    dependency_chain = []
    for table in execution_order:
        if table in node_info:
            dependency_chain.append(node_info[table])
    return dependency_chain

with DAG(
    dag_id='dag_manual_dependency_unified_trigger',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    description='运行时构建任务，支持conf参数，展示拓扑依赖图'
) as dag:

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    @task()
    def get_dependency_chain(**context):
        conf = context['dag_run'].conf if context.get('dag_run') else {}
        table_name = conf.get("TABLE_NAME", "book_sale_amt_2yearly")
        dependency_level = conf.get("DEPENDENCY_LEVEL", "resource")
        logger.info(f"手动传入参数: TABLE_NAME={table_name}, DEPENDENCY_LEVEL={dependency_level}")
        return build_dependency_chain_nx(table_name, dependency_level)

    def create_task_callable(table_name, script_name, execution_mode):
        def _inner_callable():
            logger.info(f"执行任务：{table_name} using {script_name} mode={execution_mode}")
            if not execute_script(script_name, table_name, execution_mode):
                raise Exception(f"脚本 {script_name} 执行失败")
        return _inner_callable

    def create_runtime_tasks(chain, dag):
        task_dict = {}
        for item in chain:
            table = item['table_name']
            script = item['script_name']
            mode = item['execution_mode']
            task = PythonOperator(
                task_id=f"run_{table}",
                python_callable=create_task_callable(table, script, mode),
                dag=dag
            )
            task_dict[table] = task

        for item in chain:
            downstream = item['table_name']
            upstreams = item.get('upstream_tables', [])
            if not upstreams:
                start >> task_dict[downstream]
            else:
                for up in upstreams:
                    if up in task_dict:
                        task_dict[up] >> task_dict[downstream]

        for task in task_dict.values():
            task >> end

    from airflow.operators.python import PythonOperator

    def wrapper(**context):
        chain = context['ti'].xcom_pull(task_ids='get_dependency_chain')
        create_runtime_tasks(chain, dag)

    chain_task = get_dependency_chain()
    build_tasks = PythonOperator(
        task_id='build_runtime_tasks',
        python_callable=wrapper,
        provide_context=True
    )

    start >> chain_task >> build_tasks >> end