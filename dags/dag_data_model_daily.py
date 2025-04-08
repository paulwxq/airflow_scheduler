# dag_data_model_daily.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime
from utils import get_enabled_tables, is_data_model_table, run_model_script, get_model_dependency_graph
from config import NEO4J_CONFIG
import pendulum
import logging

# 创建日志记录器
logger = logging.getLogger(__name__)

with DAG("dag_data_model_daily", start_date=datetime(2024, 1, 1), schedule_interval="@daily", catchup=False) as dag:
    logger.info("初始化 dag_data_model_daily DAG")
    
    # 等待资源表 DAG 完成
    wait_for_resource = ExternalTaskSensor(
        task_id="wait_for_data_resource",
        external_dag_id="dag_data_resource",
        external_task_id=None,
        mode="poke",
        timeout=3600,
        poke_interval=30
    )
    logger.info("创建资源表等待任务 - wait_for_data_resource")

    # 获取启用的 daily 模型表
    try:
        enabled_tables = get_enabled_tables("daily")
        model_tables = [t for t in enabled_tables if is_data_model_table(t['table_name'])]
        logger.info(f"获取到 {len(model_tables)} 个启用的 daily 模型表")
    except Exception as e:
        logger.error(f"获取 daily 模型表时出错: {str(e)}")
        raise

    # 获取依赖图
    try:
        table_names = [t['table_name'] for t in model_tables]
        dependency_graph = get_model_dependency_graph(table_names)
        logger.info(f"构建了 {len(dependency_graph)} 个表的依赖关系图")
    except Exception as e:
        logger.error(f"构建依赖关系图时出错: {str(e)}")
        raise

    # 构建 task 对象
    task_dict = {}
    for item in model_tables:
        try:
            task = PythonOperator(
                task_id=f"process_model_{item['table_name']}",
                python_callable=run_model_script,
                op_kwargs={"table_name": item['table_name'], "execution_mode": item['execution_mode']},
            )
            task_dict[item['table_name']] = task
            logger.info(f"创建模型处理任务: process_model_{item['table_name']}")
        except Exception as e:
            logger.error(f"创建任务 process_model_{item['table_name']} 时出错: {str(e)}")
            raise

    # 建立任务依赖（基于 DERIVED_FROM 图）
    dependency_count = 0
    for target, upstream_list in dependency_graph.items():
        for upstream in upstream_list:
            if upstream in task_dict and target in task_dict:
                task_dict[upstream] >> task_dict[target]
                dependency_count += 1
                logger.debug(f"建立依赖关系: {upstream} >> {target}")
            else:
                logger.warning(f"无法建立依赖关系，缺少任务: {upstream} 或 {target}")

    logger.info(f"总共建立了 {dependency_count} 个任务依赖关系")

    # 最顶层的 task（没有任何上游）需要依赖资源任务完成
    all_upstreams = set()
    for upstreams in dependency_graph.values():
        all_upstreams.update(upstreams)
    top_level_tasks = [t for t in table_names if t not in all_upstreams]
    
    if top_level_tasks:
        logger.info(f"发现 {len(top_level_tasks)} 个顶层任务: {', '.join(top_level_tasks)}")
        for name in top_level_tasks:
            wait_for_resource >> task_dict[name]
    else:
        logger.warning("没有找到顶层任务，请检查依赖关系图是否正确")
