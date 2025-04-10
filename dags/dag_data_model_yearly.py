# dag_data_model_yearly.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime
from utils import (
    get_enabled_tables, is_data_model_table, run_model_script, 
    get_model_dependency_graph, process_model_tables
)
from config import NEO4J_CONFIG
import pendulum
import logging

# 创建日志记录器
logger = logging.getLogger(__name__)

def is_first_day_of_year():
    return True
    # 生产环境中应使用实际判断
    # return pendulum.now().month == 1 and pendulum.now().day == 1

with DAG("dag_data_model_yearly", start_date=datetime(2024, 1, 1), schedule_interval="@daily", catchup=False) as dag:
    logger.info("初始化 dag_data_model_yearly DAG")
    
    # 等待月模型 DAG 完成
    wait_for_monthly = ExternalTaskSensor(
        task_id="wait_for_monthly_model",
        external_dag_id="dag_data_model_monthly",
        external_task_id="monthly_processing_completed",  # 指定完成标记任务
        mode="poke",
        timeout=3600,
        poke_interval=30
    )
    logger.info("创建月模型等待任务 - wait_for_monthly_model")
    
    # 创建一个完成标记任务，确保即使没有处理任务也能标记DAG完成
    yearly_completed = EmptyOperator(
        task_id="yearly_processing_completed",
        dag=dag
    )
    logger.info("创建任务完成标记 - yearly_processing_completed")
    
    # 检查今天是否是年初
    if is_first_day_of_year():
        logger.info("今天是年初，开始处理年模型")
        # 获取启用的 yearly 模型表
        try:
            enabled_tables = get_enabled_tables("yearly")
            # 使用公共函数处理模型表
            process_model_tables(enabled_tables, "yearly", wait_for_monthly, yearly_completed, dag)
        except Exception as e:
            logger.error(f"获取 yearly 模型表时出错: {str(e)}")
            # 出错时也要确保完成标记被触发
            wait_for_monthly >> yearly_completed
            raise
    else:
        # 如果不是年初，直接将等待任务与完成标记相连接，跳过处理
        logger.info("今天不是年初，跳过年模型处理")
        wait_for_monthly >> yearly_completed 