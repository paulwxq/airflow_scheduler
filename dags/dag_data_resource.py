from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from utils import (
    get_enabled_tables,
    get_resource_subscribed_tables,
    get_dependency_resource_tables,
    check_script_exists
)
import pendulum
import logging
import sys
from airflow.operators.empty import EmptyOperator
from config import NEO4J_CONFIG, SCRIPTS_BASE_PATH
from neo4j import GraphDatabase

# 创建日志记录器
logger = logging.getLogger(__name__)

def get_resource_script_name_from_neo4j(table_name):
    from neo4j import GraphDatabase
    from config import NEO4J_CONFIG
    
    # 正确处理Neo4j连接参数
    uri = NEO4J_CONFIG['uri']
    auth = (NEO4J_CONFIG['user'], NEO4J_CONFIG['password'])
    driver = GraphDatabase.driver(uri, auth=auth)
    
    query = """
        MATCH (dr:DataResource {en_name: $table_name})-[rel:ORIGINATES_FROM]->(ds:DataSource)
        RETURN rel.script_name AS script_name
    """
    try:
        with driver.session() as session:
            result = session.run(query, table_name=table_name)
            record = result.single()
            if record:
                logger.info(f"找到表 {table_name} 的完整记录: {record}")
                try:
                    script_name = record['script_name']
                    logger.info(f"找到表 {table_name} 的 script_name: {script_name}")
                    return script_name
                except (KeyError, TypeError) as e:
                    logger.warning(f"记录中不包含script_name字段: {e}")
                    return None
            else:
                logger.warning(f"未找到表 {table_name} 的记录")
                return None
    except Exception as e:
        logger.error(f"查询表 {table_name} 的脚本名称时出错: {str(e)}")
        return None
    finally:
        driver.close()

def load_table_data(table_name, execution_mode):
    import os
    import importlib.util

    script_name = get_resource_script_name_from_neo4j(table_name)
    if not script_name:
        logger.warning(f"未找到表 {table_name} 的 script_name，跳过")
        return
    
    logger.info(f"从Neo4j获取到表 {table_name} 的脚本名称: {script_name}")
    
    # 检查脚本文件是否存在
    exists, script_path = check_script_exists(script_name)
    if not exists:
        logger.error(f"表 {table_name} 的脚本文件 {script_name} 不存在，跳过处理")
        return False
    
    # 执行脚本
    logger.info(f"开始执行脚本: {script_path}")
    try:
        # 动态导入模块
        import importlib.util
        import sys
        
        spec = importlib.util.spec_from_file_location("dynamic_module", script_path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        
        # 检查并调用标准入口函数run
        if hasattr(module, "run"):
            logger.info(f"调用脚本 {script_name} 的标准入口函数 run()")
            module.run(table_name=table_name, execution_mode=execution_mode)
            logger.info(f"脚本 {script_name} 执行成功")
            return True
        else:
            logger.error(f"脚本 {script_name} 中未定义标准入口函数 run()，无法执行")
            return False
    except Exception as e:
        logger.error(f"执行脚本 {script_name} 时出错: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return False

with DAG(
    "dag_data_resource", 
    start_date=datetime(2024, 1, 1), 
    schedule_interval="@daily", 
    catchup=False,
    # 添加DAG级别的并行度控制，每个DAG运行最多同时执行2个任务
    max_active_tasks=8
    # 无论有多少个DAG运行实例（比如昨天、今天的运行），这个DAG定义的所有实例总共最多有5个任务同时运行
    # concurrency=5
) as dag:
    today = pendulum.today()
    is_monday = today.day_of_week == 0
    is_first_day_of_month = today.day == 1
    is_first_day_of_year = today.month == 1 and today.day == 1

    all_resource_tables = []

    # 使用循环处理不同频率的表
    frequency_configs = [
        {"name": "daily", "condition": True},
        {"name": "weekly", "condition": is_monday},
        {"name": "monthly", "condition": is_first_day_of_month},
        {"name": "yearly", "condition": is_first_day_of_year}
    ]

    for config in frequency_configs:
        frequency = config["name"]
        if config["condition"]:
            enabled_tables = get_enabled_tables(frequency)
            all_resource_tables.extend(get_resource_subscribed_tables(enabled_tables))
            all_resource_tables.extend(get_dependency_resource_tables(enabled_tables))
            logger.info(f"已添加 {frequency} 频率的资源表")

    # 去重（按表名）
    unique_resources = {}
    for item in all_resource_tables:
        name = item["table_name"]
        if name not in unique_resources:
            unique_resources[name] = item

    resource_tables = list(unique_resources.values())
    
    # 创建开始任务
    start_loading = EmptyOperator(task_id="start_resource_loading")
    
    # 创建结束任务
    end_loading = EmptyOperator(task_id="finish_resource_loading")
    
    
    for item in resource_tables:
        task = PythonOperator(
            task_id=f"load_{item['table_name']}",
            python_callable=load_table_data,
            op_kwargs={"table_name": item['table_name'], "execution_mode": item['execution_mode']},
        )
        
        # 设置依赖
        start_loading >> task >> end_loading