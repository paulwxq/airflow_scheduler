from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from utils import (
    get_enabled_tables,
    get_resource_subscribed_tables,
    get_dependency_resource_tables
)
import pendulum
import logging
import sys

# 创建日志记录器
logger = logging.getLogger(__name__)

def get_script_name_from_neo4j(table_name):
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

    script_name = get_script_name_from_neo4j(table_name)
    if not script_name:
        logger.warning(f"未找到表 {table_name} 的 script_name，跳过")
        return

    # scripts_base_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "dataops", "scripts")
    # script_path = os.path.join(scripts_base_path, script_name)
    # 使用配置文件中的绝对路径
    from pathlib import Path
    from config import SCRIPTS_BASE_PATH
    script_path = Path(SCRIPTS_BASE_PATH) / script_name

    if not os.path.exists(script_path):
        logger.error(f"脚本文件不存在: {script_path}")
        return

    logger.info(f"执行脚本: {script_path}")
    try:
        spec = importlib.util.spec_from_file_location("dynamic_script", script_path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        if hasattr(module, "run"):
            module.run(table_name=table_name, execution_mode=execution_mode)
        else:
            logger.warning(f"脚本 {script_name} 中未定义 run(...) 方法，跳过")
    except Exception as e:
        logger.error(f"执行脚本 {script_name} 时出错: {str(e)}")

with DAG("dag_data_resource", start_date=datetime(2024, 1, 1), schedule_interval="@daily", catchup=False) as dag:
    today = pendulum.today()
    is_monday = today.day_of_week == 0
    is_first_day = today.day == 1

    all_resource_tables = []

    # Daily
    daily_enabled = get_enabled_tables("daily")
    all_resource_tables.extend(get_resource_subscribed_tables(daily_enabled))
    all_resource_tables.extend(get_dependency_resource_tables(daily_enabled))

    # Weekly
    if is_monday:
        weekly_enabled = get_enabled_tables("weekly")
        all_resource_tables.extend(get_resource_subscribed_tables(weekly_enabled))
        all_resource_tables.extend(get_dependency_resource_tables(weekly_enabled))

    # Monthly
    if is_first_day:
        monthly_enabled = get_enabled_tables("monthly")
        all_resource_tables.extend(get_resource_subscribed_tables(monthly_enabled))
        all_resource_tables.extend(get_dependency_resource_tables(monthly_enabled))

    # 去重（按表名）
    unique_resources = {}
    for item in all_resource_tables:
        name = item["table_name"]
        if name not in unique_resources:
            unique_resources[name] = item

    resource_tables = list(unique_resources.values())

    for item in resource_tables:
        PythonOperator(
            task_id=f"load_{item['table_name']}",
            python_callable=load_table_data,
            op_kwargs={"table_name": item['table_name'], "execution_mode": item['execution_mode']},
        )