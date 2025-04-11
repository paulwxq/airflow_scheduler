# dag_data_resource_scheduler.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
import pendulum
import logging
import os
from pathlib import Path
from neo4j import GraphDatabase
from utils import (
    get_enabled_tables,
    get_resource_subscribed_tables,
    get_dependency_resource_tables,
    check_script_exists
)
from config import NEO4J_CONFIG, SCRIPTS_BASE_PATH

# 创建日志记录器
logger = logging.getLogger(__name__)

def get_resource_script_name_from_neo4j(table_name):
    """
    从Neo4j数据库中查询DataResource表对应的脚本名称
    这个函数直接在当前文件中实现，而不是从utils导入
    
    参数:
        table_name (str): 数据资源表名
        
    返回:
        str: 脚本名称，如果未找到则返回None
    """
    # 使用导入的Neo4j配置
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
    """执行数据资源表加载脚本的函数"""
    script_name = get_resource_script_name_from_neo4j(table_name)
    if not script_name:
        logger.warning(f"未找到表 {table_name} 的 script_name，跳过")
        return False
    
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
    "dag_data_resource_scheduler", 
    start_date=datetime(2024, 1, 1), 
    schedule_interval="@daily", 
    catchup=False,
) as dag:
    # 获取当前日期信息
    today = pendulum.today()
    # 原始代码（注释）
    # is_monday = today.day_of_week == 0
    # is_first_day_of_month = today.day == 1
    # is_first_day_of_year = today.month == 1 and today.day == 1
    
    # 测试用：所有条件设为True
    is_monday = True
    is_first_day_of_month = True
    is_first_day_of_year = True

    all_resource_tables = []

    # 使用循环处理不同频率的表
    frequency_configs = [
        {"name": "daily", "condition": True},
        {"name": "weekly", "condition": is_monday},
        {"name": "monthly", "condition": is_first_day_of_month},
        {"name": "yearly", "condition": is_first_day_of_year}
    ]

    # 记录日期信息
    logger.info(f"今日日期: {today}, 是否周一: {is_monday}, 是否月初: {is_first_day_of_month}, 是否年初: {is_first_day_of_year}")
    logger.info(f"脚本基础路径: {SCRIPTS_BASE_PATH}")

    # 收集所有需要处理的资源表
    for config in frequency_configs:
        frequency = config["name"]
        if config["condition"]:
            logger.info(f"今天需要处理 {frequency} 频率的资源表")
            enabled_tables = get_enabled_tables(frequency)
            resource_tables = get_resource_subscribed_tables(enabled_tables)
            dependency_tables = get_dependency_resource_tables(enabled_tables)
            
            all_resource_tables.extend(resource_tables)
            all_resource_tables.extend(dependency_tables)
            logger.info(f"已添加 {frequency} 频率的资源表，共 {len(resource_tables) + len(dependency_tables)} 个")
        else:
            logger.info(f"今天不需要处理 {frequency} 频率的资源表")

    # 去重（按表名）
    unique_resources = {}
    for item in all_resource_tables:
        name = item["table_name"]
        if name not in unique_resources:
            unique_resources[name] = item

    resource_tables = list(unique_resources.values())
    logger.info(f"去重后，共需处理 {len(resource_tables)} 个资源表")
    
    # 创建开始任务
    start_loading = EmptyOperator(task_id="start_resource_loading")
    
    # 创建结束任务
    end_loading = EmptyOperator(task_id="resource_loading_completed")
    
    if resource_tables:
        for item in resource_tables:
            table_name = item['table_name']
            execution_mode = item['execution_mode']
            
            logger.info(f"为资源表 {table_name} 创建加载任务")
            task = PythonOperator(
                task_id=f"load_{table_name}",
                python_callable=load_table_data,
                op_kwargs={"table_name": table_name, "execution_mode": execution_mode},
            )
            
            # 设置依赖
            start_loading >> task >> end_loading
    else:
        logger.info("没有资源表需要处理，直接连接开始和结束任务")
        # 如果没有任务，确保开始和结束任务相连
        start_loading >> end_loading