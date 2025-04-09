# dag_manual_trigger_chain_two_level.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging
import importlib.util
import os
from pathlib import Path
from neo4j import GraphDatabase
import psycopg2
from config import NEO4J_CONFIG, SCRIPTS_BASE_PATH, PG_CONFIG

# 设置logger
logger = logging.getLogger(__name__)

# DAG参数
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def get_pg_conn():
    """获取PostgreSQL连接"""
    return psycopg2.connect(**PG_CONFIG)

def get_execution_mode(table_name):
    """
    从PostgreSQL获取表的执行模式    
    参数:
        table_name (str): 表名
    注意：
        "AND is_enabled = TRUE" 这个条件在这里不适用，因为这是强制执行的。
        即使订阅表中没有这个表名，也会强制执行。
    返回:
        str: 执行模式，如果未找到则返回"append"作为默认值
    """
    try:
        conn = get_pg_conn()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT execution_mode 
            FROM table_schedule 
            WHERE table_name = %s
        """, (table_name,))
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        
        if result:
            return result[0]
        else:
            logger.warning(f"未找到表 {table_name} 的执行模式，使用默认值 'append'")
            return "append"
    except Exception as e:
        logger.error(f"获取表 {table_name} 的执行模式时出错: {str(e)}")
        return "append"

def get_dag_params(**context):
    """获取DAG运行参数"""
    params = context.get('params', {})
    table_name = params.get('TABLE_NAME')
    upper_level_stop = params.get('UPPER_LEVEL_STOP', 'DataResource')  # 默认值为DataResource
    
    if not table_name:
        raise ValueError("必须提供TABLE_NAME参数")
    
    logger.info(f"开始处理表: {table_name}, 上游停止级别: {upper_level_stop}")
    return table_name, upper_level_stop

def is_data_model_table(table_name):
    """判断表是否为DataModel类型"""
    driver = GraphDatabase.driver(
        NEO4J_CONFIG['uri'], 
        auth=(NEO4J_CONFIG['user'], NEO4J_CONFIG['password'])
    )
    query = """
        MATCH (n:DataModel {en_name: $table_name}) RETURN count(n) > 0 AS exists
    """
    try:
        with driver.session() as session:
            result = session.run(query, table_name=table_name)
            record = result.single()
            return record and record["exists"]
    finally:
        driver.close()

def is_data_resource_table(table_name):
    """判断表是否为DataResource类型"""
    driver = GraphDatabase.driver(
        NEO4J_CONFIG['uri'], 
        auth=(NEO4J_CONFIG['user'], NEO4J_CONFIG['password'])
    )
    query = """
        MATCH (n:DataResource {en_name: $table_name}) RETURN count(n) > 0 AS exists
    """
    try:
        with driver.session() as session:
            result = session.run(query, table_name=table_name)
            record = result.single()
            return record and record["exists"]
    finally:
        driver.close()

def get_upstream_models(table_name):
    """获取表的上游DataModel依赖"""
    driver = GraphDatabase.driver(
        NEO4J_CONFIG['uri'], 
        auth=(NEO4J_CONFIG['user'], NEO4J_CONFIG['password'])
    )
    query = """
        MATCH (target:DataModel {en_name: $table_name})-[:DERIVED_FROM]->(up:DataModel)
        RETURN up.en_name AS upstream
    """
    try:
        with driver.session() as session:
            result = session.run(query, table_name=table_name)
            upstream_list = [record["upstream"] for record in result]
            logger.info(f"表 {table_name} 的上游DataModel依赖: {upstream_list}")
            return upstream_list
    finally:
        driver.close()

def get_upstream_resources(table_name):
    """获取表的上游DataResource依赖"""
    driver = GraphDatabase.driver(
        NEO4J_CONFIG['uri'], 
        auth=(NEO4J_CONFIG['user'], NEO4J_CONFIG['password'])
    )
    query = """
        MATCH (target:DataModel {en_name: $table_name})-[:DERIVED_FROM]->(up:DataResource)
        RETURN up.en_name AS upstream
    """
    try:
        with driver.session() as session:
            result = session.run(query, table_name=table_name)
            upstream_list = [record["upstream"] for record in result]
            logger.info(f"表 {table_name} 的上游DataResource依赖: {upstream_list}")
            return upstream_list
    finally:
        driver.close()

def get_data_sources(resource_table_name):
    """获取DataResource表的上游DataSource"""
    driver = GraphDatabase.driver(
        NEO4J_CONFIG['uri'], 
        auth=(NEO4J_CONFIG['user'], NEO4J_CONFIG['password'])
    )
    query = """
        MATCH (dr:DataResource {en_name: $table_name})-[:ORIGINATES_FROM]->(ds:DataSource)
        RETURN ds.en_name AS source_name
    """
    try:
        with driver.session() as session:
            result = session.run(query, table_name=resource_table_name)
            return [record["source_name"] for record in result]
    finally:
        driver.close()

def get_script_name_for_model(table_name):
    """获取DataModel表对应的脚本名称"""
    driver = GraphDatabase.driver(
        NEO4J_CONFIG['uri'], 
        auth=(NEO4J_CONFIG['user'], NEO4J_CONFIG['password'])
    )
    query = """
        MATCH (target:DataModel {en_name: $table_name})-[r:DERIVED_FROM]->(n)
        WHERE n:DataModel OR n:DataResource
        RETURN r.script_name AS script_name
    """
    try:
        with driver.session() as session:
            result = session.run(query, table_name=table_name)
            record = result.single()
            if record:
                return record["script_name"]
            else:
                logger.warning(f"未找到DataModel表 {table_name} 的脚本名称")
                return None
    except Exception as e:
        logger.error(f"查询表 {table_name} 的脚本名称时出错: {str(e)}")
        return None
    finally:
        driver.close()

def get_script_name_for_resource(table_name):
    """获取DataResource表对应的脚本名称"""
    driver = GraphDatabase.driver(
        NEO4J_CONFIG['uri'], 
        auth=(NEO4J_CONFIG['user'], NEO4J_CONFIG['password'])
    )
    query = """
        MATCH (dr:DataResource {en_name: $table_name})-[rel:ORIGINATES_FROM]->(ds:DataSource)
        RETURN rel.script_name AS script_name
    """
    try:
        with driver.session() as session:
            result = session.run(query, table_name=table_name)
            record = result.single()
            if record:
                return record["script_name"]
            else:
                logger.warning(f"未找到DataResource表 {table_name} 的脚本名称")
                return None
    except Exception as e:
        logger.error(f"查询表 {table_name} 的脚本名称时出错: {str(e)}")
        return None
    finally:
        driver.close()

def build_dependency_chain(start_table, upper_level_stop='DataResource', visited=None):
    """
    递归构建依赖链
    
    参数:
        start_table (str): 起始表名
        upper_level_stop (str): 上游停止级别
        visited (set): 已访问的表集合，避免循环依赖
        
    返回:
        list: 依赖链列表，按执行顺序排序（从上游到下游）
    """
    if visited is None:
        visited = set()
    
    if start_table in visited:
        return []
    
    visited.add(start_table)
    dependency_chain = []
    
    # 判断表类型
    if is_data_model_table(start_table):
        # 处理DataModel表
        script_name = get_script_name_for_model(start_table)
        execution_mode = get_execution_mode(start_table)
        
        # 获取上游DataModel
        upstream_models = get_upstream_models(start_table)
        for upstream in upstream_models:
            # 将上游依赖添加到链条前面
            upstream_chain = build_dependency_chain(upstream, upper_level_stop, visited)
            dependency_chain.extend(upstream_chain)
        
        # 获取上游DataResource
        upstream_resources = get_upstream_resources(start_table)
        for upstream in upstream_resources:
            # 将上游依赖添加到链条前面
            upstream_chain = build_dependency_chain(upstream, upper_level_stop, visited)
            dependency_chain.extend(upstream_chain)
        
        # 当前表添加到链条末尾
        dependency_chain.append({
            'table_name': start_table,
            'script_name': script_name,
            'table_type': 'DataModel',
            'execution_mode': execution_mode
        })
        
    elif is_data_resource_table(start_table):
        # 处理DataResource表
        script_name = get_script_name_for_resource(start_table)
        execution_mode = get_execution_mode(start_table)
        
        # 如果上游停止级别为DataSource，则继续查找DataSource并先添加
        if upper_level_stop == 'DataSource':
            data_sources = get_data_sources(start_table)
            for source in data_sources:
                dependency_chain.append({
                    'table_name': source,
                    'script_name': None,  # DataSource没有脚本
                    'table_type': 'DataSource',
                    'execution_mode': None
                })
        
        # 当前DataResource表添加到链条末尾
        dependency_chain.append({
            'table_name': start_table,
            'script_name': script_name,
            'table_type': 'DataResource',
            'execution_mode': execution_mode
        })
    
    return dependency_chain

def execute_scripts(scripts_list):
    """
    执行指定的脚本列表
    
    参数:
        scripts_list (list): 要执行的脚本信息列表，每项包含table_name, script_name, execution_mode
        
    返回:
        bool: 全部执行成功返回True，任一失败返回False
    """
    if not scripts_list:
        logger.info("没有脚本需要执行")
        return True
    
    success = True
    for item in scripts_list:
        script_name = item['script_name']
        table_name = item['table_name']
        execution_mode = item['execution_mode']
        
        if not script_name:
            logger.warning(f"表 {table_name} 没有对应的脚本，跳过执行")
            continue
        
        logger.info(f"执行脚本: {script_name}, 表: {table_name}, 模式: {execution_mode}")
        
        try:
            script_path = Path(SCRIPTS_BASE_PATH) / script_name
            
            if not os.path.exists(script_path):
                logger.error(f"脚本文件不存在: {script_path}")
                success = False
                break
            
            # 动态导入模块
            spec = importlib.util.spec_from_file_location("dynamic_module", script_path)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            
            # 使用标准入口函数run
            if hasattr(module, "run"):
                logger.info(f"执行脚本 {script_name} 的标准入口函数 run()")
                result = module.run(table_name=table_name, execution_mode=execution_mode)
                if result:
                    logger.info(f"脚本 {script_name} 执行成功")
                else:
                    logger.error(f"脚本 {script_name} 执行失败")
                    success = False
                    break
            else:
                logger.warning(f"脚本 {script_name} 未定义标准入口函数 run()，无法执行")
                success = False
                break
        except Exception as e:
            logger.error(f"执行脚本 {script_name} 时出错: {str(e)}")
            success = False
            break
    
    return success

def prepare_dependency_chain(**context):
    """准备依赖链并保存到XCom"""
    # 获取参数
    table_name, upper_level_stop = get_dag_params(**context)
    
    # 获取依赖链
    dependency_chain = build_dependency_chain(table_name, upper_level_stop)
    
    if not dependency_chain:
        logger.warning(f"没有找到表 {table_name} 的依赖链")
        return False
    
    # 记录完整依赖链
    logger.info(f"依赖链完整列表: {[item['table_name'] for item in dependency_chain]}")
    
    # 过滤掉DataSource类型（它们没有脚本需要执行）
    dependency_chain = [item for item in dependency_chain if item['table_type'] != 'DataSource']
    
    # 保存依赖链到XCom以便后续任务使用
    ti = context['ti']
    ti.xcom_push(key='dependency_chain', value=dependency_chain)
    
    # 检查是否有各类型的脚本需要执行
    has_resource = any(item['table_type'] == 'DataResource' for item in dependency_chain)
    has_model = any(item['table_type'] == 'DataModel' for item in dependency_chain)
    
    logger.info(f"是否有DataResource脚本: {has_resource}, 是否有DataModel脚本: {has_model}")
    
    return True

def process_resources(**context):
    """处理所有DataResource层的脚本"""
    # 获取任务间共享变量
    ti = context['ti']
    dependency_chain = ti.xcom_pull(task_ids='prepare_dependency_chain', key='dependency_chain')
    
    # 过滤出DataResource类型的表
    resource_scripts = [item for item in dependency_chain if item['table_type'] == 'DataResource']
    
    logger.info(f"要执行的DataResource脚本: {[item['table_name'] for item in resource_scripts]}")
    
    # 执行所有DataResource脚本
    return execute_scripts(resource_scripts)

def process_models(**context):
    """处理所有DataModel层的脚本"""
    # 获取任务间共享变量
    ti = context['ti']
    dependency_chain = ti.xcom_pull(task_ids='prepare_dependency_chain', key='dependency_chain')
    
    # 过滤出DataModel类型的表
    model_scripts = [item for item in dependency_chain if item['table_type'] == 'DataModel']
    
    logger.info(f"要执行的DataModel脚本: {[item['table_name'] for item in model_scripts]}")
    
    # 执行所有DataModel脚本
    return execute_scripts(model_scripts)

# 创建DAG
with DAG(
    'dag_manual_trigger_chain',
    default_args=default_args,
    description='手动触发指定表的依赖链执行（两级任务）',
    schedule_interval=None,  # 设置为None表示只能手动触发
    catchup=False,
    is_paused_upon_creation=False,  # 添加这一行，使DAG创建时不处于暂停状态
) as dag:
    
    # 第一个任务：准备依赖链
    prepare_task = PythonOperator(
        task_id='prepare_dependency_chain',
        python_callable=prepare_dependency_chain,
        provide_context=True,
    )
    
    # 第二个任务：执行DataResource脚本
    resource_task = PythonOperator(
        task_id='process_resources',
        python_callable=process_resources,
        provide_context=True,
    )
    
    # 第三个任务：执行DataModel脚本
    model_task = PythonOperator(
        task_id='process_models',
        python_callable=process_models,
        provide_context=True,
    )
    
    # 设置任务依赖关系
    prepare_task >> resource_task >> model_task