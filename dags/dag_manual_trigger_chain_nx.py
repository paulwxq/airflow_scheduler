# dag_manual_dependency_trigger.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging
import importlib.util
import os
from pathlib import Path
from neo4j import GraphDatabase
import psycopg2
import networkx as nx
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
    dependency_level = params.get('DEPENDENCY_LEVEL', 'resource')  # 默认值为resource
    
    if not table_name:
        raise ValueError("必须提供TABLE_NAME参数")
    
    # 验证dependency_level参数
    if dependency_level not in ['self', 'resource', 'source']:
        logger.warning(f"无效的DEPENDENCY_LEVEL参数: {dependency_level}，使用默认值'resource'")
        dependency_level = 'resource'
    
    logger.info(f"开始处理表: {table_name}, 依赖级别: {dependency_level}")
    return table_name, dependency_level

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

def build_dependency_chain_nx(start_table, dependency_level='resource'):
    """
    使用networkx构建依赖链
    
    参数:
        start_table (str): 起始表名
        dependency_level (str): 依赖级别
            - 'self': 只执行自己
            - 'resource': 到Resource层 (默认)
            - 'source': 到Source层
        
    返回:
        list: 依赖链列表，按执行顺序排序（从上游到下游）
    """
    # 创建有向图
    G = nx.DiGraph()
    
    # 设置起始节点属性
    if is_data_model_table(start_table):
        G.add_node(start_table, type='DataModel')
        table_type = 'DataModel'
    elif is_data_resource_table(start_table):
        G.add_node(start_table, type='DataResource')
        table_type = 'DataResource'
    else:
        logger.warning(f"表 {start_table} 不是DataModel或DataResource类型")
        return []
    
    # 如果只执行自己，直接返回
    if dependency_level == 'self':
        script_name = get_script_name_for_model(start_table) if table_type == 'DataModel' else get_script_name_for_resource(start_table)
        execution_mode = get_execution_mode(start_table)
        return [{
            'table_name': start_table,
            'script_name': script_name,
            'table_type': table_type,
            'execution_mode': execution_mode
        }]
    
    # BFS构建依赖图
    visited = set([start_table])
    queue = [start_table]
    
    while queue:
        current = queue.pop(0)
        
        # 处理当前节点的上游依赖
        if G.nodes[current].get('type') == 'DataModel':
            # 获取DataModel的上游依赖
            upstream_models = get_upstream_models(current)
            for upstream in upstream_models:
                if upstream not in visited:
                    G.add_node(upstream, type='DataModel')
                    visited.add(upstream)
                    queue.append(upstream)
                G.add_edge(current, upstream, type='model_to_model')
            
            # 获取上游DataResource
            upstream_resources = get_upstream_resources(current)
            for upstream in upstream_resources:
                if upstream not in visited:
                    G.add_node(upstream, type='DataResource')
                    visited.add(upstream)
                    # 如果依赖级别为source并且上游是DataResource，则继续向上查找DataSource
                    if dependency_level == 'source':
                        queue.append(upstream)
                G.add_edge(current, upstream, type='model_to_resource')
        
        # 如果当前节点是DataResource且依赖级别为source，则查找上游DataSource
        elif G.nodes[current].get('type') == 'DataResource' and dependency_level == 'source':
            data_sources = get_data_sources(current)
            for source in data_sources:
                if source not in visited:
                    G.add_node(source, type='DataSource')
                    visited.add(source)
                G.add_edge(current, source, type='resource_to_source')
    
    # 检测循环依赖
    cycles = list(nx.simple_cycles(G))
    if cycles:
        logger.warning(f"检测到循环依赖，将尝试打破循环: {cycles}")
        # 打破循环依赖（简单策略：移除每个循环中的一条边）
        for cycle in cycles:
            G.remove_edge(cycle[-1], cycle[0])
            logger.info(f"打破循环依赖: 移除 {cycle[-1]} -> {cycle[0]} 的依赖")
    
    # 生成拓扑排序（从上游到下游的顺序）
    try:
        # 注意：拓扑排序给出的是从上游到下游的顺序
        # 我们需要的是执行顺序，所以要反转图然后进行拓扑排序
        reverse_G = G.reverse()
        execution_order = list(nx.topological_sort(reverse_G))
        
        # 构建最终依赖链
        dependency_chain = []
        for table_name in execution_order:
            node_type = G.nodes[table_name].get('type')
            
            # 跳过DataSource节点，它们没有脚本需要执行
            if node_type == 'DataSource':
                continue
            
            # 获取脚本和执行模式
            if node_type == 'DataModel':
                script_name = get_script_name_for_model(table_name)
            else:  # DataResource
                script_name = get_script_name_for_resource(table_name)
            
            execution_mode = get_execution_mode(table_name)
            
            dependency_chain.append({
                'table_name': table_name,
                'script_name': script_name,
                'table_type': node_type,
                'execution_mode': execution_mode
            })
        
        return dependency_chain
    
    except Exception as e:
        logger.error(f"生成拓扑排序时出错: {str(e)}")
        return []

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
    table_name, dependency_level = get_dag_params(**context)
    
    # 获取依赖链
    dependency_chain = build_dependency_chain_nx(table_name, dependency_level)
    
    if not dependency_chain:
        logger.warning(f"没有找到表 {table_name} 的依赖链")
        return False
    
    # 记录完整依赖链
    logger.info(f"依赖链完整列表: {[item['table_name'] for item in dependency_chain]}")
    
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
    'dag_manual_dependency_trigger',
    default_args=default_args,
    description='手动触发指定表的依赖链执行（使用networkx优化依赖路径）',
    schedule_interval=None,  # 设置为None表示只能手动触发
    catchup=False,
    is_paused_upon_creation=False,  # 添加这一行，使DAG创建时不处于暂停状态
    params={
        'TABLE_NAME': '',
        'DEPENDENCY_LEVEL': {
            'type': 'string',
            'enum': ['self', 'resource', 'source'],
            'default': 'resource',
            'description': '依赖级别: self-仅本表, resource-到Resource层, source-到Source层'
        }
    },
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