# dag_dependency_analysis.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.param import Param
from datetime import datetime, timedelta
import logging
import os
from pathlib import Path
import networkx as nx
from neo4j import GraphDatabase
from config import NEO4J_CONFIG, SCRIPTS_BASE_PATH

# 导入工具函数
from utils import (
    get_pg_conn, is_data_model_table, is_data_resource_table, 
    get_script_name_from_neo4j, execute_script,
    check_script_exists, run_model_script
)

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

def get_execution_mode(table_name):
    """
    从PostgreSQL获取表的执行模式
    
    参数:
        table_name (str): 表名
    
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

def get_table_metadata(table_name):
    """
    获取表的元数据信息
    
    参数:
        table_name (str): 表名
    
    返回:
        dict: 表的元数据
    """
    driver = GraphDatabase.driver(
        NEO4J_CONFIG['uri'], 
        auth=(NEO4J_CONFIG['user'], NEO4J_CONFIG['password'])
    )
    
    metadata = {
        'table_name': table_name,
        'type': None,
        'script_name': None,
        'execution_mode': get_execution_mode(table_name)
    }
    
    try:
        # 判断表类型
        if is_data_model_table(table_name):
            metadata['type'] = 'DataModel'
        elif is_data_resource_table(table_name):
            metadata['type'] = 'DataResource'
        else:
            # 尝试查询是否为DataSource类型
            with driver.session() as session:
                query = """
                    MATCH (ds:DataSource {en_name: $table_name})
                    RETURN count(ds) > 0 AS exists
                """
                result = session.run(query, table_name=table_name)
                record = result.single()
                if record and record['exists']:
                    metadata['type'] = 'DataSource'
        
        # 查询脚本名称
        if metadata['type'] in ['DataModel', 'DataResource']:
            metadata['script_name'] = get_script_name_from_neo4j(table_name)
            
        return metadata
    finally:
        driver.close()

def get_upstream_tables(table_name, dependency_level):
    """
    获取表的上游依赖
    
    参数:
        table_name (str): 表名
        dependency_level (str): 依赖级别 (self/resource/source)
    
    返回:
        list: 上游表名列表
    """
    # 如果只需要自身，返回空列表
    if dependency_level == 'self':
        return []
    
    driver = GraphDatabase.driver(
        NEO4J_CONFIG['uri'], 
        auth=(NEO4J_CONFIG['user'], NEO4J_CONFIG['password'])
    )
    
    upstream_tables = []
    
    try:
        with driver.session() as session:
            # 根据依赖级别构建不同的查询
            if dependency_level == 'resource':
                # 查询上游DataModel和DataResource表
                query = """
                    MATCH (target {en_name: $table_name})-[:DERIVED_FROM]->(up)
                    WHERE up:DataModel OR up:DataResource
                    RETURN up.en_name AS upstream, labels(up) AS types
                """
            else:  # source级别
                # 查询所有上游表，包括DataSource
                query = """
                    MATCH (target {en_name: $table_name})-[:DERIVED_FROM]->(up)
                    RETURN up.en_name AS upstream, labels(up) AS types
                """
                
            result = session.run(query, table_name=table_name)
            for record in result:
                upstream_tables.append({
                    'table_name': record['upstream'],
                    'type': record['types'][0] if record['types'] else 'Unknown'
                })
                
        return upstream_tables
    finally:
        driver.close()

def build_dependency_graph(start_table, dependency_level):
    """
    构建依赖图
    
    参数:
        start_table (str): 起始表名
        dependency_level (str): 依赖级别 (self/resource/source)
    
    返回:
        tuple: (表信息字典, 依赖图)
    """
    logger.info(f"开始构建 {start_table} 的依赖图，依赖级别: {dependency_level}")
    
    # 创建有向图
    G = nx.DiGraph()
    
    # 添加起始节点
    G.add_node(start_table)
    
    # 记录表信息的字典
    table_info = {}
    
    # 获取起始表的元数据
    table_metadata = get_table_metadata(start_table)
    table_info[start_table] = table_metadata
    
    # 如果依赖级别为self，只返回起始表的信息
    if dependency_level == 'self':
        logger.info(f"依赖级别为'self'，只包含起始表: {start_table}")
        return table_info, G
    
    # 记录已访问的表，避免循环
    visited = set()
    
    def add_dependencies(table_name):
        """递归添加依赖到图中"""
        if table_name in visited:
            return
        
        visited.add(table_name)
        
        # 获取上游依赖
        upstream_tables = get_upstream_tables(table_name, dependency_level)
        
        for upstream in upstream_tables:
            up_table_name = upstream['table_name']
            
            # 添加节点和边
            G.add_node(up_table_name)
            G.add_edge(up_table_name, table_name)  # 上游指向下游，执行时上游先执行
            
            # 递归处理上游依赖
            if up_table_name not in table_info:
                up_metadata = get_table_metadata(up_table_name)
                table_info[up_table_name] = up_metadata
                
                # 如果是resource级别，不继续处理DataSource节点
                if dependency_level == 'resource' and up_metadata['type'] == 'DataSource':
                    continue
                    
                add_dependencies(up_table_name)
    
    # 开始递归构建依赖图
    add_dependencies(start_table)
    
    # 检测和处理循环依赖
    cycles = list(nx.simple_cycles(G))
    if cycles:
        logger.warning(f"检测到循环依赖: {cycles}")
        for cycle in cycles:
            # 移除循环中的最后一条边来打破循环
            G.remove_edge(cycle[-1], cycle[0])
            logger.info(f"打破循环依赖: 移除 {cycle[-1]} -> {cycle[0]} 的依赖")
    
    return table_info, G

def optimize_execution_order(dependency_graph):
    """
    优化执行顺序
    
    参数:
        dependency_graph: NetworkX依赖图
        
    返回:
        list: 优化后的执行顺序
    """
    # 使用拓扑排序生成执行顺序
    try:
        execution_order = list(nx.topological_sort(dependency_graph))
        logger.info(f"生成拓扑排序: {execution_order}")
        return execution_order
    except nx.NetworkXUnfeasible:
        logger.error("无法生成拓扑排序，图可能仍然包含循环")
        # 返回图中的所有节点作为备选
        return list(dependency_graph.nodes())

def analyze_and_prepare_dag(**context):
    """
    分析依赖关系并准备DAG结构，但不执行任何脚本
    """
    # 获取参数
    params = context['params']
    target_table = params.get('TABLE_NAME')
    dependency_level = params.get('DEPENDENCY_LEVEL', 'resource')
    
    if not target_table:
        raise ValueError("必须提供TABLE_NAME参数")
    
    logger.info(f"开始分析表 {target_table} 的依赖，依赖级别: {dependency_level}")
    
    # 构建依赖图
    table_info, dependency_graph = build_dependency_graph(target_table, dependency_level)
    
    if not table_info:
        logger.warning(f"没有找到表 {target_table} 的依赖信息")
        return {}
    
    # 优化执行顺序
    execution_order = optimize_execution_order(dependency_graph)
    
    # 过滤掉没有脚本的表
    executable_tables = [
        table_name for table_name in execution_order 
        if table_name in table_info and table_info[table_name]['script_name']
    ]
    
    logger.info(f"需要执行的表: {executable_tables}")
    
    # 返回执行计划，包含每个表的信息和执行顺序
    execution_plan = {
        'executable_tables': executable_tables,
        'table_info': {k: v for k, v in table_info.items() if k in executable_tables},
        'dependencies': {
            k: list(dependency_graph.predecessors(k)) 
            for k in executable_tables
        }
    }
    
    return execution_plan

# 创建DAG
with DAG(
    'dag_dependency_analysis',
    default_args=default_args,
    description='分析表依赖路径并执行相关脚本',
    schedule_interval=None,  # 设置为None表示只能手动触发
    catchup=False,
    is_paused_upon_creation=False,
    params={
        'TABLE_NAME': Param('', type='string', description='目标表名称'),
        'DEPENDENCY_LEVEL': Param('resource', type='string', enum=['self', 'resource', 'source'], description='依赖级别: self-仅本表, resource-到Resource层, source-到Source层')
    },
) as dag:
    # 创建分析依赖的任务
    analyze_task = PythonOperator(
        task_id='analyze_dependencies',
        python_callable=analyze_and_prepare_dag,
        provide_context=True,
        dag=dag,
    )
    
    # 动态确定要执行的任务列表
    def determine_and_create_tasks(**context):
        """
        根据分析结果确定要执行的任务，并动态创建任务
        """
        # 获取analyze_dependencies任务的输出
        ti = context['ti']
        execution_plan = ti.xcom_pull(task_ids='analyze_dependencies')
        
        if not execution_plan or 'executable_tables' not in execution_plan:
            logger.warning("未获取到执行计划，无法创建任务")
            return None
        
        executable_tables = execution_plan.get('executable_tables', [])
        table_info = execution_plan.get('table_info', {})
        dependencies = execution_plan.get('dependencies', {})
        
        if not executable_tables:
            logger.warning("没有表需要执行")
            return None
        
        # 记录执行计划
        logger.info(f"要执行的表: {executable_tables}")
        for table_name in executable_tables:
            logger.info(f"表 {table_name} 的信息: {table_info.get(table_name, {})}")
            logger.info(f"表 {table_name} 的依赖: {dependencies.get(table_name, [])}")
        
        # 为每个需要执行的表创建任务
        for table_name in executable_tables:
            table_data = table_info.get(table_name, {})
            execution_mode = table_data.get('execution_mode', 'append')
            
            # 创建处理任务
            task = PythonOperator(
                task_id=f'process_{table_name}',
                python_callable=run_model_script,
                op_kwargs={
                    'table_name': table_name,
                    'execution_mode': execution_mode
                },
                dag=dag,
            )
            
            # 设置依赖关系
            # 当前表依赖的上游表
            upstream_tables = dependencies.get(table_name, [])
            # 过滤出在executable_tables中的上游表
            upstream_tables = [t for t in upstream_tables if t in executable_tables]
            
            for upstream in upstream_tables:
                # 获取上游任务（假设已经创建）
                upstream_task = dag.get_task(f'process_{upstream}')
                if upstream_task:
                    # 设置依赖: 上游任务 >> 当前任务
                    upstream_task >> task
                    logger.info(f"设置任务依赖: process_{upstream} >> process_{table_name}")
            
            # 如果没有上游任务，直接依赖于分析任务
            if not upstream_tables:
                analyze_task >> task
                logger.info(f"设置任务依赖: analyze_dependencies >> process_{table_name}")
        
        # 找到没有依赖的第一个表（入口任务）
        entry_tables = [
            table for table in executable_tables
            if not dependencies.get(table, [])
        ]
        
        # 返回入口任务的ID，如果有的话
        if entry_tables:
            return f'process_{entry_tables[0]}'
        else:
            # 如果没有明确的入口任务，使用第一个表
            return f'process_{executable_tables[0]}'
    
    # 使用BranchPythonOperator
    branch_task = PythonOperator(
        task_id='branch_and_create_tasks',
        python_callable=determine_and_create_tasks,
        provide_context=True,
        dag=dag,
    )
    
    # 设置基本任务流
    analyze_task >> branch_task 