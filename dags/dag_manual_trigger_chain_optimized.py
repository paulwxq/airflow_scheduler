# dag_manual_trigger_chain_optimized.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
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
    get_model_dependency_graph, generate_optimized_execution_order,
    build_model_dependency_dag, create_task_dict, build_task_dependencies,
    connect_start_and_end_tasks
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

def build_dependency_chain_with_networkx(start_table, dependency_level='resource'):
    """
    使用networkx构建依赖链
    
    参数:
        start_table (str): 起始表名
        dependency_level (str): 依赖级别，可选值：
            'self' - 只包含起始表自身
            'resource' - 包含到DataResource层级（默认）
            'source' - 包含到DataSource层级
        
    返回:
        dict: 依赖图 {表名: 表信息字典}
        networkx.DiGraph: 表示依赖关系的有向图
    """
    logger.info(f"使用networkx构建依赖链, 起始表: {start_table}, 依赖级别: {dependency_level}")
    
    # 创建有向图
    G = nx.DiGraph()
    
    # 添加起始节点
    G.add_node(start_table)
    
    # 记录表类型和脚本信息的字典
    table_info = {}
    
    # 只执行起始表自身
    if dependency_level == 'self':
        # 确定表类型并记录信息
        if is_data_model_table(start_table):
            script_name = get_script_name_from_neo4j(start_table)
            execution_mode = get_execution_mode(start_table)
            table_info[start_table] = {
                'table_name': start_table,
                'script_name': script_name,
                'table_type': 'DataModel',
                'execution_mode': execution_mode
            }
        elif is_data_resource_table(start_table):
            script_name = get_script_name_from_neo4j(start_table)
            execution_mode = get_execution_mode(start_table)
            table_info[start_table] = {
                'table_name': start_table,
                'script_name': script_name,
                'table_type': 'DataResource',
                'execution_mode': execution_mode
            }
        
        logger.info(f"依赖级别为'self'，只处理起始表: {start_table}")
        return table_info, G
    
    # 处理完整依赖链
    # 用于检测循环的已访问集合
    visited = set()
    
    def add_dependencies(table, level):
        """递归添加依赖到图中"""
        if table in visited:
            return
        
        visited.add(table)
        
        # 确定表类型并记录信息
        if is_data_model_table(table):
            script_name = get_script_name_from_neo4j(table)
            execution_mode = get_execution_mode(start_table)
            table_info[table] = {
                'table_name': table,
                'script_name': script_name,
                'table_type': 'DataModel',
                'execution_mode': execution_mode
            }
            
            # 添加DataModel上游依赖
            upstream_models = get_upstream_models(table)
            for upstream in upstream_models:
                G.add_node(upstream)
                G.add_edge(upstream, table)  # 上游指向下游，执行时上游先执行
                add_dependencies(upstream, level)
            
            # 添加DataResource上游依赖
            upstream_resources = get_upstream_resources(table)
            for upstream in upstream_resources:
                G.add_node(upstream)
                G.add_edge(upstream, table)
                add_dependencies(upstream, level)
                
        elif is_data_resource_table(table):
            script_name = get_script_name_from_neo4j(table)
            execution_mode = get_execution_mode(start_table)
            table_info[table] = {
                'table_name': table,
                'script_name': script_name,
                'table_type': 'DataResource',
                'execution_mode': execution_mode
            }
            
            # 如果依赖级别为source，则继续查找DataSource
            if level == 'source':
                data_sources = get_data_sources(table)
                for source in data_sources:
                    G.add_node(source)
                    G.add_edge(source, table)
                    table_info[source] = {
                        'table_name': source,
                        'script_name': None,
                        'table_type': 'DataSource',
                        'execution_mode': None
                    }
    
    # 开始递归构建依赖图
    add_dependencies(start_table, dependency_level)
    
    # 检测和处理循环依赖
    cycles = list(nx.simple_cycles(G))
    if cycles:
        logger.warning(f"检测到循环依赖: {cycles}")
        for cycle in cycles:
            # 移除循环中的最后一条边来打破循环
            G.remove_edge(cycle[-1], cycle[0])
            logger.info(f"打破循环依赖: 移除 {cycle[-1]} -> {cycle[0]} 的依赖")
    
    return table_info, G

def process_table_chain_manual(target_table, dependency_level, **context):
    """
    处理指定表及其依赖链
    
    参数:
        target_table: 目标表名
        dependency_level: 依赖级别
    """
    logger.info(f"开始处理表 {target_table} 的依赖链，依赖级别: {dependency_level}")
    
    # 构建依赖链
    table_info, dependency_graph = build_dependency_chain_with_networkx(target_table, dependency_level)
    
    if not table_info:
        logger.warning(f"没有找到表 {target_table} 的依赖信息")
        return
    
    # 使用networkx生成拓扑排序
    try:
        execution_order = list(nx.topological_sort(dependency_graph))
        logger.info(f"生成拓扑排序结果: {execution_order}")
    except nx.NetworkXUnfeasible:
        logger.error("无法生成拓扑排序，图可能仍然包含循环")
        execution_order = [target_table]  # 至少包含目标表
    
    # 过滤掉DataSource类型的表
    model_tables = []
    for table_name in execution_order:
        if table_name in table_info and table_info[table_name]['table_type'] != 'DataSource':
            model_tables.append({
                'table_name': table_name,
                'script_name': table_info[table_name]['script_name'],
                'execution_mode': table_info[table_name]['execution_mode'],
                'table_type': table_info[table_name]['table_type']
            })
    
    # 按顺序处理表
    processed_count = 0
    failed_tables = []
    
    for table_info in model_tables:
        table_name = table_info['table_name']
        script_name = table_info['script_name']
        execution_mode = table_info['execution_mode']
        
        logger.info(f"处理表: {table_name} ({processed_count + 1}/{len(model_tables)})")
        
        result = execute_script(script_name, table_name, execution_mode)
        
        if result:
            processed_count += 1
            logger.info(f"表 {table_name} 处理成功")
        else:
            failed_tables.append(table_name)
            logger.error(f"表 {table_name} 处理失败")
            # 是否要中断处理？取决于您的要求
            # break
    
    # 处理结果
    logger.info(f"依赖链处理完成: 成功 {processed_count} 个表, 失败 {len(failed_tables)} 个表")
    
    if failed_tables:
        logger.warning(f"失败的表: {', '.join(failed_tables)}")
        return False
        
    return True

def run_model_script(table_name, execution_mode):
    """
    运行模型脚本的封装函数，用于Airflow任务
    
    参数:
        table_name: 表名
        execution_mode: 执行模式
    """
    script_name = get_script_name_from_neo4j(table_name)
    return execute_script(script_name, table_name, execution_mode)

# 创建DAG
with DAG(
    'dag_manual_trigger_chain_optimized',
    default_args=default_args,
    description='手动触发指定表的依赖链执行（串行任务链）',
    schedule_interval=None,  # 设置为None表示只能手动触发
    catchup=False,
    is_paused_upon_creation=False,  # DAG创建时不处于暂停状态
    params={
        'TABLE_NAME': Param('', type='string', description='目标表名称'),
        'DEPENDENCY_LEVEL': Param('resource', type='string', enum=['self', 'resource', 'source'], description='依赖级别: self-仅本表, resource-到Resource层, source-到Source层')
    },
) as dag:
    
    # 起始任务
    start_task = EmptyOperator(
        task_id='start',
        dag=dag,
    )
    
    # 结束任务
    end_task = EmptyOperator(
        task_id='end',
        dag=dag,
    )
    
    # 分析依赖的任务
    def analyze_dependencies(**context):
        """分析依赖链，准备表信息和执行顺序"""
        # 获取参数
        params = context['params']
        target_table = params.get('TABLE_NAME')
        dependency_level = params.get('DEPENDENCY_LEVEL', 'resource')
        
        if not target_table:
            raise ValueError("必须提供TABLE_NAME参数")
            
        # 验证依赖级别参数
        valid_levels = ['self', 'resource', 'source']
        if dependency_level not in valid_levels:
            logger.warning(f"无效的依赖级别: {dependency_level}，使用默认值 'resource'")
            dependency_level = 'resource'
        
        logger.info(f"开始分析表 {target_table} 的依赖链, 依赖级别: {dependency_level}")
        
        # 构建依赖链
        table_info, dependency_graph = build_dependency_chain_with_networkx(target_table, dependency_level)
        
        if not table_info:
            logger.warning(f"没有找到表 {target_table} 的依赖信息")
            return []
        
        # 使用networkx生成拓扑排序
        try:
            execution_order = list(nx.topological_sort(dependency_graph))
            logger.info(f"生成拓扑排序结果: {execution_order}")
        except nx.NetworkXUnfeasible:
            logger.error("无法生成拓扑排序，图可能仍然包含循环")
            execution_order = [target_table]  # 至少包含目标表
        
        # 准备表信息
        model_tables = []
        for table_name in execution_order:
            if table_name in table_info and table_info[table_name]['table_type'] != 'DataSource':
                model_tables.append({
                    'table_name': table_name,
                    'script_name': table_info[table_name]['script_name'],
                    'execution_mode': table_info[table_name]['execution_mode']
                })
        
        # 将结果保存到XCom
        ti = context['ti']
        ti.xcom_push(key='model_tables', value=model_tables)
        ti.xcom_push(key='dependency_graph', value={k: list(v) for k, v in dependency_graph.items()})
        
        return model_tables
    
    # 创建分析任务  
    analyze_task = PythonOperator(
        task_id='analyze_dependencies',
        python_callable=analyze_dependencies,
        provide_context=True,
        dag=dag,
    )
    
    # 创建构建任务链的任务
    def create_dynamic_task_chain(**context):
        """创建动态任务链"""
        ti = context['ti']
        model_tables = ti.xcom_pull(task_ids='analyze_dependencies', key='model_tables')
        dependency_data = ti.xcom_pull(task_ids='analyze_dependencies', key='dependency_graph')
        
        if not model_tables:
            logger.warning("没有找到需要处理的表")
            return 'end'
            
        # 重建依赖图为networkx格式
        dependency_graph = nx.DiGraph()
        for target, upstreams in dependency_data.items():
            for upstream in upstreams:
                dependency_graph.add_edge(upstream, target)
        
        # 提取表名列表
        table_names = [t['table_name'] for t in model_tables]
        
        # 创建任务字典
        task_dict = {}
        for table_info in model_tables:
            table_name = table_info['table_name']
            task_id = f"process_{table_name}"
            
            # 创建处理任务
            task = PythonOperator(
                task_id=task_id,
                python_callable=run_model_script,
                op_kwargs={
                    'table_name': table_name,
                    'execution_mode': table_info['execution_mode']
                },
                dag=dag,
            )
            
            task_dict[table_name] = task
        
        # 设置任务间依赖关系
        for i, table_name in enumerate(table_names):
            if i > 0:
                prev_table = table_names[i-1]
                task_dict[prev_table] >> task_dict[table_name]
                logger.info(f"设置任务依赖: {prev_table} >> {table_name}")
        
        # 连接第一个和最后一个任务
        if table_names:
            first_task = task_dict[table_names[0]]
            last_task = task_dict[table_names[-1]]
            
            analyze_task >> first_task
            last_task >> end_task
        else:
            # 如果没有表需要处理，直接连接到结束任务
            analyze_task >> end_task
        
        return 'end'
    
    # 创建构建任务链的任务
    build_task = PythonOperator(
        task_id='build_task_chain',
        python_callable=create_dynamic_task_chain,
        provide_context=True,
        dag=dag,
    )
    
    # 设置任务链
    start_task >> analyze_task >> build_task 