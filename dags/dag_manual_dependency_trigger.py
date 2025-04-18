# dag_manual_dependency_trigger.py
"""
手动触发数据表依赖链执行DAG

功能：
- 根据指定的表名，构建并执行其上游依赖链
- 支持三种依赖级别：
  - 'self'：只执行当前表，不处理上游依赖
  - 'resource'：查找依赖到Resource层，但只执行DataModel层
  - 'source'：查找并执行完整依赖链到Source层

参数：
- table_name：目标表名
- dependency_level：依赖级别

使用示例：
```
{
  "conf": {
    "table_name": "book_sale_amt_2yearly",
    "dependency_level": "resource"
  }
}
```
"""
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
    table_name = params.get('table_name')
    
    # 记录原始参数信息
    logger.info(f"接收到的原始参数: {params}")
    
    # 获取依赖级别参数
    dependency_level = params.get('dependency_level')
    logger.info(f"获取的依赖级别值: {dependency_level}")

    if not table_name:
        raise ValueError("必须提供TABLE_NAME参数")
    
    # 验证dependency_level参数
    if dependency_level not in ['self', 'resource', 'source']:
        logger.warning(f"无效的依赖级别参数: {dependency_level}，使用默认值'resource'")
        dependency_level = 'resource'
    
    logger.info(f"最终使用的参数 - 表名: {table_name}, 依赖级别: {dependency_level}")
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
    # 记录依赖级别
    logger.info(f"构建依赖链 - 起始表: {start_table}, 依赖级别: {dependency_level}")
    
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
        logger.info(f"依赖级别为'self'，只包含起始表: {start_table}")
        script_name = get_script_name_for_model(start_table) if table_type == 'DataModel' else get_script_name_for_resource(start_table)
        execution_mode = get_execution_mode(start_table)
        return [{
            'table_name': start_table,
            'script_name': script_name,
            'table_type': table_type,
            'execution_mode': execution_mode
        }]
    
    # 判断resource级别还是source级别
    need_source = (dependency_level == 'source')
    logger.info(f"是否需要查找到Source层: {need_source}")
    
    # BFS构建依赖图
    visited = set([start_table])
    queue = [start_table]
    
    while queue:
        current = queue.pop(0)
        current_type = G.nodes[current].get('type')
        logger.info(f"处理节点: {current}, 类型: {current_type}")
        
        # 处理当前节点的上游依赖
        if current_type == 'DataModel':
            # 获取DataModel的上游依赖
            upstream_models = get_upstream_models(current)
            for upstream in upstream_models:
                if upstream not in visited:
                    G.add_node(upstream, type='DataModel')
                    visited.add(upstream)
                    queue.append(upstream)
                G.add_edge(current, upstream, type='model_to_model')
            
            # 获取上游DataResource - 对于resource和source级别都需要查找DataResource
            upstream_resources = get_upstream_resources(current)
            for upstream in upstream_resources:
                if upstream not in visited:
                    G.add_node(upstream, type='DataResource')
                    visited.add(upstream)
                    # 只有在source级别时才继续向上查找DataSource
                    if need_source:
                        queue.append(upstream)
                G.add_edge(current, upstream, type='model_to_resource')
        
        # 如果当前节点是DataResource，只有在source级别才查找上游DataSource
        elif current_type == 'DataResource' and need_source:
            data_sources = get_data_sources(current)
            for source in data_sources:
                if source not in visited:
                    G.add_node(source, type='DataSource')
                    visited.add(source)
                G.add_edge(current, source, type='resource_to_source')
    
    # 记录依赖图节点和边信息
    logger.info(f"依赖图节点数: {len(G.nodes)}, 边数: {len(G.edges)}")
    
    # 在resource级别，确保不处理DataSource节点的脚本
    if dependency_level == 'resource':
        # 查找所有DataSource节点
        source_nodes = [node for node, attrs in G.nodes(data=True) if attrs.get('type') == 'DataSource']
        logger.info(f"依赖级别为'resource'，将移除 {len(source_nodes)} 个DataSource节点")
        
        # 移除所有DataSource节点
        for node in source_nodes:
            G.remove_node(node)
        
        # 重新记录依赖图信息
        logger.info(f"清理后依赖图节点数: {len(G.nodes)}, 边数: {len(G.edges)}")
    
    logger.info(f"依赖图节点: {list(G.nodes)}")
    
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
        logger.info(f"计算出的执行顺序: {execution_order}")
        
        # 构建最终依赖链
        dependency_chain = []
        for table_name in execution_order:
            node_type = G.nodes[table_name].get('type')
            
            # 跳过DataSource节点，它们没有脚本需要执行
            if node_type == 'DataSource':
                logger.info(f"跳过DataSource节点: {table_name}")
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
            logger.info(f"添加到依赖链: {table_name}, 类型: {node_type}")
        
        logger.info(f"最终依赖链长度: {len(dependency_chain)}")
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
    """
    准备依赖链并保存到XCom
    
    不同依赖级别的行为：
    - self: 只执行当前表，不查找上游依赖
    - resource: 仅查找数据模型依赖到Resource层，但不执行Resource层的脚本
    - source: 完整查找所有依赖到Source层，并执行所有相关脚本
    """
    # 获取参数
    table_name, dependency_level = get_dag_params(**context)
    
    # 记录依赖级别信息
    logger.info(f"依赖级别说明:")
    logger.info(f"- self: 只执行当前表，不查找上游依赖")
    logger.info(f"- resource: 仅查找数据模型依赖到Resource层，但不执行Resource层的脚本")
    logger.info(f"- source: 完整查找所有依赖到Source层，并执行所有相关脚本")
    logger.info(f"当前依赖级别: {dependency_level}")
    
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
    
    # 保存依赖级别，便于后续任务使用
    ti.xcom_push(key='dependency_level', value=dependency_level)
    
    # 检查是否有各类型的脚本需要执行
    resource_tables = [item for item in dependency_chain if item['table_type'] == 'DataResource']
    model_tables = [item for item in dependency_chain if item['table_type'] == 'DataModel']
    
    has_resource = len(resource_tables) > 0
    has_model = len(model_tables) > 0
    
    # 处理特殊情况：如果是self级别，且起始表是DataResource
    if dependency_level == 'self' and not has_model and has_resource:
        # 确保只有一个DataResource表，而且是起始表
        is_start_resource = any(item['table_name'] == table_name for item in resource_tables)
        logger.info(f"依赖级别为'self'，起始表是DataResource: {is_start_resource}")
        
        # 额外保存标志，标记这是特殊情况
        ti.xcom_push(key='is_start_resource_only', value=is_start_resource)
    
    logger.info(f"是否有DataResource脚本: {has_resource}({len(resource_tables)}个), 是否有DataModel脚本: {has_model}({len(model_tables)}个)")
    
    return True

def process_resources(**context):
    """
    处理所有DataResource层的脚本
    
    依赖级别处理策略：
    - self: 只有当起始表是DataResource类型时才执行
    - resource: 不执行任何DataResource脚本
    - source: 执行所有依赖链中的DataResource脚本
    """
    # 获取任务间共享变量
    ti = context['ti']
    dependency_chain = ti.xcom_pull(task_ids='prepare_dependency_chain', key='dependency_chain')
    
    # 直接从XCom获取依赖级别，避免重复解析
    dependency_level = ti.xcom_pull(task_ids='prepare_dependency_chain', key='dependency_level')
    
    # 记录当前任务的依赖级别
    logger.info(f"process_resources任务 - 当前依赖级别: {dependency_level}")
    
    # 检查特殊标志
    is_start_resource_only = ti.xcom_pull(task_ids='prepare_dependency_chain', key='is_start_resource_only', default=False)
    
    # 依赖级别处理逻辑
    if dependency_level == 'self' and not is_start_resource_only:
        logger.info("依赖级别为'self'且起始表不是DataResource，跳过process_resources任务")
        return True
    elif dependency_level == 'resource':
        logger.info("依赖级别为'resource'，根据设计不执行DataResource表脚本")
        return True
    
    # 获取表名（仅在self级别需要）
    table_name = None
    if dependency_level == 'self':
        params = context.get('params', {})
        table_name = params.get('table_name')
        logger.info(f"依赖级别为'self'，目标表: {table_name}")
    
    # 根据依赖级别过滤要执行的脚本
    if dependency_level == 'self' and is_start_resource_only:
        # 特殊情况：只处理与起始表名匹配的Resource表
        resource_scripts = [item for item in dependency_chain if item['table_type'] == 'DataResource' and item['table_name'] == table_name]
        logger.info(f"依赖级别为'self'且起始表是DataResource，只处理表: {table_name}")
    elif dependency_level == 'source':
        # source级别：处理所有Resource表
        resource_scripts = [item for item in dependency_chain if item['table_type'] == 'DataResource']
        logger.info(f"依赖级别为'source'，处理所有DataResource表")
    else:
        # 其他情况，返回空列表
        resource_scripts = []
    
    if not resource_scripts:
        logger.info("没有找到DataResource类型的表需要处理")
        return True
    
    # 详细记录要执行的脚本信息
    logger.info(f"要执行的DataResource脚本数量: {len(resource_scripts)}")
    for idx, item in enumerate(resource_scripts, 1):
        logger.info(f"Resource脚本[{idx}]: 表={item['table_name']}, 脚本={item['script_name']}, 模式={item['execution_mode']}")
    
    # 执行所有DataResource脚本
    return execute_scripts(resource_scripts)

def process_models(**context):
    """
    处理所有DataModel层的脚本
    
    依赖级别处理策略：
    - self: 只执行起始表（如果是DataModel类型）
    - resource/source: 执行所有依赖链中的DataModel脚本
    """
    # 获取任务间共享变量
    ti = context['ti']
    dependency_chain = ti.xcom_pull(task_ids='prepare_dependency_chain', key='dependency_chain')
    
    # 直接从XCom获取依赖级别，避免重复解析
    dependency_level = ti.xcom_pull(task_ids='prepare_dependency_chain', key='dependency_level')
    
    # 记录当前任务的依赖级别
    logger.info(f"process_models任务 - 当前依赖级别: {dependency_level}")
    
    # 获取表名（在所有级别都需要）
    params = context.get('params', {})
    table_name = params.get('table_name')
    logger.info(f"目标表: {table_name}")
    
    # 如果依赖级别是'self'，只处理起始表
    if dependency_level == 'self':
        logger.info(f"依赖级别为'self'，只处理起始表: {table_name}")
        model_scripts = [item for item in dependency_chain if item['table_name'] == table_name and item['table_type'] == 'DataModel']
    else:
        # 否则处理所有DataModel表
        logger.info(f"依赖级别为'{dependency_level}'，处理所有DataModel表")
        model_scripts = [item for item in dependency_chain if item['table_type'] == 'DataModel']
    
    if not model_scripts:
        logger.info("没有找到DataModel类型的表需要处理")
        return True
    
    # 详细记录要执行的脚本信息
    logger.info(f"要执行的DataModel脚本数量: {len(model_scripts)}")
    for idx, item in enumerate(model_scripts, 1):
        logger.info(f"Model脚本[{idx}]: 表={item['table_name']}, 脚本={item['script_name']}, 模式={item['execution_mode']}")
    
    # 执行所有DataModel脚本
    return execute_scripts(model_scripts)

# 创建DAG
with DAG(
    'dag_manual_dependency_trigger',
    default_args=default_args,
    description='手动触发指定表的依赖链执行，支持三种依赖级别：self(仅本表)、resource(到Resource层但不执行Resource)、source(完整依赖到Source层)',
    schedule_interval=None,  # 设置为None表示只能手动触发
    catchup=False,
    is_paused_upon_creation=True,  # 添加这一行，使DAG创建时不处于暂停状态
    params={
        'table_name': '',
        'dependency_level': {
            'type': 'string',
            'enum': ['self', 'resource', 'source'],
            'default': 'resource',
            'description': '依赖级别: self-仅本表, resource-到Resource层(不执行Resource脚本), source-到Source层'
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