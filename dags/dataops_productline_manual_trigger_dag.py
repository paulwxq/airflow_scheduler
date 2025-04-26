# dataops_productline_manual_trigger_dag.py
"""
手动触发数据产品线脚本执行DAG

功能：
- 根据指定的脚本名称和目标表名，构建并执行其上游依赖链
- 支持三种依赖级别：
  - 'self'：只执行当前脚本，不处理上游依赖
  - 'dependency'：依据脚本之间的直接依赖关系构建执行链
  - 'full'：构建完整依赖链，包括所有间接依赖
- 支持三种脚本类型：
  - 'python_script'：执行物理Python脚本文件
  - 'python'：从data_transform_scripts表获取Python脚本内容并执行
  - 'sql'：从data_transform_scripts表获取SQL脚本内容并执行

参数：
- script_name：目标脚本名称
- target_table：目标表名
- dependency_level：依赖级别

使用示例：
{
    "conf": {
        "script_name": "book_sale_amt_monthly_process.py",
        "target_table": "book_sale_amt_monthly",
        "dependency_level": "dependency"
    }
}
"""
from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import logging
import importlib.util
import os
from pathlib import Path
from neo4j import GraphDatabase
import psycopg2
import networkx as nx
import json
from config import NEO4J_CONFIG, SCRIPTS_BASE_PATH, PG_CONFIG
import traceback

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
    'retry_delay': timedelta(minutes=1),
}

def get_pg_conn():
    """获取PostgreSQL连接"""
    return psycopg2.connect(**PG_CONFIG)

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

def get_dag_params(**context):
    """获取DAG运行参数"""
    params = context.get('params', {})
    script_name = params.get('script_name')
    target_table = params.get('target_table')
    
    # 记录原始参数信息
    logger.info(f"接收到的原始参数: {params}")
    
    # 获取依赖级别参数
    dependency_level = params.get('dependency_level')
    logger.info(f"获取的依赖级别值: {dependency_level}")

    if not script_name:
        raise ValueError("必须提供script_name参数")
        
    if not target_table:
        raise ValueError("必须提供target_table参数")
    
    # 验证dependency_level参数
    if dependency_level not in ['self', 'dependency', 'full']:
        logger.warning(f"无效的依赖级别参数: {dependency_level}，使用默认值'dependency'")
        dependency_level = 'dependency'
    
    logger.info(f"最终使用的参数 - 脚本名称: {script_name}, 目标表: {target_table}, 依赖级别: {dependency_level}")
    return script_name, target_table, dependency_level

def get_table_label(table_name):
    """确定表的标签类型（DataModel or DataResource）"""
    driver = GraphDatabase.driver(
        NEO4J_CONFIG['uri'], 
        auth=(NEO4J_CONFIG['user'], NEO4J_CONFIG['password'])
    )
    query = """
        MATCH (n {en_name: $table_name})
        RETURN labels(n) AS labels
    """
    try:
        with driver.session() as session:
            result = session.run(query, table_name=table_name)
            record = result.single()
            if record and record.get("labels"):
                labels = record.get("labels")
                if "DataModel" in labels:
                    return "DataModel"
                elif "DataResource" in labels:
                    return "DataResource"
                elif "DataSource" in labels:
                    return "DataSource"
            return None
    except Exception as e:
        logger.error(f"获取表 {table_name} 的标签时出错: {str(e)}")
        return None
    finally:
        driver.close()

def get_script_info_from_neo4j(script_name, target_table):
    """
    从Neo4j获取脚本和表的详细信息
    
    参数:
        script_name: 脚本名称
        target_table: 目标表名
        
    返回:
        dict: 脚本和表的详细信息
    """
    driver = GraphDatabase.driver(
        NEO4J_CONFIG['uri'], 
        auth=(NEO4J_CONFIG['user'], NEO4J_CONFIG['password'])
    )
    
    script_info = {
        'script_name': script_name,
        'target_table': target_table,
        'script_id': f"{script_name.replace('.', '_')}_{target_table}",
        'target_table_label': get_table_label(target_table),
        'source_tables': [],
        'script_type': 'python_script'  # 默认类型改为python_script，表示物理脚本文件
    }
    
    # 根据表标签类型查询脚本信息和依赖关系
    try:
        with driver.session() as session:
            if script_info['target_table_label'] == 'DataModel':
                # 查询DataModel的上游依赖
                query = """
                    MATCH (target:DataModel {en_name: $table_name})-[rel:DERIVED_FROM]->(source)
                    RETURN source.en_name AS source_table, labels(source) AS source_labels, 
                           rel.script_name AS script_name, rel.script_type AS script_type
                """
                result = session.run(query, table_name=target_table)
                
                for record in result:
                    source_table = record.get("source_table")
                    source_labels = record.get("source_labels", [])
                    db_script_name = record.get("script_name")
                    script_type = record.get("script_type", "python_script")
                    
                    # 验证脚本名称匹配
                    if db_script_name and db_script_name == script_name:
                        if source_table and source_table not in script_info['source_tables']:
                            script_info['source_tables'].append(source_table)
                        script_info['script_type'] = script_type
            
            elif script_info['target_table_label'] == 'DataResource':
                # 查询DataResource的上游依赖
                query = """
                    MATCH (target:DataResource {en_name: $table_name})-[rel:ORIGINATES_FROM]->(source)
                    RETURN source.en_name AS source_table, labels(source) AS source_labels, 
                           rel.script_name AS script_name, rel.script_type AS script_type
                """
                result = session.run(query, table_name=target_table)
                
                for record in result:
                    source_table = record.get("source_table")
                    source_labels = record.get("source_labels", [])
                    db_script_name = record.get("script_name")
                    script_type = record.get("script_type", "python_script")
                    
                    # 验证脚本名称匹配
                    if db_script_name and db_script_name == script_name:
                        if source_table and source_table not in script_info['source_tables']:
                            script_info['source_tables'].append(source_table)
                        script_info['script_type'] = script_type
            
            # 如果没有找到依赖关系，记录警告
            if not script_info['source_tables']:
                logger.warning(f"未找到脚本 {script_name} 和表 {target_table} 的依赖关系")
                
            # 获取特殊属性（如果是structure类型）
            if script_info['target_table_label'] == 'DataResource':
                query = """
                    MATCH (n:DataResource {en_name: $table_name})
                    RETURN n.type AS target_type, n.storage_location AS storage_location, n.frequency AS frequency
                """
                result = session.run(query, table_name=target_table)
                record = result.single()
                
                if record:
                    target_type = record.get("target_type")
                    storage_location = record.get("storage_location")
                    frequency = record.get("frequency")
                    
                    if target_type:
                        script_info['target_type'] = target_type
                    if storage_location:
                        script_info['storage_location'] = storage_location
                    if frequency:
                        script_info['frequency'] = frequency
    
    except Exception as e:
        logger.error(f"从Neo4j获取脚本 {script_name} 和表 {target_table} 的信息时出错: {str(e)}")
        logger.error(traceback.format_exc())
    finally:
        driver.close()
    
    # 获取表的执行模式
    script_info['execution_mode'] = get_execution_mode(target_table)
    
    logger.info(f"获取到脚本信息: {script_info}")
    return script_info

def get_upstream_script_dependencies(script_info, dependency_level='dependency'):
    """
    获取脚本的上游依赖

    参数:
        script_info: 脚本信息
        dependency_level: 依赖级别
            - self: 只考虑当前脚本
            - dependency: 考虑直接依赖
            - full: 考虑所有间接依赖
    
    返回:
        list: 依赖链脚本列表
    """
    # 如果是self级别，直接返回当前脚本信息
    if dependency_level == 'self':
        logger.info(f"依赖级别为'self'，只包含当前脚本: {script_info['script_name']}")
        return [script_info]
    
    # 创建依赖图
    G = nx.DiGraph()
    
    # 记录所有处理过的脚本
    processed_scripts = {}
    script_id = script_info['script_id']
    
    # 添加起始节点
    G.add_node(script_id, **script_info)
    processed_scripts[script_id] = script_info
    
    # 使用BFS算法遍历依赖树
    queue = [script_info]
    visited = set([script_id])
    
    while queue:
        current = queue.pop(0)
        current_id = current['script_id']
        
        # 如果没有源表依赖，则跳过
        if not current.get('source_tables'):
            logger.info(f"脚本 {current['script_name']} 没有源表依赖")
            continue
        
        # 对每个源表查找对应的脚本
        for source_table in current.get('source_tables', []):
            # 获取源表对应的脚本信息（假设一个表只有一个主要脚本）
            driver = GraphDatabase.driver(
                NEO4J_CONFIG['uri'], 
                auth=(NEO4J_CONFIG['user'], NEO4J_CONFIG['password'])
            )
            
            try:
                with driver.session() as session:
                    # 根据表的类型，查询不同的关系
                    table_label = get_table_label(source_table)
                    
                    if table_label == 'DataModel':
                        query = """
                            MATCH (target:DataModel {en_name: $table_name})<-[rel:DERIVED_FROM]-(source)
                            RETURN source.en_name AS target_table, rel.script_name AS script_name, rel.script_type AS script_type
                            LIMIT 1
                        """
                    elif table_label == 'DataResource':
                        query = """
                            MATCH (target:DataResource {en_name: $table_name})<-[rel:ORIGINATES_FROM]-(source)
                            RETURN source.en_name AS target_table, rel.script_name AS script_name, rel.script_type AS script_type
                            LIMIT 1
                        """
                    else:
                        logger.warning(f"表 {source_table} 类型未知或不受支持: {table_label}")
                        continue
                    
                    result = session.run(query, table_name=source_table)
                    record = result.single()
                    
                    if record and record.get("script_name"):
                        upstream_script_name = record.get("script_name")
                        upstream_target_table = source_table
                        upstream_script_type = record.get("script_type", "python_script")
                        
                        # 构建上游脚本ID
                        upstream_id = f"{upstream_script_name.replace('.', '_')}_{upstream_target_table}"
                        
                        # 如果是full级别，或者是dependency级别且是直接依赖，则处理
                        if dependency_level == 'full' or (dependency_level == 'dependency' and upstream_id not in visited):
                            # 获取完整的上游脚本信息
                            upstream_info = get_script_info_from_neo4j(upstream_script_name, upstream_target_table)
                            
                            # 添加到图中
                            G.add_node(upstream_id, **upstream_info)
                            G.add_edge(current_id, upstream_id)
                            
                            # 记录处理过的脚本
                            processed_scripts[upstream_id] = upstream_info
                            
                            # 只有full级别才继续递归处理
                            if dependency_level == 'full' and upstream_id not in visited:
                                visited.add(upstream_id)
                                queue.append(upstream_info)
                    else:
                        logger.warning(f"未找到表 {source_table} 对应的脚本信息")
            
            except Exception as e:
                logger.error(f"获取表 {source_table} 对应的脚本信息时出错: {str(e)}")
                logger.error(traceback.format_exc())
            finally:
                driver.close()
    
    # 检测循环依赖
    cycles = list(nx.simple_cycles(G))
    if cycles:
        logger.warning(f"检测到循环依赖，将尝试打破循环: {cycles}")
        for cycle in cycles:
            if len(cycle) > 1:
                G.remove_edge(cycle[0], cycle[1])
                logger.info(f"打破循环依赖: 移除 {cycle[0]} -> {cycle[1]} 的依赖")
    
    # 生成拓扑排序
    try:
        # 对于依赖图，我们需要反转边的方向，因为我们记录的是"依赖于"关系，而拓扑排序是按照"先于"关系
        reverse_G = G.reverse()
        execution_order = list(nx.topological_sort(reverse_G))
        
        # 构建最终依赖链
        dependency_chain = []
        for script_id in execution_order:
            script_info = processed_scripts[script_id]
            dependency_chain.append(script_info)
        
        logger.info(f"最终依赖链长度: {len(dependency_chain)}")
        logger.info(f"最终依赖链: {[info['script_name'] for info in dependency_chain]}")
        return dependency_chain
    
    except Exception as e:
        logger.error(f"生成拓扑排序时出错: {str(e)}")
        logger.error(traceback.format_exc())
        
        # 出错时，至少返回当前脚本
        return [script_info]

def check_script_exists(script_name):
    """检查脚本文件是否存在"""
    script_path = os.path.join(SCRIPTS_BASE_PATH, script_name)
    if os.path.exists(script_path):
        logger.info(f"脚本文件存在: {script_path}")
        return True, script_path
    else:
        logger.error(f"脚本文件不存在: {script_path}")
        return False, script_path

def execute_python_script(script_info):
    """
    执行Python脚本文件
    
    参数:
        script_info: 脚本信息字典
        
    返回:
        bool: 执行成功返回True，失败返回False
    """
    script_name = script_info.get('script_name')
    target_table = script_info.get('target_table')
    execution_mode = script_info.get('execution_mode', 'append')
    target_table_label = script_info.get('target_table_label')
    source_tables = script_info.get('source_tables', [])
    frequency = script_info.get('frequency', 'daily')
    
    # 记录开始执行
    logger.info(f"===== 开始执行物理Python脚本文件: {script_name} =====")
    logger.info(f"目标表: {target_table}")
    logger.info(f"执行模式: {execution_mode}")
    logger.info(f"表标签: {target_table_label}")
    logger.info(f"源表: {source_tables}")
    logger.info(f"频率: {frequency}")
    
    # 检查脚本文件是否存在
    exists, script_path = check_script_exists(script_name)
    if not exists:
        return False
    
    try:
        # 创建执行开始时间
        start_time = datetime.now()
        
        # 动态导入模块
        spec = importlib.util.spec_from_file_location("dynamic_module", script_path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        
        # 检查并调用标准入口函数run
        if hasattr(module, "run"):
            logger.info(f"调用脚本 {script_name} 的标准入口函数 run()")
            
            # 构建函数参数
            run_kwargs = {
                "table_name": target_table,
                "execution_mode": execution_mode,
                "frequency": frequency,
                "exec_date": datetime.now().strftime('%Y-%m-%d')
            }
            
            # 如果是structure类型，添加特殊参数
            if target_table_label == 'DataResource' and script_info.get('target_type') == 'structure':
                run_kwargs["target_type"] = script_info.get('target_type')
                run_kwargs["storage_location"] = script_info.get('storage_location')
            
            # 添加源表
            if source_tables:
                run_kwargs["source_tables"] = source_tables
            
            # 执行脚本
            result = module.run(**run_kwargs)
            
            # 记录结束时间
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            # 确保结果是布尔值
            if not isinstance(result, bool):
                result = bool(result)
            
            logger.info(f"脚本 {script_name} 执行完成，结果: {result}, 耗时: {duration:.2f}秒")
            return result
        else:
            logger.error(f"脚本 {script_name} 没有定义标准入口函数 run()")
            return False
            
    except Exception as e:
        logger.error(f"执行脚本 {script_name} 时出错: {str(e)}")
        logger.error(traceback.format_exc())
        return False

def execute_sql(script_info):
    """
    执行SQL脚本（从data_transform_scripts表获取）
    
    参数:
        script_info: 脚本信息字典
        
    返回:
        bool: 执行成功返回True，失败返回False
    """
    script_name = script_info.get('script_name')
    target_table = script_info.get('target_table')
    execution_mode = script_info.get('execution_mode', 'append')
    target_table_label = script_info.get('target_table_label')
    frequency = script_info.get('frequency', 'daily')
    
    # 记录开始执行
    logger.info(f"===== 开始执行SQL脚本: {script_name} =====")
    logger.info(f"目标表: {target_table}")
    logger.info(f"执行模式: {execution_mode}")
    logger.info(f"表标签: {target_table_label}")
    logger.info(f"频率: {frequency}")
    
    try:
        # 记录执行开始时间
        start_time = datetime.now()
        
        # 导入execution_sql模块
        exec_sql_path = os.path.join(SCRIPTS_BASE_PATH, "execution_sql.py")
        if not os.path.exists(exec_sql_path):
            logger.error(f"SQL执行脚本文件不存在: {exec_sql_path}")
            return False
        
        # 动态导入execution_sql模块
        spec = importlib.util.spec_from_file_location("execution_sql", exec_sql_path)
        exec_sql_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(exec_sql_module)
        
        # 检查并调用标准入口函数run
        if hasattr(exec_sql_module, "run"):
            logger.info(f"调用SQL执行脚本的标准入口函数 run()")
            
            # 构建函数参数
            run_kwargs = {
                "script_type": "sql",
                "target_table": target_table,
                "script_name": script_name,
                "exec_date": datetime.now().strftime('%Y-%m-%d'),
                "frequency": frequency,
                "target_table_label": target_table_label,
                "execution_mode": execution_mode
            }
            
            # 如果是structure类型，添加特殊参数
            if target_table_label == 'DataResource' and script_info.get('target_type') == 'structure':
                run_kwargs["target_type"] = script_info.get('target_type')
                run_kwargs["storage_location"] = script_info.get('storage_location')
            
            # 添加源表
            if 'source_tables' in script_info and script_info['source_tables']:
                run_kwargs["source_tables"] = script_info['source_tables']
            
            # 执行脚本
            result = exec_sql_module.run(**run_kwargs)
            
            # 记录结束时间
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            # 确保结果是布尔值
            if not isinstance(result, bool):
                result = bool(result)
            
            logger.info(f"SQL脚本 {script_name} 执行完成，结果: {result}, 耗时: {duration:.2f}秒")
            return result
        else:
            logger.error(f"SQL执行模块没有定义标准入口函数 run()")
            return False
            
    except Exception as e:
        logger.error(f"执行SQL脚本 {script_name} 时出错: {str(e)}")
        logger.error(traceback.format_exc())
        return False

def execute_python(script_info):
    """
    执行Python脚本（从data_transform_scripts表获取）
    
    参数:
        script_info: 脚本信息字典
        
    返回:
        bool: 执行成功返回True，失败返回False
    """
    script_name = script_info.get('script_name')
    target_table = script_info.get('target_table')
    execution_mode = script_info.get('execution_mode', 'append')
    target_table_label = script_info.get('target_table_label')
    frequency = script_info.get('frequency', 'daily')
    
    # 记录开始执行
    logger.info(f"===== 开始执行Python脚本(data_transform_scripts): {script_name} =====")
    logger.info(f"目标表: {target_table}")
    logger.info(f"执行模式: {execution_mode}")
    logger.info(f"表标签: {target_table_label}")
    logger.info(f"频率: {frequency}")
    
    try:
        # 记录执行开始时间
        start_time = datetime.now()
        
        # 导入execution_python模块
        exec_python_path = os.path.join(SCRIPTS_BASE_PATH, "execution_python.py")
        if not os.path.exists(exec_python_path):
            logger.error(f"Python执行脚本文件不存在: {exec_python_path}")
            return False
        
        # 动态导入execution_python模块
        spec = importlib.util.spec_from_file_location("execution_python", exec_python_path)
        exec_python_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(exec_python_module)
        
        # 检查并调用标准入口函数run
        if hasattr(exec_python_module, "run"):
            logger.info(f"调用Python执行脚本的标准入口函数 run()")
            
            # 构建函数参数
            run_kwargs = {
                "script_type": "python",
                "target_table": target_table,
                "script_name": script_name,
                "exec_date": datetime.now().strftime('%Y-%m-%d'),
                "frequency": frequency,
                "target_table_label": target_table_label,
                "execution_mode": execution_mode
            }
            
            # 如果是structure类型，添加特殊参数
            if target_table_label == 'DataResource' and script_info.get('target_type') == 'structure':
                run_kwargs["target_type"] = script_info.get('target_type')
                run_kwargs["storage_location"] = script_info.get('storage_location')
            
            # 添加源表
            if 'source_tables' in script_info and script_info['source_tables']:
                run_kwargs["source_tables"] = script_info['source_tables']
            
            # 执行脚本
            result = exec_python_module.run(**run_kwargs)
            
            # 记录结束时间
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            # 确保结果是布尔值
            if not isinstance(result, bool):
                result = bool(result)
            
            logger.info(f"Python脚本 {script_name} 执行完成，结果: {result}, 耗时: {duration:.2f}秒")
            return result
        else:
            logger.error(f"Python执行模块没有定义标准入口函数 run()")
            return False
            
    except Exception as e:
        logger.error(f"执行Python脚本 {script_name} 时出错: {str(e)}")
        logger.error(traceback.format_exc())
        return False

def choose_executor(script_info):
    """
    根据脚本类型选择合适的执行函数
    
    参数:
        script_info: 脚本信息字典
        
    返回:
        function: 执行函数
    """
    script_type = script_info.get('script_type', 'python_script').lower()
    target_table_label = script_info.get('target_table_label')
    
    # 根据脚本类型和目标表标签选择执行函数
    if script_type == 'sql' and target_table_label == 'DataModel':
        # 使用SQL脚本执行函数
        logger.info(f"脚本 {script_info['script_id']} 是SQL类型且目标表标签为DataModel，使用execute_sql函数执行")
        return execute_sql
    elif script_type == 'python' and target_table_label == 'DataModel':
        # 使用Python脚本执行函数
        logger.info(f"脚本 {script_info['script_id']} 是Python类型且目标表标签为DataModel，使用execute_python函数执行")
        return execute_python
    elif script_type == 'python_script':
        # 使用Python脚本文件执行函数
        logger.info(f"脚本 {script_info['script_id']} 是python_script类型，使用execute_python_script函数执行")
        return execute_python_script
    else:
        # 默认使用Python脚本文件执行函数
        logger.warning(f"未识别的脚本类型 {script_type}，使用默认execute_python_script函数执行")
        return execute_python_script

def prepare_dependency_chain(**context):
    """
    准备依赖链并保存到XCom
    """
    # 获取脚本和表名参数
    script_name, target_table, dependency_level = get_dag_params(**context)
    
    # 记录依赖级别信息
    logger.info(f"依赖级别说明:")
    logger.info(f"- self: 只执行当前脚本，不处理上游依赖")
    logger.info(f"- dependency: 处理直接依赖")
    logger.info(f"- full: 处理所有间接依赖")
    logger.info(f"当前依赖级别: {dependency_level}")
    
    # 获取脚本信息
    script_info = get_script_info_from_neo4j(script_name, target_table)
    
    # 验证脚本信息
    if not script_info.get('target_table_label'):
        logger.warning(f"未能确定表 {target_table} 的类型")
        return False
    
    # 获取脚本依赖链
    dependency_chain = get_upstream_script_dependencies(script_info, dependency_level)
    
    if not dependency_chain:
        logger.warning(f"没有找到脚本 {script_name} 的依赖链")
        return False
    
    # 保存依赖链到XCom
    ti = context['ti']
    ti.xcom_push(key='dependency_chain', value=dependency_chain)
    
    return True

def execute_script_chain(**context):
    """
    执行依赖链中的所有脚本
    """
    # 获取依赖链
    ti = context['ti']
    dependency_chain = ti.xcom_pull(task_ids='prepare_dependency_chain', key='dependency_chain')
    
    if not dependency_chain:
        logger.error("没有找到依赖链，无法执行脚本")
        return False
    
    # 记录依赖链信息
    logger.info(f"准备执行依赖链中的 {len(dependency_chain)} 个脚本")
    for idx, script_info in enumerate(dependency_chain, 1):
        logger.info(f"脚本[{idx}]: {script_info['script_name']} -> {script_info['target_table']} (类型: {script_info['script_type']})")
    
    # 逐个执行脚本
    all_success = True
    results = []
    
    for idx, script_info in enumerate(dependency_chain, 1):
        script_name = script_info['script_name']
        target_table = script_info['target_table']
        script_type = script_info.get('script_type', 'python_script')
        
        logger.info(f"===== 执行脚本 {idx}/{len(dependency_chain)}: {script_name} -> {target_table} (类型: {script_type}) =====")
        
        # 根据脚本类型选择执行函数
        executor = choose_executor(script_info)
        
        # 执行脚本
        success = executor(script_info)
        
        # 记录结果
        result = {
            "script_name": script_name,
            "target_table": target_table,
            "script_type": script_type,
            "success": success
        }
        results.append(result)
        
        # 如果任何一个脚本执行失败，标记整体失败
        if not success:
            all_success = False
            logger.error(f"脚本 {script_name} 执行失败，中断执行链")
            break
    
    # 保存执行结果
    ti.xcom_push(key='execution_results', value=results)
    
    return all_success

def generate_execution_report(**context):
    """
    生成执行报告
    """
    # 获取执行结果
    ti = context['ti']
    results = ti.xcom_pull(task_ids='execute_script_chain', key='execution_results')
    dependency_chain = ti.xcom_pull(task_ids='prepare_dependency_chain', key='dependency_chain')
    
    if not results:
        report = "未找到执行结果，无法生成报告"
        ti.xcom_push(key='execution_report', value=report)
        return report
    
    # 计算执行统计信息
    total = len(results)
    success_count = sum(1 for r in results if r['success'])
    fail_count = total - success_count
    
    # 统计不同类型脚本数量
    script_types = {}
    for result in results:
        script_type = result.get('script_type', 'python_script')
        if script_type not in script_types:
            script_types[script_type] = 0
        script_types[script_type] += 1
    
    # 构建报告
    report = []
    report.append("\n========== 脚本执行报告 ==========")
    report.append(f"执行日期: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report.append(f"总脚本数: {total}")
    report.append(f"成功数: {success_count}")
    report.append(f"失败数: {fail_count}")
    report.append(f"成功率: {success_count / total * 100:.2f}%")
    
    # 添加脚本类型统计
    report.append("\n--- 脚本类型统计 ---")
    for script_type, count in script_types.items():
        report.append(f"{script_type}: {count} 个")
    
    report.append("\n--- 执行详情 ---")
    for idx, result in enumerate(results, 1):
        script_name = result['script_name']
        target_table = result['target_table']
        script_type = result.get('script_type', 'python_script')
        success = result['success']
        status = "✓ 成功" if success else "✗ 失败"
        report.append(f"{idx}. {script_name} -> {target_table} ({script_type}): {status}")
    
    report.append("\n========== 报告结束 ==========")
    
    # 转换为字符串
    report_str = "\n".join(report)
    
    # 保存报告
    ti.xcom_push(key='execution_report', value=report_str)
    
    # 记录到日志
    logger.info(report_str)
    
    return report_str

# 创建DAG
with DAG(
    'dataops_productline_manual_trigger_dag',
    default_args=default_args,
    description='手动触发指定脚本的执行，支持三种依赖级别：self(仅当前脚本)、dependency(直接依赖)、full(所有间接依赖)',
    schedule_interval=None,  # 设置为None表示只能手动触发
    catchup=False,
    is_paused_upon_creation=False,
    params={
        'script_name': '',
        'target_table': '',
        'dependency_level': {
            'type': 'string',
            'enum': ['self', 'dependency', 'full'],
            'default': 'dependency',
            'description': '依赖级别: self-仅当前脚本, dependency-直接依赖, full-所有间接依赖'
        }
    },
) as dag:
    
    # 任务1: 准备依赖阶段
    prepare_task = ShortCircuitOperator(
        task_id='prepare_dependency_chain',
        python_callable=prepare_dependency_chain,
        provide_context=True,
    )
    
    # 任务2: 执行脚本链
    execute_task = PythonOperator(
        task_id='execute_script_chain',
        python_callable=execute_script_chain,
        provide_context=True,
    )
    
    # 任务3: 生成执行报告
    report_task = PythonOperator(
        task_id='generate_execution_report',
        python_callable=generate_execution_report,
        provide_context=True,
        trigger_rule='all_done'  # 无论前面的任务成功或失败，都生成报告
    )
    
    # 任务4: 完成标记
    completed_task = EmptyOperator(
        task_id='execution_completed',
        trigger_rule='all_done'  # 无论前面的任务成功或失败，都标记为完成
    )
    
    # 设置任务依赖关系
    prepare_task >> execute_task >> report_task >> completed_task
        # 使用Python脚