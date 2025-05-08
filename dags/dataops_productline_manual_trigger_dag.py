# dataops_productline_manual_trigger_dag.py
"""
手动触发数据产品线脚本执行DAG

功能：
- 支持灵活的参数组合:
  - 可以只提供脚本名称，自动查找目标表
  - 可以只提供目标表，智能处理对应的脚本组合
  - 可以同时提供脚本名称和目标表
- 支持三种依赖级别：
  - 'self'：只执行当前脚本，不处理上游依赖
  - 'resource'：到Resource层
  - 'source'：到Source层
- 支持三种脚本类型：
  - 'python_script'：执行物理Python脚本文件
  - 'python'：从data_transform_scripts表获取Python脚本内容并执行
  - 'sql'：从data_transform_scripts表获取SQL脚本内容并执行
- 支持 Logical date 参数：
  - 可以在Airflow UI中选择特定的Logical date
  - 选择的日期将被转换为执行日期并传递给脚本

参数：
- script_name：[可选] 目标脚本名称
- target_table：[可选] 目标表名
- dependency_level：依赖级别

使用示例：
1. 只提供脚本名称:
{
    "conf": {
        "script_name": "book_sale_amt_monthly_process.py",
        "dependency_level": "resource"
    }
}

2. 只提供目标表:
{
    "conf": {
        "target_table": "book_sale_amt_monthly",
        "dependency_level": "resource"
    }
}

3. 同时提供脚本名称和目标表:
{
    "conf": {
        "script_name": "book_sale_amt_monthly_process.py",
        "target_table": "book_sale_amt_monthly",
        "dependency_level": "resource"
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
import pendulum
import pytz
from utils import get_pg_conn, get_cn_exec_date, check_script_exists, get_complete_script_info, get_table_label
from airflow.exceptions import AirflowException

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

def get_dag_params(**context):
    """获取DAG运行参数"""
    #params = context.get('params', {})
    dag_run = context.get('dag_run')
    params = dag_run.conf if dag_run and dag_run.conf else {}
    
    script_name = params.get('script_name', '')
    target_table = params.get('target_table', '')
    
    # 记录原始参数信息
    logger.info(f"接收到的原始参数: {params}")
    
    # 获取依赖级别参数
    dependency_level = params.get('dependency_level', 'self')
    logger.info(f"获取的依赖级别值: {dependency_level}")

    # 获取 logical_date
    dag_run = context.get('dag_run')
    logical_date = dag_run.logical_date if dag_run else datetime.now()
    exec_date, local_logical_date = get_cn_exec_date(logical_date)
    logger.info(f"【时间参数】get_dag_params: exec_date={exec_date}, logical_date={logical_date}, local_logical_date={local_logical_date}")

    # 验证参数组合
    if not script_name and not target_table:
        logger.error("必须至少提供script_name或target_table参数之一")
        raise ValueError("必须至少提供script_name或target_table参数之一")
    
    # 验证dependency_level参数
    if dependency_level not in ['self', 'resource', 'source']:
        logger.warning(f"无效的依赖级别参数: {dependency_level}，使用默认值'self'")
        dependency_level = 'self'
    
    logger.info(f"最终使用的参数 - 脚本名称: {script_name}, 目标表: {target_table}, 依赖级别: {dependency_level}, 执行日期: {exec_date}")
    return script_name, target_table, dependency_level, exec_date, logical_date

def find_target_table_for_script(script_name):
    """
    根据脚本名称查找对应的目标表
    
    参数:
        script_name (str): 脚本名称
        
    返回:
        str: 目标表名，如果找不到则返回None
    """
    driver = GraphDatabase.driver(
        NEO4J_CONFIG['uri'], 
        auth=(NEO4J_CONFIG['user'], NEO4J_CONFIG['password'])
    )
    
    # 首先查找DataModel表中的脚本关系
    query_datamodel = """
        MATCH (source)-[rel:DERIVED_FROM]->(target)
        WHERE rel.script_name = $script_name
        RETURN source.en_name AS target_table LIMIT 1
    """
    
    # 如果在DataModel中找不到，尝试查找DataResource表
    query_dataresource = """
        MATCH (source)-[rel:ORIGINATES_FROM]->(target)
        WHERE rel.script_name = $script_name
        RETURN source.en_name AS target_table LIMIT 1
    """
    
    try:
        with driver.session() as session:
            # 先查找DataModel
            result = session.run(query_datamodel, script_name=script_name)
            record = result.single()
            
            if record and record.get("target_table"):
                return record.get("target_table")
            
            # 如果在DataModel中找不到，尝试DataResource
            result = session.run(query_dataresource, script_name=script_name)
            record = result.single()
            
            if record and record.get("target_table"):
                return record.get("target_table")
            
            logger.warning(f"未找到脚本 {script_name} 对应的目标表")
            return None
    except Exception as e:
        logger.error(f"查找脚本 {script_name} 对应的目标表时出错: {str(e)}")
        return None
    finally:
        driver.close()

def find_scripts_for_table(table_name):
    """
    根据表名查找对应的脚本信息
    
    参数:
        table_name (str): 表名
        
    返回:
        list: 脚本信息列表，如果找不到则返回空列表
    """
    driver = GraphDatabase.driver(
        NEO4J_CONFIG['uri'], 
        auth=(NEO4J_CONFIG['user'], NEO4J_CONFIG['password'])
    )
    
    table_label = get_table_label(table_name)
    scripts = []
    
    try:
        with driver.session() as session:
            # 先检查是否为structure类型的DataResource
            if table_label == "DataResource":
                query_type = """
                    MATCH (n:DataResource {en_name: $table_name})
                    RETURN n.type AS type
                """
                result = session.run(query_type, table_name=table_name)
                record = result.single()
                
                if record and record.get("type") == "structure":
                    logger.info(f"表 {table_name} 是structure类型的DataResource，使用默认脚本'load_file.py'")
                    scripts.append({
                        "script_name": "load_file.py",
                        "script_type": "python_script"
                    })
                    return scripts
                
            if table_label == "DataModel":
                # 查询DataModel表中的脚本关系
                query = """
                    MATCH (target:DataModel {en_name: $table_name})-[rel:DERIVED_FROM]->(source)
                    RETURN rel.script_name AS script_name, rel.script_type AS script_type
                """
            elif table_label == "DataResource":
                # 查询DataResource表中的脚本关系
                query = """
                    MATCH (target:DataResource {en_name: $table_name})-[rel:ORIGINATES_FROM]->(source)
                    RETURN rel.script_name AS script_name, rel.script_type AS script_type
                """
            else:
                logger.warning(f"表 {table_name} 类型未知或不受支持，无法查找脚本")
                return scripts
            
            result = session.run(query, table_name=table_name)
            
            for record in result:
                script_name = record.get("script_name")
                script_type = record.get("script_type", "python_script")
                
                if script_name:
                    scripts.append({
                        "script_name": script_name,
                        "script_type": script_type
                    })
                    
        # 如果没有找到脚本，则尝试使用表名作为脚本名称的一部分查找
        if not scripts:
            logger.info(f"未在关系中找到表 {table_name} 对应的脚本，尝试使用表名查找")
            
            default_script_name = f"{table_name}_process.py"
            script_path = os.path.join(SCRIPTS_BASE_PATH, default_script_name)
            
            if os.path.exists(script_path):
                logger.info(f"找到默认脚本: {default_script_name}")
                scripts.append({
                    "script_name": default_script_name,
                    "script_type": "python_script"
                })
    except Exception as e:
        logger.error(f"查找表 {table_name} 的脚本信息时出错: {str(e)}")
        logger.error(traceback.format_exc())
    finally:
        driver.close()
        
    return scripts

def get_upstream_script_dependencies(script_info, dependency_level='resource'):
    """
    获取脚本的上游依赖

    参数:
        script_info: 脚本信息
        dependency_level: 依赖级别
            - self: 只考虑当前脚本
            - resource: 到Resource层
            - source: 到Source层
    
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
                    # 获取表的类型
                    table_label = get_table_label(source_table)
                    
                    # 检查依赖级别和表类型
                    # 如果是resource级别且表类型是DataSource，则不继续处理
                    if dependency_level == 'resource' and table_label == 'DataSource':
                        logger.info(f"依赖级别为'resource'，跳过DataSource类型表: {source_table}")
                        continue
                    
                    # 如果是DataResource表，检查是否为structure类型
                    if table_label == 'DataResource':
                        query_structure = """
                            MATCH (n:DataResource {en_name: $table_name})
                            RETURN n.type AS type
                        """
                        result = session.run(query_structure, table_name=source_table)
                        record = result.single()
                        
                        if record and record.get("type") == "structure":
                            logger.info(f"上游表 {source_table} 是structure类型的DataResource，使用默认脚本'load_file.py'")
                            
                            # 获取structure类型表的基本信息
                            structure_query = """
                                MATCH (n:DataResource {en_name: $table_name})
                                RETURN n.type AS type, n.storage_location AS storage_location, 
                                      n.schedule_frequency AS schedule_frequency
                            """
                            structure_result = session.run(structure_query, table_name=source_table)
                            structure_record = structure_result.single()
                            
                            # 构建structure类型表的脚本信息
                            upstream_script_name = "load_file.py"
                            upstream_target_table = source_table
                            upstream_id = f"load_file_py_{upstream_target_table}"
                            
                            # 如果还没有处理过这个脚本
                            if upstream_id not in visited:
                                # 创建脚本信息
                                upstream_info = get_complete_script_info(upstream_script_name, upstream_target_table)
                                
                                # 添加到图中
                                G.add_node(upstream_id, **upstream_info)
                                G.add_edge(current_id, upstream_id)
                                
                                # 记录处理过的脚本
                                processed_scripts[upstream_id] = upstream_info
                                
                                # 添加到已访问集合
                                visited.add(upstream_id)
                                queue.append(upstream_info)
                            
                            # 处理完structure类型表后继续下一个源表
                            continue
                    
                    # 根据表的类型，查询不同的关系
                    if table_label == 'DataModel':
                        query = """
                            MATCH (target:DataModel {en_name: $table_name})-[rel:DERIVED_FROM]->(source)
                            RETURN source.en_name AS source_table, rel.script_name AS script_name, rel.script_type AS script_type
                            LIMIT 1
                        """
                    elif table_label == 'DataResource':
                        query = """
                            MATCH (target:DataResource {en_name: $table_name})-[rel:ORIGINATES_FROM]->(source)
                            RETURN source.en_name AS source_table, rel.script_name AS script_name, rel.script_type AS script_type
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
                        source_table_from_db = record.get("source_table")
                        
                        # 记录源表信息
                        if source_table_from_db:
                            logger.info(f"表 {source_table} 的上游源表: {source_table_from_db}")
                        
                        # 构建上游脚本ID
                        upstream_id = f"{upstream_script_name.replace('.', '_')}_{upstream_target_table}"
                        
                        if upstream_id not in visited:
                            # 获取完整的上游脚本信息
                            upstream_info = get_complete_script_info(upstream_script_name, upstream_target_table)
                            
                            # 添加到图中
                            G.add_node(upstream_id, **upstream_info)
                            G.add_edge(current_id, upstream_id)
                            
                            # 记录处理过的脚本
                            processed_scripts[upstream_id] = upstream_info
                            
                            # 继续递归处理，除非遇到限制条件
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
    update_mode = script_info.get('update_mode', 'append')
    target_table_label = script_info.get('target_table_label')
    source_tables = script_info.get('source_tables', [])
    schedule_frequency = script_info.get('schedule_frequency', 'daily')
    # 使用传入的执行日期，如果不存在则使用当前日期
    exec_date = script_info.get('exec_date', datetime.now().strftime('%Y-%m-%d'))
    is_manual_dag_trigger = script_info.get('is_manual_dag_trigger', False)  # 获取手动DAG触发标识
    
    # 记录开始执行
    logger.info(f"===== 开始执行物理Python脚本文件: {script_name} =====")
    logger.info(f"目标表: {target_table}")
    logger.info(f"更新模式: {update_mode}")
    logger.info(f"表标签: {target_table_label}")
    logger.info(f"源表: {source_tables}")
    logger.info(f"频率: {schedule_frequency}")
    logger.info(f"执行日期: {exec_date}")
    logger.info(f"是否手工DAG触发: {is_manual_dag_trigger}")
    
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
                "update_mode": update_mode,
                "schedule_frequency": schedule_frequency,
                "exec_date": exec_date,  # 使用传入的执行日期而不是当前日期
                "is_manual_dag_trigger": is_manual_dag_trigger  # 添加手动DAG触发标识
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
    update_mode = script_info.get('update_mode', 'append')
    target_table_label = script_info.get('target_table_label')
    schedule_frequency = script_info.get('schedule_frequency', 'daily')
    is_manual_dag_trigger = script_info.get('is_manual_dag_trigger', False)
    # 使用传入的执行日期，如果不存在则使用当前日期
    exec_date = script_info.get('exec_date', datetime.now().strftime('%Y-%m-%d'))
    
    # 记录开始执行
    logger.info(f"===== 开始执行SQL脚本: {script_name} =====")
    logger.info(f"目标表: {target_table}")
    logger.info(f"更新模式: {update_mode}")
    logger.info(f"表标签: {target_table_label}")
    logger.info(f"频率: {schedule_frequency}")
    logger.info(f"执行日期: {exec_date}")
    logger.info(f"是否手工DAG触发: {is_manual_dag_trigger}")
    
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
                "exec_date": exec_date,  # 使用传入的执行日期而不是当前日期
                "schedule_frequency": schedule_frequency,
                "target_table_label": target_table_label,
                "update_mode": update_mode,
                "is_manual_dag_trigger": is_manual_dag_trigger
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
    update_mode = script_info.get('update_mode', 'append')
    target_table_label = script_info.get('target_table_label')
    schedule_frequency = script_info.get('schedule_frequency', 'daily')
    is_manual_dag_trigger = script_info.get('is_manual_dag_trigger', False)
    # 使用传入的执行日期，如果不存在则使用当前日期
    exec_date = script_info.get('exec_date', datetime.now().strftime('%Y-%m-%d'))
    
    # 记录开始执行
    logger.info(f"===== 开始执行Python脚本(data_transform_scripts): {script_name} =====")
    logger.info(f"目标表: {target_table}")
    logger.info(f"更新模式: {update_mode}")
    logger.info(f"表标签: {target_table_label}")
    logger.info(f"频率: {schedule_frequency}")
    logger.info(f"执行日期: {exec_date}")
    logger.info(f"是否手工DAG触发: {is_manual_dag_trigger}")
    
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
                "exec_date": exec_date,  # 使用传入的执行日期而不是当前日期
                "schedule_frequency": schedule_frequency,
                "target_table_label": target_table_label,
                "update_mode": update_mode,
                "is_manual_dag_trigger": is_manual_dag_trigger
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
    script_type = script_info.get('script_type').lower()
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

def prepare_script_info(script_name=None, target_table=None, dependency_level=None):
    """
    准备脚本信息，根据输入的参数组合智能处理
    
    参数:
        script_name: [可选] 脚本名称
        target_table: [可选] 目标表名
        dependency_level: 依赖级别
        
    返回:
        list: 脚本信息列表
    """
    all_script_infos = []
    
    # 如果script_name和target_table都为空或None
    if not script_name and not target_table:
        logger.error("script_name和target_table参数都为空，无法确定要执行的脚本")
        raise ValueError("必须至少提供script_name或target_table参数之一")
    
    # 情况1: 同时提供脚本名称和目标表名
    if script_name and target_table:
        logger.info(f"情况1: 同时提供了脚本名称和目标表名")
        script_info = get_complete_script_info(script_name, target_table)
        if script_info:
            all_script_infos.append(script_info)
    
    # 情况2: 只提供脚本名称，自动查找目标表
    elif script_name and not target_table:
        logger.info(f"情况2: 只提供了脚本名称，自动查找目标表")
        target_table = find_target_table_for_script(script_name)
        if target_table:
            logger.info(f"找到脚本 {script_name} 对应的目标表: {target_table}")
            script_info = get_complete_script_info(script_name, target_table)
            if script_info:
                all_script_infos.append(script_info)
        else:
            logger.error(f"未找到脚本 {script_name} 对应的目标表")
    
    # 情况3: 只提供目标表名，查找并处理相关的脚本
    elif not script_name and target_table:
        logger.info(f"情况3: 只提供了目标表名，查找相关的脚本")
        
        # 首先检查是否为structure类型的DataResource表
        table_label = get_table_label(target_table)
        
        if table_label == 'DataResource':
            driver = GraphDatabase.driver(
                NEO4J_CONFIG['uri'], 
                auth=(NEO4J_CONFIG['user'], NEO4J_CONFIG['password'])
            )
            
            try:
                with driver.session() as session:
                    query = """
                        MATCH (n:DataResource {en_name: $table_name})
                        RETURN n.type AS type
                    """
                    result = session.run(query, table_name=target_table)
                    record = result.single()
                    
                    if record and record.get("type") == "structure":
                        logger.info(f"表 {target_table} 是structure类型的DataResource，使用默认脚本'load_file.py'")
                        script_info = get_complete_script_info('load_file.py', target_table)
                        if script_info:
                            all_script_infos.append(script_info)
                            return all_script_infos
            finally:
                driver.close()
        
        # 如果不是structure类型，使用原有逻辑查找脚本
        scripts = find_scripts_for_table(target_table)
        
        if not scripts:
            logger.warning(f"未找到表 {target_table} 关联的脚本")
            
            # 如果是DataResource的表，再次检查是否为structure类型
            if table_label == 'DataResource':
                logger.info(f"尝试使用默认脚本'load_file.py'处理表 {target_table}")
                script_info = get_complete_script_info('load_file.py', target_table)
                if script_info:
                    all_script_infos.append(script_info)
            
            return all_script_infos
        
        # 查看是否所有脚本名称都相同
        script_names = set(script['script_name'] for script in scripts)
        
        if len(script_names) == 1:
            # 如果只有一个不同的脚本名称，处理为单个脚本
            single_script_name = next(iter(script_names))
            logger.info(f"表 {target_table} 只关联了一个脚本: {single_script_name}")
            script_info = get_complete_script_info(single_script_name, target_table)
            if script_info:
                all_script_infos.append(script_info)
        else:
            # 如果有多个不同的脚本名称，分别处理每个脚本
            logger.info(f"表 {target_table} 关联了多个不同脚本: {script_names}")
            for script in scripts:
                script_name = script['script_name']
                script_info = get_complete_script_info(script_name, target_table)
                if script_info:
                    all_script_infos.append(script_info)
    
    return all_script_infos

def prepare_dependency_chain(**context):
    """
    准备依赖链并保存到XCom
    """
    # 获取脚本和表名参数
    script_name, target_table, dependency_level, exec_date, logical_date = get_dag_params(**context)
    
    # 记录依赖级别信息
    logger.info(f"依赖级别说明:")
    logger.info(f"- self: 只执行当前脚本，不处理上游依赖")
    logger.info(f"- resource: 到Resource层")
    logger.info(f"- source: 到Source层")
    logger.info(f"当前依赖级别: {dependency_level}")
    logger.info(f"执行日期: {exec_date}")
    
    # 准备脚本信息
    script_infos = prepare_script_info(script_name, target_table, dependency_level)
    
    if not script_infos:
        logger.error(f"未能获取有效的脚本信息")
        return False
    
    # 获取完整的依赖链
    all_dependencies = []
    
    for script_info in script_infos:
        # 验证脚本信息
        if not script_info.get('target_table_label'):
            logger.warning(f"未能确定表 {script_info.get('target_table')} 的类型")
            continue
        
        # 获取脚本依赖链
        dependency_chain = get_upstream_script_dependencies(script_info, dependency_level)
        
        if dependency_chain:
            all_dependencies.extend(dependency_chain)
        else:
            logger.warning(f"没有找到脚本 {script_info.get('script_name')} 的依赖链")
    
    # 去重
    unique_dependencies = []
    seen_script_ids = set()
    
    for dep in all_dependencies:
        script_id = dep.get('script_id')
        if script_id and script_id not in seen_script_ids:
            seen_script_ids.add(script_id)
            unique_dependencies.append(dep)
    
    if not unique_dependencies:
        logger.error("没有找到任何有效的依赖链")
        return False
    
    # 保存依赖链和执行日期到XCom
    ti = context['ti']
    ti.xcom_push(key='dependency_chain', value=unique_dependencies)
    ti.xcom_push(key='exec_date', value=exec_date)
    ti.xcom_push(key='logical_date', value=logical_date.isoformat() if isinstance(logical_date, datetime) else logical_date)
    
    logger.info(f"成功准备了 {len(unique_dependencies)} 个脚本的依赖链")
    return True

def execute_script_chain(**context):
    """
    执行依赖链中的所有脚本
    """
      
    # 获取依赖链和执行日期
    ti = context['ti']
    dependency_chain = ti.xcom_pull(task_ids='prepare_dependency_chain', key='dependency_chain')
    exec_date = ti.xcom_pull(task_ids='prepare_dependency_chain', key='exec_date')
    logical_date_str = ti.xcom_pull(task_ids='prepare_dependency_chain', key='logical_date')
    
    # 转换logical_date为datetime对象（如果是字符串）
    if isinstance(logical_date_str, str):
        try:
            logical_date = pendulum.parse(logical_date_str)
        except:
            logical_date = datetime.now()
    else:
        logical_date = datetime.now()
    
    logger.info(f"【时间参数】execute_script_chain: exec_date={exec_date}, logical_date={logical_date}")
    
    if not dependency_chain:
        logger.error("没有找到依赖链，无法执行脚本")
        raise AirflowException("没有找到依赖链，无法执行脚本")
    
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
        script_type = script_info.get('script_type')
        
        logger.info(f"===== 执行脚本 {idx}/{len(dependency_chain)}: {script_name} -> {target_table} (类型: {script_type}) =====")
        
        # 根据脚本类型选择执行函数
        executor = choose_executor(script_info)
        
        # 将执行日期添加到脚本信息中
        script_info['exec_date'] = exec_date
        script_info['logical_date'] = logical_date

        # 这个参数用来标识脚本是不是被手工DAG触发的，它会影响脚本执行之前的幂等性操作。
        script_info['is_manual_dag_trigger'] = True
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
        
        # 如果任何一个脚本执行失败，标记整体失败并抛出异常
        if not success:
            all_success = False
            error_message = f"脚本 {script_name} 执行失败，中断执行链"
            logger.error(error_message)
            # 保存执行结果后抛出异常
            ti.xcom_push(key='execution_results', value=results)
            raise AirflowException(error_message)
    
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
    exec_date = ti.xcom_pull(task_ids='prepare_dependency_chain', key='exec_date')
    
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
        script_type = result.get('script_type')
        if script_type not in script_types:
            script_types[script_type] = 0
        script_types[script_type] += 1
    
    # 构建报告
    report = []
    report.append("\n========== 脚本执行报告 ==========")
    report.append(f"执行日期: {exec_date}")
    report.append(f"报告生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
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
        script_type = result.get('script_type')
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
    description='script_name和target_table可以二选一，支持三种依赖级别：self(仅当前表或脚本)、resource(到Resource层)、source(到Source层)',
    schedule_interval=None,  # 设置为None表示只能手动触发
    catchup=False,
    is_paused_upon_creation=False,
    params={
        "script_name": "",
        "target_table": "",
        "dependency_level": "self"
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
        trigger_rule='all_success'  # 修改为仅在上游任务成功时执行
    )
    
    # 任务3: 生成执行报告
    report_task = PythonOperator(
        task_id='generate_execution_report',
        python_callable=generate_execution_report,
        provide_context=True,
        trigger_rule='all_success'  # 修改为仅在上游任务成功时执行
    )
    
    # 任务4: 完成标记
    completed_task = EmptyOperator(
        task_id='execution_completed',
        trigger_rule='all_success'  # 修改为仅在上游任务成功时执行
    )
    
    # 设置任务依赖关系
    prepare_task >> execute_task >> report_task >> completed_task