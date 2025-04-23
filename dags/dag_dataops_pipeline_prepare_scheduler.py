# dag_dataops_pipeline_prepare_scheduler.py
from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import logging
import networkx as nx
import json
import os
import re
import glob
from pathlib import Path
import hashlib
import pendulum
from common import (
    get_pg_conn, 
    get_neo4j_driver,
    get_today_date
)
from config import PG_CONFIG, NEO4J_CONFIG

# 创建日志记录器
logger = logging.getLogger(__name__)

def get_enabled_tables():
    """获取所有启用的表"""
    conn = get_pg_conn()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT owner_id, table_name 
            FROM schedule_status 
            WHERE schedule_is_enabled = TRUE
        """)
        result = cursor.fetchall()
        return [row[1] for row in result]  # 只返回表名
    except Exception as e:
        logger.error(f"获取启用表失败: {str(e)}")
        return []
    finally:
        cursor.close()
        conn.close()

def check_table_directly_subscribed(table_name):
    """检查表是否在schedule_status表中直接调度"""
    conn = get_pg_conn()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT schedule_is_enabled
            FROM schedule_status 
            WHERE table_name = %s
        """, (table_name,))
        result = cursor.fetchone()
        return result and result[0] is True
    except Exception as e:
        logger.error(f"检查表订阅状态失败: {str(e)}")
        return False
    finally:
        cursor.close()
        conn.close()


def should_execute_today(table_name, frequency, exec_date):
    """
    判断指定频率的表在给定执行日期是否应该执行
    
    参数:
        table_name (str): 表名，用于日志记录
        frequency (str): 调度频率，如'daily'、'weekly'、'monthly'、'yearly'，为None时默认为'daily'
        exec_date (str): 执行日期，格式为'YYYY-MM-DD'
    
    返回:
        bool: 如果该表应该在执行日期执行，则返回True，否则返回False
    """
    # 将执行日期字符串转换为pendulum日期对象
    try:
        exec_date_obj = pendulum.parse(exec_date)
    except Exception as e:
        logger.error(f"解析执行日期 {exec_date} 出错: {str(e)}，使用当前日期")
        exec_date_obj = pendulum.today()
    
    # 计算下一个日期，用于判断是否是月初、周初等
    next_date = exec_date_obj.add(days=1)
    
    # 如果频率为None或空字符串，默认为daily
    if not frequency:
        logger.info(f"表 {table_name} 未指定调度频率，默认为daily")
        return True
    
    frequency = frequency.lower() if isinstance(frequency, str) else 'daily'
    
    if frequency == 'daily':
        # 日任务每天都执行
        return True
    elif frequency == 'weekly':
        # 周任务只在周日执行（因为exec_date+1是周一时才执行）
        is_sunday = next_date.day_of_week == 1  # 1表示周一
        logger.info(f"表 {table_name} 是weekly任务，exec_date={exec_date}，next_date={next_date.to_date_string()}，是否周日: {is_sunday}")
        return is_sunday
    elif frequency == 'monthly':
        # 月任务只在每月最后一天执行（因为exec_date+1是月初时才执行）
        is_month_end = next_date.day == 1
        logger.info(f"表 {table_name} 是monthly任务，exec_date={exec_date}，next_date={next_date.to_date_string()}，是否月末: {is_month_end}")
        return is_month_end
    elif frequency == 'quarterly':
        # 季度任务只在每季度最后一天执行（因为exec_date+1是季度初时才执行）
        is_quarter_end = next_date.day == 1 and next_date.month in [1, 4, 7, 10]
        logger.info(f"表 {table_name} 是quarterly任务，exec_date={exec_date}，next_date={next_date.to_date_string()}，是否季末: {is_quarter_end}")
        return is_quarter_end
    elif frequency == 'yearly':
        # 年任务只在每年最后一天执行（因为exec_date+1是年初时才执行）
        is_year_end = next_date.day == 1 and next_date.month == 1
        logger.info(f"表 {table_name} 是yearly任务，exec_date={exec_date}，next_date={next_date.to_date_string()}，是否年末: {is_year_end}")
        return is_year_end
    else:
        # 未知频率，默认执行
        logger.warning(f"表 {table_name} 使用未知的调度频率: {frequency}，默认执行")
        return True

def get_table_info_from_neo4j(table_name):
    """从Neo4j获取表的详细信息"""
    driver = get_neo4j_driver()
     # 检查表是否直接订阅
    is_directly_schedule = check_table_directly_subscribed(table_name)

    table_info = {
        'target_table': table_name,
        'is_directly_schedule': is_directly_schedule,  # 初始值设为True，从schedule_status表获取
    }
    
    try:
        with driver.session() as session:
            # 查询表标签和状态
            query_table = """
                MATCH (t {en_name: $table_name})
                RETURN labels(t) AS labels, t.status AS status, t.frequency AS frequency,
                       t.type AS type, t.storage_location AS storage_location
            """
            result = session.run(query_table, table_name=table_name)
            record = result.single()
            
            if record:
                labels = record.get("labels", [])
                table_info['target_table_label'] = [label for label in labels if label in ["DataResource", "DataModel", "DataSource"]][0] if labels else None
                table_info['target_table_status'] = record.get("status", True)  # 默认为True
                # table_info['default_update_frequency'] = record.get("frequency")
                table_info['frequency'] = record.get("frequency")
                table_info['target_type'] = record.get("type")  # 获取type属性
                table_info['storage_location'] = record.get("storage_location")  # 获取storage_location属性
                
                # 根据标签类型查询关系和脚本信息
                if "DataResource" in labels:
                    # 检查是否为structure类型
                    if table_info.get('target_type') == "structure":
                        # 对于structure类型，设置默认值，不查询关系
                        table_info['source_tables'] = []  # 使用空数组表示无源表
                        table_info['script_name'] = "load_file.py"
                        table_info['script_type'] = "python"
                        
                        # csv类型的DataResource没有上游，使用默认的append模式
                        table_info['script_exec_mode'] = "append"
                        logger.info(f"表 {table_name} 为structure类型，使用默认执行模式: append")

                        return table_info
                    else:
                        query_rel = """
                            MATCH (target {en_name: $table_name})-[rel:ORIGINATES_FROM]->(source)
                            WITH source, rel, 
                                 CASE WHEN rel.script_name IS NULL THEN target.en_name + '_script.py' ELSE rel.script_name END AS script_name,
                                 CASE WHEN rel.script_type IS NULL THEN 'python' ELSE rel.script_type END AS script_type
                            RETURN source.en_name AS source_table, script_name AS script_name,
                                   script_type AS script_type, 'append' AS script_exec_mode
                        """
                elif "DataModel" in labels:
                    query_rel = """
                        MATCH (target {en_name: $table_name})-[rel:DERIVED_FROM]->(source)
                        WITH source, rel, 
                             CASE WHEN rel.script_name IS NULL THEN target.en_name + '_script.py' ELSE rel.script_name END AS script_name,
                             CASE WHEN rel.script_type IS NULL THEN 'python' ELSE rel.script_type END AS script_type
                        RETURN source.en_name AS source_table, script_name AS script_name,
                               script_type AS script_type, 'append' AS script_exec_mode
                    """
                else:
                    logger.warning(f"表 {table_name} 不是DataResource或DataModel类型")
                    return table_info
                
                # 收集所有关系记录
                result = session.run(query_rel, table_name=table_name)
                # 检查result对象是否有collect方法，否则使用data方法或list直接转换
                try:
                    if hasattr(result, 'collect'):
                        records = result.collect()  # 使用collect()获取所有记录
                    else:
                        # 尝试使用其他方法获取记录
                        logger.info(f"表 {table_name} 的查询结果不支持collect方法，尝试使用其他方法")
                        try:
                            records = list(result)  # 直接转换为列表
                        except Exception as e1:
                            logger.warning(f"尝试列表转换失败: {str(e1)}，尝试使用data方法")
                            try:
                                records = result.data()  # 使用data()方法
                            except Exception as e2:
                                logger.warning(f"所有方法都失败，使用空列表: {str(e2)}")
                                records = []
                except Exception as e:
                    logger.warning(f"获取查询结果时出错: {str(e)}，使用空列表")
                    records = []
                
                # 记录查询到的原始记录
                logger.info(f"表 {table_name} 查询到 {len(records)} 条关系记录")
                for idx, rec in enumerate(records):
                    logger.info(f"关系记录[{idx}]: source_table={rec.get('source_table')}, script_name={rec.get('script_name')}, " 
                                f"script_type={rec.get('script_type')}, script_exec_mode={rec.get('script_exec_mode')}")
                
                if records:
                    # 按脚本名称分组源表
                    scripts_info = {}
                    for record in records:
                        script_name = record.get("script_name")
                        source_table = record.get("source_table")
                        script_type = record.get("script_type", "python")
                        script_exec_mode = record.get("script_exec_mode", "append")
                        
                        logger.info(f"处理记录: source_table={source_table}, script_name={script_name}")
                        
                        # 如果script_name为空，生成默认的脚本名
                        if not script_name:
                            script_name = f"{table_name}_process.py"
                            logger.warning(f"表 {table_name} 的关系中没有script_name属性，使用默认值: {script_name}")
                            
                        if script_name not in scripts_info:
                            scripts_info[script_name] = {
                                "sources": [],
                                "script_type": script_type,
                                "script_exec_mode": script_exec_mode
                            }
                        
                        # 确保source_table有值且不为None才添加到sources列表中
                        if source_table and source_table not in scripts_info[script_name]["sources"]:
                            scripts_info[script_name]["sources"].append(source_table)
                            logger.debug(f"为表 {table_name} 的脚本 {script_name} 添加源表: {source_table}")
                    
                    # 处理分组信息
                    if scripts_info:
                        # 存储完整的脚本信息
                        table_info['scripts_info'] = scripts_info
                        
                        # 如果只有一个脚本，直接使用它
                        if len(scripts_info) == 1:
                            script_name = list(scripts_info.keys())[0]
                            script_info = scripts_info[script_name]
                            
                            table_info['source_tables'] = script_info["sources"]  # 使用数组
                            table_info['script_name'] = script_name
                            table_info['script_type'] = script_info["script_type"]
                            table_info['script_exec_mode'] = script_info["script_exec_mode"]
                            logger.info(f"表 {table_name} 有单个脚本 {script_name}，源表: {script_info['sources']}")
                        else:
                            # 如果有多个不同脚本，记录多脚本信息
                            logger.info(f"表 {table_name} 有多个不同脚本: {list(scripts_info.keys())}")
                            # 暂时使用第一个脚本的信息作为默认值
                            first_script = list(scripts_info.keys())[0]
                            table_info['source_tables'] = scripts_info[first_script]["sources"]
                            table_info['script_name'] = first_script
                            table_info['script_type'] = scripts_info[first_script]["script_type"]
                            table_info['script_exec_mode'] = scripts_info[first_script]["script_exec_mode"]
                    else:
                        logger.warning(f"表 {table_name} 未找到有效的脚本信息")
                        table_info['source_tables'] = []  # 使用空数组
                else:
                    logger.warning(f"未找到表 {table_name} 的关系信息")
                    table_info['source_tables'] = []  # 使用空数组
            else:
                logger.warning(f"在Neo4j中找不到表 {table_name} 的信息")
    except Exception as e:
        logger.error(f"获取表 {table_name} 的信息时出错: {str(e)}")
    finally:
        driver.close()
    
    return table_info

def process_dependencies(tables_info):
    """处理表间依赖关系，添加被动调度的表"""
    # 存储所有表信息的字典
    all_tables = {t['target_table']: t for t in tables_info}
    driver = get_neo4j_driver()
    
    try:
        with driver.session() as session:
            for table_name, table_info in list(all_tables.items()):
                if table_info.get('target_table_label') == 'DataModel':
                    # 查询其依赖表
                    query = """
                        MATCH (dm {en_name: $table_name})-[:DERIVED_FROM]->(dep)
                        RETURN dep.en_name AS dep_name, labels(dep) AS dep_labels, 
                               dep.status AS dep_status, dep.frequency AS dep_frequency
                    """
                    result = session.run(query, table_name=table_name)
                    
                    for record in result:
                        dep_name = record.get("dep_name")
                        dep_labels = record.get("dep_labels", [])
                        dep_status = record.get("dep_status", True)
                        dep_frequency = record.get("dep_frequency")
                        
                        # 处理未被直接调度的依赖表
                        if dep_name and dep_name not in all_tables:
                            logger.info(f"发现被动依赖表: {dep_name}, 标签: {dep_labels}")
                            
                            # 获取依赖表详细信息
                            dep_info = get_table_info_from_neo4j(dep_name)
                            dep_info['is_directly_schedule'] = False
                            
                            # 处理调度频率继承
                            if not dep_info.get('frequency'):
                                dep_info['frequency'] = table_info.get('frequency')
                            
                            all_tables[dep_name] = dep_info
    except Exception as e:
        logger.error(f"处理依赖关系时出错: {str(e)}")
    finally:
        driver.close()
    
    return list(all_tables.values())

def filter_invalid_tables(tables_info):
    """过滤无效表及其依赖，使用NetworkX构建依赖图"""
    # 构建表名到索引的映射
    table_dict = {t['target_table']: i for i, t in enumerate(tables_info)}
    
    # 找出无效表
    invalid_tables = set()
    for table in tables_info:
        if table.get('target_table_status') is False:
            invalid_tables.add(table['target_table'])
            logger.info(f"表 {table['target_table']} 的状态为无效")
    
    # 构建依赖图
    G = nx.DiGraph()
    
    # 添加所有节点
    for table in tables_info:
        G.add_node(table['target_table'])
    
    # 查询并添加依赖边
    driver = get_neo4j_driver()
    try:
        with driver.session() as session:
            for table in tables_info:
                if table.get('target_table_label') == 'DataModel':
                    query = """
                        MATCH (source {en_name: $table_name})-[:DERIVED_FROM]->(target)
                        RETURN target.en_name AS target_name
                    """
                    result = session.run(query, table_name=table['target_table'])
                    
                    for record in result:
                        target_name = record.get("target_name")
                        if target_name and target_name in table_dict:
                            # 添加从目标到源的边，表示目标依赖于源
                            G.add_edge(table['target_table'], target_name)
                            logger.debug(f"添加依赖边: {table['target_table']} -> {target_name}")
    except Exception as e:
        logger.error(f"构建依赖图时出错: {str(e)}")
    finally:
        driver.close()
    
    # 找出依赖于无效表的所有表
    downstream_invalid = set()
    for invalid_table in invalid_tables:
        # 获取可从无效表到达的所有节点
        try:
            descendants = nx.descendants(G, invalid_table)
            downstream_invalid.update(descendants)
            logger.info(f"表 {invalid_table} 的下游无效表: {descendants}")
        except Exception as e:
            logger.error(f"处理表 {invalid_table} 的下游依赖时出错: {str(e)}")
    
    # 合并所有无效表
    all_invalid = invalid_tables.union(downstream_invalid)
    logger.info(f"总共 {len(all_invalid)} 个表被标记为无效: {all_invalid}")
    
    # 过滤出有效表
    valid_tables = [t for t in tables_info if t['target_table'] not in all_invalid]
    logger.info(f"过滤后保留 {len(valid_tables)} 个有效表")
    
    return valid_tables

def touch_data_scheduler_file():
    """
    更新数据调度器DAG文件的修改时间，触发重新解析
    
    返回:
        bool: 是否成功更新
    """
    data_scheduler_path = os.path.join(os.path.dirname(__file__), 'dag_dataops_pipeline_data_scheduler.py')

    
    success = False
    try:
        if os.path.exists(data_scheduler_path):
            # 更新文件修改时间，触发Airflow重新解析
            os.utime(data_scheduler_path, None)
            logger.info(f"已触发数据调度器DAG重新解析: {data_scheduler_path}")
            success = True
        else:
            logger.warning(f"数据调度器DAG文件不存在: {data_scheduler_path}")
                
        return success
    except Exception as e:
        logger.error(f"触发DAG重新解析时出错: {str(e)}")
        return False

def get_subscription_state_hash():
    """获取订阅表状态的哈希值"""
    conn = get_pg_conn()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT table_name, schedule_is_enabled
            FROM schedule_status
            ORDER BY table_name
        """)
        rows = cursor.fetchall()
        # 将所有行拼接成一个字符串，然后计算哈希值
        data_str = '|'.join(f"{row[0]}:{row[1]}" for row in rows)
        return hashlib.md5(data_str.encode()).hexdigest()
    except Exception as e:
        logger.error(f"计算订阅表状态哈希值时出错: {str(e)}")
        return None
    finally:
        cursor.close()
        conn.close()

def check_execution_plan_in_db(**kwargs):
    """
    检查当天的执行计划是否存在于数据库中
    返回False将阻止所有下游任务执行
    """
    # 获取执行日期
    dag_run = kwargs.get('dag_run')
    logical_date = dag_run.logical_date
    local_logical_date = pendulum.instance(logical_date).in_timezone('Asia/Shanghai')
    exec_date = local_logical_date.strftime('%Y-%m-%d')
    logger.info(f"logical_date： {logical_date} ")
    logger.info(f"local_logical_date {local_logical_date} ")
    logger.info(f"检查执行日期 exec_date {exec_date} 的执行计划是否存在于数据库中")
   
    
    # 检查数据库中是否存在执行计划
    conn = get_pg_conn()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT plan
            FROM airflow_exec_plans
            WHERE exec_date = %s
            ORDER BY logical_date DESC
            LIMIT 1
        """, (exec_date,))
        
        result = cursor.fetchone()
        if not result:
            logger.error(f"数据库中不存在执行日期 {exec_date} 的执行计划")
            return False
        
        # 检查执行计划内容是否有效
        try:
            # PostgreSQL的jsonb类型会被psycopg2自动转换为Python字典，无需再使用json.loads
            plan_data = result[0]            
            # 检查必要字段
            if "exec_date" not in plan_data:
                logger.error("执行计划缺少exec_date字段")
                return False
                
            if not isinstance(plan_data.get("resource_tasks", []), list):
                logger.error("执行计划的resource_tasks字段无效")
                return False
                
            if not isinstance(plan_data.get("model_tasks", []), list):
                logger.error("执行计划的model_tasks字段无效")
                return False
            
            # 检查是否有任务数据
            resource_tasks = plan_data.get("resource_tasks", [])
            model_tasks = plan_data.get("model_tasks", [])
            
            logger.info(f"执行计划验证成功: 包含 {len(resource_tasks)} 个资源任务和 {len(model_tasks)} 个模型任务")
            return True
            
        except Exception as je:
            logger.error(f"处理执行计划数据时出错: {str(je)}")
            return False
        
    except Exception as e:
        logger.error(f"检查数据库中执行计划时出错: {str(e)}")
        return False
    finally:
        cursor.close()
        conn.close()

def save_execution_plan_to_db(execution_plan, dag_id, run_id, logical_date, ds):
    """
    将执行计划保存到airflow_exec_plans表
    
    参数:
        execution_plan (dict): 执行计划字典
        dag_id (str): DAG的ID
        run_id (str): DAG运行的ID
        logical_date (datetime): 逻辑日期
        ds (str): 日期字符串，格式为YYYY-MM-DD
    
    返回:
        bool: 操作是否成功
    """
    conn = get_pg_conn()
    cursor = conn.cursor()
    
    try:
        # 将执行计划转换为JSON字符串
        plan_json = json.dumps(execution_plan)
        
        # 获取本地时间
        local_logical_date = pendulum.instance(logical_date).in_timezone('Asia/Shanghai')
        
        # 插入记录
        cursor.execute("""
            INSERT INTO airflow_exec_plans
            (dag_id, run_id, logical_date, local_logical_date, exec_date, plan)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (dag_id, run_id, logical_date, local_logical_date, ds, plan_json))
        
        conn.commit()
        logger.info(f"成功将执行计划保存到airflow_exec_plans表，dag_id={dag_id}, run_id={run_id}, exec_date={ds}")
        return True
    except Exception as e:
        logger.error(f"保存执行计划到数据库时出错: {str(e)}")
        conn.rollback()
        return False
    finally:
        cursor.close()
        conn.close()

def prepare_pipeline_dag_schedule(**kwargs):
    """准备Pipeline DAG调度任务的主函数"""
    # 检查是否是手动触发模式
    is_manual_trigger = False
    params = kwargs.get('params', {})
    if params and 'MANUAL_TRIGGER' in params:
        is_manual_trigger = params.get('MANUAL_TRIGGER', False)
        if is_manual_trigger:
            logger.info(f"接收到手动触发参数: MANUAL_TRIGGER={is_manual_trigger}")
    
    # 获取执行日期
    dag_run = kwargs.get('dag_run')
    logical_date = dag_run.logical_date
    local_logical_date = pendulum.instance(logical_date).in_timezone('Asia/Shanghai')
    exec_date = local_logical_date.strftime('%Y-%m-%d')
    logger.info(f"开始准备执行日期 {exec_date} 的Pipeline调度任务")
    
    # 检查是否需要创建新的执行计划
    need_create_plan = False
    
    # 条件1: 数据库中不存在当天的执行计划
    has_plan_in_db = check_execution_plan_in_db(**kwargs)
    if not has_plan_in_db:
        logger.info(f"数据库中不存在执行日期exec_date {exec_date} 的执行计划，需要创建新的执行计划")
        need_create_plan = True
    
    # 条件2: schedule_status表中的数据发生了变更
    if not need_create_plan:
        # 计算当前哈希值
        current_hash = get_subscription_state_hash()
        # 读取上次记录的哈希值
        hash_file = os.path.join(os.path.dirname(__file__), '.subscription_state')
        last_hash = None
        if os.path.exists(hash_file):
            try:
                with open(hash_file, 'r') as f:
                    last_hash = f.read().strip()
            except Exception as e:
                logger.warning(f"读取上次订阅状态哈希值失败: {str(e)}")
        
        # 如果哈希值不同，表示数据发生了变更
        if current_hash != last_hash:
            logger.info(f"检测到schedule_status表数据变更。旧哈希值: {last_hash}, 新哈希值: {current_hash}")
            need_create_plan = True
    
    # 手动触发模式覆盖以上判断
    if is_manual_trigger:
        logger.info("手动触发模式，将创建新的执行计划")
        need_create_plan = True
    
    # 如果不需要创建新的执行计划，直接返回
    if not need_create_plan:
        logger.info("无需创建新的执行计划")
        return 0
    
    # 继续处理，创建新的执行计划
    # 1. 获取启用的表
    enabled_tables = get_enabled_tables()
    logger.info(f"从schedule_status表获取到 {len(enabled_tables)} 个启用的表")
    
    if not enabled_tables:
        logger.warning("没有找到启用的表，准备工作结束")
        return 0
    
    # 2. 获取表的详细信息
    tables_info = []
    for table_name in enabled_tables:
        table_info = get_table_info_from_neo4j(table_name)
        if table_info:
            tables_info.append(table_info)
    
    logger.info(f"成功获取 {len(tables_info)} 个表的详细信息")
    
    # 2.1 根据调度频率过滤表（新增的步骤）
    filtered_tables_info = []
    for table_info in tables_info:
        table_name = table_info['target_table']
        frequency = table_info.get('frequency')
        
        if should_execute_today(table_name, frequency, exec_date):
            filtered_tables_info.append(table_info)
            logger.info(f"表 {table_name} (频率: {frequency}) 将在今天{exec_date}执行")
        else:
            logger.info(f"表 {table_name} (频率: {frequency}) 今天{exec_date}不执行，已过滤")
    
    logger.info(f"按调度频率过滤后，今天{exec_date}需要执行的表有 {len(filtered_tables_info)} 个")


    # 3. 处理依赖关系，添加被动调度的表
    enriched_tables = process_dependencies(filtered_tables_info)
    logger.info(f"处理依赖后，总共有 {len(enriched_tables)} 个表")
    
    # 4. 过滤无效表及其依赖
    valid_tables = filter_invalid_tables(enriched_tables)
    logger.info(f"过滤无效表后，最终有 {len(valid_tables)} 个有效表")
    
    # 构建执行计划并保存到数据库
    try:
        # 构建执行计划
        resource_tasks = []
        model_tasks = []
        
        # 遍历所有有效表，创建任务信息
        for table in valid_tables:
            # 确保每个表对象都有source_tables字段且是一个列表
            if 'source_tables' not in table or not isinstance(table.get('source_tables'), list):
                logger.warning(f"表 {table['target_table']} 没有source_tables或不是列表，初始化为空列表")
                table['source_tables'] = []
            
            # 处理资源表任务
            if table.get('target_table_label') == 'DataResource':
                task_info = {
                    "source_tables": table.get('source_tables', []),  # 使用数组存储源表
                    "target_table": table['target_table'],
                    "target_table_label": "DataResource",
                    "script_name": table.get('script_name'),
                    "script_exec_mode": table.get('script_exec_mode', 'append'),
                    "frequency": table.get('frequency')
                }
                # 为structure类型添加特殊属性
                if table.get('target_type') == "structure":
                    task_info["target_type"] = "structure"
                    task_info["storage_location"] = table.get('storage_location')  
                              
                resource_tasks.append(task_info)
            # 处理模型表任务
            elif table.get('target_table_label') == 'DataModel':
                # 检查是否有多个脚本信息
                if 'scripts_info' in table and len(table['scripts_info']) > 1:
                    # 处理多脚本情况，为每个脚本创建单独的任务
                    logger.info(f"表 {table['target_table']} 有多个脚本，单独处理每个脚本")
                    
                    for script_name, script_info in table['scripts_info'].items():
                        model_tasks.append({
                            "source_tables": script_info.get("sources", []),  # 使用数组存储源表
                            "target_table": table['target_table'],
                            "target_table_label": "DataModel",
                            "script_name": script_name,
                            "script_exec_mode": script_info.get("script_exec_mode", 'append'),
                            "script_type": script_info.get("script_type", 'python'),
                            "frequency": table.get('frequency')
                        })
                else:
                    # 处理单脚本情况
                    model_tasks.append({
                        "source_tables": table.get('source_tables', []),  # 使用数组存储源表
                        "target_table": table['target_table'],
                        "target_table_label": "DataModel",
                        "script_name": table.get('script_name'),
                        "script_exec_mode": table.get('script_exec_mode', 'append'),
                        "frequency": table.get('frequency')
                    })
        
        # 获取和处理依赖关系
        dependencies = {}
        model_table_names = [t['target_table'] for t in model_tasks]
        
        # 初始化依赖关系字典
        for table_name in model_table_names:
            dependencies[table_name] = []
        
        # 查询Neo4j获取依赖关系
        driver = get_neo4j_driver()
        try:
            with driver.session() as session:
                # 为每个模型表查询依赖
                for table_name in model_table_names:
                    query = """
                        MATCH (source:DataModel {en_name: $table_name})-[:DERIVED_FROM]->(target)
                        RETURN source.en_name AS source, target.en_name AS target, labels(target) AS target_labels
                    """
                    try:
                        # 执行查询
                        result = session.run(query, table_name=table_name)
                        
                        # 尝试获取记录
                        records = []
                        try:
                            if hasattr(result, 'collect'):
                                records = result.collect()
                            else:
                                records = list(result)
                        except Exception as e:
                            logger.warning(f"获取表 {table_name} 的依赖关系记录失败: {str(e)}")
                            records = []
                        
                        # 源表列表，用于后续更新model_tasks
                        source_tables_list = []
                        
                        # 处理依赖关系记录
                        for record in records:
                            target = record.get("target")
                            target_labels = record.get("target_labels", [])
                            
                            if target:
                                # 确定依赖表类型
                                table_type = next((label for label in target_labels 
                                                 if label in ["DataModel", "DataResource"]), None)
                                
                                # 添加依赖关系
                                dependencies[table_name].append({
                                    "table_name": target,
                                    "table_type": table_type
                                })
                                
                                # 记录源表
                                source_tables_list.append(target)
                                logger.info(f"添加其他依赖: {table_name} -> {target}")
                        
                        # 更新model_tasks中的source_tables
                        for mt in model_tasks:
                            if mt['target_table'] == table_name:
                                # 确保source_tables是数组
                                if not isinstance(mt.get('source_tables'), list):
                                    mt['source_tables'] = []
                                
                                # 添加依赖的源表
                                for source_table in source_tables_list:
                                    if source_table and source_table not in mt['source_tables']:
                                        mt['source_tables'].append(source_table)
                                        logger.info(f"从依赖关系中添加源表 {source_table} 到 {table_name}")
                    
                    except Exception as e:
                        logger.error(f"处理表 {table_name} 的依赖关系时出错: {str(e)}")
                        
        except Exception as e:
            logger.error(f"查询Neo4j依赖关系时出错: {str(e)}")
        finally:
            driver.close()
        
        # 创建最终执行计划
        execution_plan = {
            "exec_date": exec_date,
            "resource_tasks": resource_tasks,
            "model_tasks": model_tasks,
            "dependencies": dependencies
        }
        
        # 更新订阅表状态哈希值
        current_hash = get_subscription_state_hash()
        hash_file = os.path.join(os.path.dirname(__file__), '.subscription_state')
        with open(hash_file, 'w') as f:
            f.write(current_hash)
        logger.info(f"已更新订阅表状态哈希值: {current_hash}")
        
        # 触发数据调度器DAG重新解析
        touch_data_scheduler_file()
        
        # 保存执行计划到数据库表
        try:
            # 获取DAG运行信息
            dag_run = kwargs.get('dag_run')
            if dag_run:
                dag_id = dag_run.dag_id
                run_id = dag_run.run_id
                logical_date = dag_run.logical_date
                local_logical_date = pendulum.instance(logical_date).in_timezone('Asia/Shanghai')
            else:
                # 如果无法获取dag_run，使用默认值
                dag_id = kwargs.get('dag').dag_id if 'dag' in kwargs else "dag_dataops_pipeline_prepare_scheduler"
                run_id = f"manual_{datetime.now().strftime('%Y%m%d%H%M%S')}"
                logical_date = datetime.now()
            
            # 保存到数据库
            save_result = save_execution_plan_to_db(
                execution_plan=execution_plan,
                dag_id=dag_id,
                run_id=run_id,
                logical_date=local_logical_date,
                ds=exec_date
            )
            
            if save_result:
                logger.info("执行计划已成功保存到数据库")
            else:
                raise Exception("执行计划保存到数据库失败")
            
        except Exception as db_e:
            # 捕获数据库保存错误
            error_msg = f"保存执行计划到数据库时出错: {str(db_e)}"
            logger.error(error_msg)
            raise Exception(error_msg)
            
    except Exception as e:
        error_msg = f"创建或保存执行计划时出错: {str(e)}"
        logger.error(error_msg)
        # 强制抛出异常，确保任务失败，阻止下游DAG执行
        raise Exception(error_msg)
    
    return len(valid_tables)  # 返回有效表数量

# 创建DAG
with DAG(
    "dag_dataops_pipeline_prepare_scheduler",
    start_date=datetime(2024, 1, 1),
    # 每小时执行一次
    schedule_interval="0 * * * *",
    catchup=False,
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    params={
        'MANUAL_TRIGGER': False, 
    },
) as dag:
    
    # 任务开始标记
    start_preparation = EmptyOperator(
        task_id="start_preparation",
        dag=dag
    )
    
    # 准备调度任务
    prepare_task = PythonOperator(
        task_id="prepare_pipeline_dag_schedule",
        python_callable=prepare_pipeline_dag_schedule,
        provide_context=True,
        dag=dag
    )
    
    # 检查执行计划是否存在于数据库中
    check_plan_in_db = ShortCircuitOperator(
        task_id="check_execution_plan_in_db",
        python_callable=check_execution_plan_in_db,
        provide_context=True,
        dag=dag
    )
    
    # 准备完成标记
    preparation_completed = EmptyOperator(
        task_id="preparation_completed",
        dag=dag
    )
    
    # 设置任务依赖
    start_preparation >> prepare_task >> check_plan_in_db >> preparation_completed