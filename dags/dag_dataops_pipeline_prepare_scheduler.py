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
from common import (
    get_pg_conn, 
    get_neo4j_driver,
    get_today_date
)
from config import PG_CONFIG, NEO4J_CONFIG, EXECUTION_PLAN_KEEP_COUNT

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
    """检查表是否在schedule_status表中直接订阅"""
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
                RETURN labels(t) AS labels, t.status AS status, t.frequency AS frequency
            """
            result = session.run(query_table, table_name=table_name)
            record = result.single()
            
            if record:
                labels = record.get("labels", [])
                table_info['target_table_label'] = [label for label in labels if label in ["DataResource", "DataModel", "DataSource"]][0] if labels else None
                table_info['target_table_status'] = record.get("status", True)  # 默认为True
                table_info['default_update_frequency'] = record.get("frequency")
                
                # 根据标签类型查询关系和脚本信息
                if "DataResource" in labels:
                    query_rel = """
                        MATCH (target {en_name: $table_name})-[rel:ORIGINATES_FROM]->(source)
                        RETURN source.en_name AS source_table, rel.script_name AS script_name,
                               rel.script_type AS script_type, rel.script_exec_mode AS script_exec_mode
                    """
                elif "DataModel" in labels:
                    query_rel = """
                        MATCH (target {en_name: $table_name})-[rel:DERIVED_FROM]->(source)
                        RETURN source.en_name AS source_table, rel.script_name AS script_name,
                               rel.script_type AS script_type, rel.script_exec_mode AS script_exec_mode
                    """
                else:
                    logger.warning(f"表 {table_name} 不是DataResource或DataModel类型")
                    return table_info
                
                result = session.run(query_rel, table_name=table_name)
                record = result.single()
                
                if record:
                    table_info['source_table'] = record.get("source_table")     

                    # 检查script_name是否为空
                    script_name = record.get("script_name")
                    if not script_name:
                        logger.warning(f"表 {table_name} 的关系中没有script_name属性，可能导致后续处理出错")
                    table_info['script_name'] = script_name
                    
                    # 设置默认值，确保即使属性为空也有默认值
                    table_info['script_type'] = record.get("script_type", "python")  # 默认为python
                    table_info['script_exec_mode'] = record.get("script_exec_mode", "append")  # 默认为append
                else:
                    logger.warning(f"未找到表 {table_name} 的关系信息")
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
                            if not dep_info.get('default_update_frequency'):
                                dep_info['default_update_frequency'] = table_info.get('default_update_frequency')
                            
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

def check_if_date_exists(exec_date):
    """
    检查指定日期是否已存在于airflow_dag_schedule表中
    
    参数:
        exec_date (str): 执行日期，格式为'YYYY-MM-DD'
        
    返回:
        bool: 如果日期已存在返回True，否则返回False
    """
    conn = get_pg_conn()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT COUNT(*) 
            FROM airflow_dag_schedule 
            WHERE exec_date = %s
        """, (exec_date,))
        result = cursor.fetchone()
        count = result[0] if result else 0
        exists = count > 0
        
        if exists:
            logger.info(f"执行日期 {exec_date} 已存在于airflow_dag_schedule表中，共有 {count} 条记录")
        else:
            logger.info(f"执行日期 {exec_date} 不存在于airflow_dag_schedule表中")
            
        return exists
    except Exception as e:
        logger.error(f"检查日期 {exec_date} 是否存在时出错: {str(e)}")
        return False
    finally:
        cursor.close()
        conn.close()

def clear_existing_records(exec_date):
    """
    清除指定日期的所有记录
    
    参数:
        exec_date (str): 执行日期，格式为'YYYY-MM-DD'
        
    返回:
        int: 清除的记录数量
    """
    conn = get_pg_conn()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT COUNT(*) 
            FROM airflow_dag_schedule 
            WHERE exec_date = %s
        """, (exec_date,))
        result = cursor.fetchone()
        count = result[0] if result else 0
        
        if count > 0:
            cursor.execute("""
                DELETE FROM airflow_dag_schedule 
                WHERE exec_date = %s
            """, (exec_date,))
            conn.commit()
            logger.info(f"已清除日期 {exec_date} 的 {count} 条记录")
        else:
            logger.info(f"日期 {exec_date} 没有记录需要清除")
        
        return count
    except Exception as e:
        logger.error(f"清除日期 {exec_date} 的记录时出错: {str(e)}")
        conn.rollback()
        return 0
    finally:
        cursor.close()
        conn.close()

def write_to_airflow_dag_schedule(exec_date, tables_info):
    """将表信息写入airflow_dag_schedule表"""
    conn = get_pg_conn()
    cursor = conn.cursor()
    
    try:
        # 批量插入新数据
        inserted_count = 0
        for table in tables_info:
            cursor.execute("""
                INSERT INTO airflow_dag_schedule (
                    exec_date, source_table, target_table, target_table_label,
                    target_table_status, is_directly_schedule, default_update_frequency,
                    script_name, script_type, script_exec_mode
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                exec_date,
                table.get('source_table'),
                table['target_table'],
                table.get('target_table_label'),
                table.get('target_table_status', True),
                table.get('is_directly_schedule', False),
                table.get('default_update_frequency'),
                table.get('script_name'),
                table.get('script_type', 'python'),
                table.get('script_exec_mode', 'append')
            ))
            inserted_count += 1
        
        conn.commit()
        logger.info(f"成功插入 {inserted_count} 条记录到 airflow_dag_schedule 表")
        return inserted_count
    except Exception as e:
        logger.error(f"写入 airflow_dag_schedule 表时出错: {str(e)}")
        conn.rollback()
        # 不要返回0，而是重新抛出异常，确保错误被正确传播
        raise
    finally:
        cursor.close()
        conn.close()

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

def has_any_execution_plans():
    """
    检查当前目录下是否存在任何执行计划文件
    
    返回:
        bool: 如果存在任何执行计划文件返回True，否则返回False
    """
    dag_dir = os.path.dirname(__file__)
    for file in os.listdir(dag_dir):
        if file.startswith('exec_plan_') and file.endswith('.json'):
            logger.info(f"找到现有执行计划文件: {file}")
            return True
    
    logger.info("未找到任何执行计划文件")
    return False

def get_execution_plan_files():
    """
    获取所有执行计划文件，按日期排序
    
    返回:
        list: 排序后的执行计划文件列表，格式为[(日期, json文件路径, ready文件路径)]
    """
    dag_dir = os.path.dirname(__file__)
    plan_files = []
    
    # 查找所有执行计划文件
    for file in os.listdir(dag_dir):
        match = re.match(r'exec_plan_(\d{4}-\d{2}-\d{2})\.json', file)
        if match:
            date_str = match.group(1)
            json_path = os.path.join(dag_dir, file)
            ready_path = os.path.join(dag_dir, f"exec_plan_{date_str}.ready")
            
            if os.path.exists(ready_path):
                plan_files.append((date_str, json_path, ready_path))
    
    # 按日期排序（从旧到新）
    plan_files.sort(key=lambda x: x[0])
    
    return plan_files

def cleanup_old_execution_plans(keep_count=None):
    """
    清理过期的执行计划文件，保留最新的指定数量
    
    参数:
        keep_days (int): 要保留的文件天数，如果为None则使用配置
    
    返回:
        int: 删除的文件数量
    """
    if keep_count is None:
        keep_count = EXECUTION_PLAN_KEEP_COUNT
    
    # 获取所有执行计划文件
    plan_files = get_execution_plan_files()
    logger.info(f"找到 {len(plan_files)} 个执行计划文件，将保留最新的 {keep_count} 个")
    
    # 如果文件数量未超过保留数，不需要删除
    if len(plan_files) <= keep_count:
        logger.info(f"执行计划文件数量 ({len(plan_files)}) 未超过保留数量 ({keep_count})，无需清理")
        return 0
    
    # 删除最旧的文件
    files_to_delete = plan_files[:-keep_count]
    deleted_count = 0
    
    for _, json_path, ready_path in files_to_delete:
        try:
            # 删除JSON文件
            if os.path.exists(json_path):
                os.remove(json_path)
                deleted_count += 1
                logger.info(f"已删除过期执行计划文件: {json_path}")
            
            # 删除ready文件
            if os.path.exists(ready_path):
                os.remove(ready_path)
                deleted_count += 1
                logger.info(f"已删除过期ready文件: {ready_path}")
        except Exception as e:
            logger.error(f"删除文件时出错: {str(e)}")
    
    return deleted_count

def prepare_pipeline_dag_schedule(**kwargs):
    """准备Pipeline DAG调度任务的主函数"""
    # 检查是否是手动触发模式
    is_force_refresh = False
    params = kwargs.get('params', {})
    if params and 'FORCE_REFRESH' in params:
        is_force_refresh = params.get('FORCE_REFRESH', False)
        logger.info(f"接收到强制刷新参数: FORCE_REFRESH={is_force_refresh}")
    
    # 获取执行日期
    exec_date = kwargs.get('ds') or get_today_date()
    logger.info(f"开始准备执行日期 {exec_date} 的Pipeline调度任务")
    
    # 定义执行计划文件路径 - 使用新的基于日期的命名
    plan_base_path = os.path.join(os.path.dirname(__file__), f'exec_plan_{exec_date}')
    plan_path = f"{plan_base_path}.json"
    ready_path = f"{plan_base_path}.ready"
    
    # 检查是否需要创建新的执行计划文件
    need_create_plan = False
    
    # 新的条件1: 当前目录下没有任何json文件
    has_any_plans = has_any_execution_plans()
    if not has_any_plans:
        logger.info("当前目录下没有任何执行计划文件，需要创建新的执行计划")
        need_create_plan = True
    
    # 新的条件2: schedule_status表中的数据发生了变更
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
    
    # 强制刷新模式覆盖以上判断
    if is_force_refresh:
        logger.info("强制刷新模式，将创建新的执行计划")
        need_create_plan = True
    
    # 如果不需要创建新的执行计划，直接返回
    if not need_create_plan:
        logger.info("无需创建新的执行计划文件")
        return 0
    
    # 继续处理，创建新的执行计划
    # 清除数据库中的现有记录
    clear_existing_records(exec_date)
    
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
    
    # 3. 处理依赖关系，添加被动调度的表
    enriched_tables = process_dependencies(tables_info)
    logger.info(f"处理依赖后，总共有 {len(enriched_tables)} 个表")
    
    # 4. 过滤无效表及其依赖
    valid_tables = filter_invalid_tables(enriched_tables)
    logger.info(f"过滤无效表后，最终有 {len(valid_tables)} 个有效表")
    
    # 5. 写入airflow_dag_schedule表
    inserted_count = write_to_airflow_dag_schedule(exec_date, valid_tables)
    
    # 6. 检查插入操作是否成功，如果失败则抛出异常
    if inserted_count == 0 and valid_tables:
        error_msg = f"插入操作失败，无记录被插入到airflow_dag_schedule表，但有{len(valid_tables)}个有效表需要处理"
        logger.error(error_msg)
        raise Exception(error_msg)
    
    # 7. 保存最新执行计划，供DAG读取使用
    try:
        # 构建执行计划
        resource_tasks = []
        model_tasks = []
        
        for table in valid_tables:
            if table.get('target_table_label') == 'DataResource':
                resource_tasks.append({
                    "source_table": table.get('source_table'),
                    "target_table": table['target_table'],
                    "target_table_label": "DataResource",
                    "script_name": table.get('script_name'),
                    "script_exec_mode": table.get('script_exec_mode', 'append')
                })
            elif table.get('target_table_label') == 'DataModel':
                model_tasks.append({
                    "source_table": table.get('source_table'),
                    "target_table": table['target_table'],
                    "target_table_label": "DataModel",
                    "script_name": table.get('script_name'),
                    "script_exec_mode": table.get('script_exec_mode', 'append')
                })
        
        # 获取依赖关系
        model_table_names = [t['target_table'] for t in model_tasks]
        dependencies = {}
        
        driver = get_neo4j_driver()
        try:
            with driver.session() as session:
                for table_name in model_table_names:
                    query = """
                        MATCH (source:DataModel {en_name: $table_name})-[:DERIVED_FROM]->(target)
                        RETURN source.en_name AS source, target.en_name AS target, labels(target) AS target_labels
                    """
                    result = session.run(query, table_name=table_name)
                    
                    deps = []
                    for record in result:
                        target = record.get("target")
                        target_labels = record.get("target_labels", [])
                        
                        if target:
                            table_type = next((label for label in target_labels if label in ["DataModel", "DataResource"]), None)
                            deps.append({
                                "table_name": target,
                                "table_type": table_type
                            })
                    
                    dependencies[table_name] = deps
        finally:
            driver.close()
        
        # 创建执行计划
        execution_plan = {
            "exec_date": exec_date,
            "resource_tasks": resource_tasks,
            "model_tasks": model_tasks,
            "dependencies": dependencies
        }
        
        # 创建临时文件
        temp_plan_path = f"{plan_path}.temp"
        
        try:
            # 写入临时文件
            with open(temp_plan_path, 'w') as f:
                json.dump(execution_plan, f, indent=2)
            logger.info(f"已保存执行计划到临时文件: {temp_plan_path}")
            
            # 原子替换正式文件
            os.replace(temp_plan_path, plan_path)
            logger.info(f"已替换执行计划文件: {plan_path}")
            
            # 创建ready文件，标记执行计划就绪，包含详细时间信息
            now = datetime.now()
            timestamp = now.strftime("%Y-%m-%d %H:%M:%S")
            with open(ready_path, 'w') as f:
                f.write(f"Created at: {timestamp}\nFor date: {exec_date}")
            logger.info(f"已创建ready标记文件: {ready_path}")
            
            # 更新订阅表状态哈希值
            current_hash = get_subscription_state_hash()
            hash_file = os.path.join(os.path.dirname(__file__), '.subscription_state')
            with open(hash_file, 'w') as f:
                f.write(current_hash)
            logger.info(f"已更新订阅表状态哈希值: {current_hash}")
            
            # 清理过期的执行计划文件
            deleted_count = cleanup_old_execution_plans()
            logger.info(f"清理了 {deleted_count} 个过期执行计划文件")
            
            # dag_dataops_pipeline_data_scheduler.py文件的修改日期更新
            touch_data_scheduler_file()
            
        except Exception as e:
            logger.error(f"保存执行计划文件或触发DAG重新解析时出错: {str(e)}")
            # 出错时清理临时文件
            if os.path.exists(temp_plan_path):
                try:
                    os.remove(temp_plan_path)
                    logger.info(f"已清理临时文件: {temp_plan_path}")
                except Exception as rm_e:
                    logger.error(f"清理临时文件时出错: {str(rm_e)}")
            raise  # 重新抛出异常，确保任务失败
                
    except Exception as e:
        error_msg = f"保存或验证执行计划文件时出错: {str(e)}"
        logger.error(error_msg)
        # 强制抛出异常，确保任务失败，阻止下游DAG执行
        raise Exception(error_msg)
    
    return inserted_count

def check_execution_plan_file(**kwargs):
    """
    检查当天的执行计划文件是否存在且有效
    返回False将阻止所有下游任务执行
    """
    # 获取执行日期
    exec_date = kwargs.get('ds') or get_today_date()
    logger.info(f"检查执行日期 {exec_date} 的执行计划文件是否存在且有效")
    
    # 定义执行计划文件路径
    plan_path = os.path.join(os.path.dirname(__file__), f'exec_plan_{exec_date}.json')
    ready_path = os.path.join(os.path.dirname(__file__), f'exec_plan_{exec_date}.ready')
    
    # 检查文件是否存在
    if not os.path.exists(plan_path):
        logger.error(f"执行计划文件不存在: {plan_path}")
        return False
    
    # 检查ready标记是否存在
    if not os.path.exists(ready_path):
        logger.error(f"执行计划ready标记文件不存在: {ready_path}")
        return False
    
    # 检查文件是否可读且内容有效
    try:
        with open(plan_path, 'r') as f:
            data = json.load(f)
            
            # 检查必要字段
            if "exec_date" not in data:
                logger.error("执行计划缺少exec_date字段")
                return False
                
            if not isinstance(data.get("resource_tasks", []), list):
                logger.error("执行计划的resource_tasks字段无效")
                return False
                
            if not isinstance(data.get("model_tasks", []), list):
                logger.error("执行计划的model_tasks字段无效")
                return False
            
            # 检查是否有任务数据
            resource_tasks = data.get("resource_tasks", [])
            model_tasks = data.get("model_tasks", [])
            if not resource_tasks and not model_tasks:
                logger.warning("执行计划不包含任何任务，但文件格式有效")
                # 注意：即使没有任务，我们仍然允许流程继续
            
            logger.info(f"执行计划文件验证成功: 包含 {len(resource_tasks)} 个资源任务和 {len(model_tasks)} 个模型任务")
            return True
            
    except json.JSONDecodeError as je:
        logger.error(f"执行计划文件不是有效的JSON: {str(je)}")
        return False
    except Exception as e:
        logger.error(f"检查执行计划文件时出错: {str(e)}")
        return False

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
        'FORCE_REFRESH': False,  # 添加强制刷新参数，默认为False
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
    
    # 检查执行计划文件
    check_plan_file = ShortCircuitOperator(
        task_id="check_execution_plan_file",
        python_callable=check_execution_plan_file,
        provide_context=True,
        dag=dag
    )
    
    # 准备完成标记
    preparation_completed = EmptyOperator(
        task_id="preparation_completed",
        dag=dag
    )
    
    # 设置任务依赖
    start_preparation >> prepare_task >> check_plan_file >> preparation_completed