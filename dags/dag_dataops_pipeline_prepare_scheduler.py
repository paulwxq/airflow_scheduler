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
    
    # 3. 处理依赖关系，添加被动调度的表
    enriched_tables = process_dependencies(tables_info)
    logger.info(f"处理依赖后，总共有 {len(enriched_tables)} 个表")
    
    # 4. 过滤无效表及其依赖
    valid_tables = filter_invalid_tables(enriched_tables)
    logger.info(f"过滤无效表后，最终有 {len(valid_tables)} 个有效表")
    
    # 构建执行计划并保存到数据库
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