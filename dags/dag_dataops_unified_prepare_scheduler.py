# dag_dataops_unified_prepare_scheduler.py
from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import logging
import networkx as nx
import json
import os
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

def write_to_airflow_dag_schedule(exec_date, tables_info):
    """将表信息写入airflow_dag_schedule表"""
    conn = get_pg_conn()
    cursor = conn.cursor()
    
    try:
        # 清理当日数据，避免重复
        cursor.execute("""
            DELETE FROM airflow_dag_schedule WHERE exec_date = %s
        """, (exec_date,))
        logger.info(f"已清理执行日期 {exec_date} 的现有数据")
        
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
# dag_dataops_unified_prepare_scheduler.py (续)
        conn.rollback()
        # 不要返回0，而是重新抛出异常，确保错误被正确传播
        raise
    finally:
        cursor.close()
        conn.close()

def prepare_unified_dag_schedule(**kwargs):
    """准备统一DAG调度任务的主函数"""
    import hashlib
    
    exec_date = kwargs.get('ds') or get_today_date()
    logger.info(f"开始准备执行日期 {exec_date} 的统一调度任务")
    
    # 检查执行计划文件和ready文件是否存在
    plan_path = os.path.join(os.path.dirname(__file__), 'last_execution_plan.json')
    ready_path = f"{plan_path}.ready"
    files_exist = os.path.exists(plan_path) and os.path.exists(ready_path)
    
    if not files_exist:
        logger.info("执行计划文件或ready标记文件不存在，将重新生成执行计划")
    
    # 1. 计算当前订阅表状态的哈希值，用于检测变化
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
    
    # 获取当前订阅表状态哈希值
    current_hash = get_subscription_state_hash()
    if not current_hash:
        logger.error("无法获取订阅表状态，将中止处理")
        return 0
    
    # 2. 读取上次记录的哈希值
    hash_file = os.path.join(os.path.dirname(__file__), '.subscription_state')
    last_hash = None
    if os.path.exists(hash_file):
        try:
            with open(hash_file, 'r') as f:
                last_hash = f.read().strip()
        except Exception as e:
            logger.warning(f"读取上次订阅状态哈希值失败: {str(e)}")
    
    # 3. 如果哈希值相同且文件存在，说明订阅表未变化且执行计划存在，可以提前退出
    if last_hash == current_hash and files_exist:
        logger.info("订阅表状态未变化且执行计划文件存在，无需更新执行计划")
        return 0
    
    # 记录重新生成原因
    if not files_exist:
        logger.info("执行计划文件或ready标记文件不存在，需要重新生成")
    else:
        logger.info(f"检测到订阅表状态变化。旧哈希值: {last_hash}, 新哈希值: {current_hash}")
    
    # 4. 获取启用的表
    enabled_tables = get_enabled_tables()
    logger.info(f"从schedule_status表获取到 {len(enabled_tables)} 个启用的表")
    
    if not enabled_tables:
        logger.warning("没有找到启用的表，准备工作结束")
        return 0
    
    # 5. 获取表的详细信息
    tables_info = []
    for table_name in enabled_tables:
        table_info = get_table_info_from_neo4j(table_name)
        if table_info:
            tables_info.append(table_info)
    
    logger.info(f"成功获取 {len(tables_info)} 个表的详细信息")
    
    # 6. 处理依赖关系，添加被动调度的表
    enriched_tables = process_dependencies(tables_info)
    logger.info(f"处理依赖后，总共有 {len(enriched_tables)} 个表")
    
    # 7. 过滤无效表及其依赖
    valid_tables = filter_invalid_tables(enriched_tables)
    logger.info(f"过滤无效表后，最终有 {len(valid_tables)} 个有效表")
    
    # 8. 写入airflow_dag_schedule表
    inserted_count = write_to_airflow_dag_schedule(exec_date, valid_tables)
    
    # 9. 检查插入操作是否成功，如果失败则抛出异常
    if inserted_count == 0 and valid_tables:
        error_msg = f"插入操作失败，无记录被插入到airflow_dag_schedule表，但有{len(valid_tables)}个有效表需要处理"
        logger.error(error_msg)
        raise Exception(error_msg)
    
    # 10. 保存最新执行计划，供DAG读取使用
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
        
        # 使用临时文件先写入内容，再原子替换，确保写入过程不会被中断
        temp_plan_path = f"{plan_path}.temp"
        
        try:
            # 10.1 写入临时文件
            with open(temp_plan_path, 'w') as f:
                json.dump(execution_plan, f, indent=2)
            logger.info(f"已保存执行计划到临时文件: {temp_plan_path}")
            
            # 10.2 原子替换正式文件
            os.replace(temp_plan_path, plan_path)
            logger.info(f"已替换执行计划文件: {plan_path}")
            
            # 10.3 创建ready文件，标记执行计划就绪
            with open(ready_path, 'w') as f:
                f.write(datetime.now().isoformat())
            logger.info(f"已创建ready标记文件: {ready_path}")
            
            # 10.4 更新订阅表状态哈希值
            with open(hash_file, 'w') as f:
                f.write(current_hash)
            logger.info(f"已更新订阅表状态哈希值: {current_hash}")
            
            # 10.5 触发data_scheduler DAG重新解析
            data_scheduler_path = os.path.join(os.path.dirname(__file__), 'dag_dataops_unified_data_scheduler.py')
            if os.path.exists(data_scheduler_path):
                # 更新文件修改时间，触发Airflow重新解析
                os.utime(data_scheduler_path, None)
                logger.info(f"已触发数据调度器DAG重新解析: {data_scheduler_path}")
            else:
                logger.warning(f"数据调度器DAG文件不存在: {data_scheduler_path}")
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
    检查执行计划文件是否存在且有效
    返回False将阻止所有下游任务执行
    """
    logger.info("检查执行计划文件是否存在且有效")
    plan_path = os.path.join(os.path.dirname(__file__), 'last_execution_plan.json')
    
    # 检查文件是否存在
    if not os.path.exists(plan_path):
        logger.error(f"执行计划文件不存在: {plan_path}")
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
    "dag_dataops_unified_prepare_scheduler",
    start_date=datetime(2024, 1, 1),
    # 每10分钟运行一次，而不是每天
    # schedule_interval="*/5 * * * *",  
    # 修改调度间隔为每小时执行一次
    schedule_interval="0 * * * *",
    catchup=False,
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    }
) as dag:
    
    # 任务开始标记
    start_preparation = EmptyOperator(
        task_id="start_preparation",
        dag=dag
    )
    
    # 准备调度任务
    prepare_task = PythonOperator(
        task_id="prepare_unified_dag_schedule",
        python_callable=prepare_unified_dag_schedule,
        provide_context=True,
        dag=dag
    )
    
    # 检查执行计划文件
    check_plan_file = ShortCircuitOperator(
        task_id="check_execution_plan_file",
        python_callable=check_execution_plan_file,
        dag=dag
    )
    
    # 准备完成标记
    preparation_completed = EmptyOperator(
        task_id="preparation_completed",
        dag=dag
    )
    
    # 设置任务依赖
    start_preparation >> prepare_task >> check_plan_file >> preparation_completed