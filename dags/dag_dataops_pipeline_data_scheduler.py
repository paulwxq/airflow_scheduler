"""
统一数据运维调度器 DAG

功能：
1. 将数据处理与统计汇总整合到一个DAG中
2. 保留原有的每个处理脚本单独运行的特性，方便通过Web UI查看
3. 支持执行计划文件的动态解析和执行
4. 执行完成后自动生成汇总报告
"""
from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta, date
import logging
import networkx as nx
import json
import os
from decimal import Decimal
from common import (
    get_pg_conn, 
    get_neo4j_driver,
    execute_with_monitoring,
    get_today_date
)
from config import TASK_RETRY_CONFIG, SCRIPTS_BASE_PATH, PG_CONFIG, NEO4J_CONFIG

# 创建日志记录器
logger = logging.getLogger(__name__)

#############################################
# 通用工具函数
#############################################

def json_serial(obj):
    """将日期对象序列化为ISO格式字符串的JSON序列化器"""
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError(f"类型 {type(obj)} 不能被序列化为JSON")

# 添加自定义JSON编码器解决Decimal序列化问题
class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        # 处理日期类型
        elif isinstance(obj, (datetime, date)):
            return obj.isoformat()
        # 让父类处理其他类型
        return super(DecimalEncoder, self).default(obj)

#############################################
# 第一阶段: 准备阶段(Prepare Phase)的函数
#############################################

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
        conn.rollback()
        # 不要返回0，而是重新抛出异常，确保错误被正确传播
        raise
    finally:
        cursor.close()
        conn.close()

def prepare_dag_schedule(**kwargs):
    """准备DAG调度任务的主函数"""
    exec_date = kwargs.get('ds') or get_today_date()
    logger.info(f"开始准备执行日期 {exec_date} 的统一调度任务")
    
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
    
    # 7. 生成执行计划数据
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
    
    # 将执行计划保存到XCom
    kwargs['ti'].xcom_push(key='execution_plan', value=json.dumps(execution_plan, default=json_serial))
    logger.info(f"准备了执行计划，包含 {len(resource_tasks)} 个资源表任务和 {len(model_tasks)} 个模型表任务")
    
    # 保存执行计划到文件
    try:
        plan_path = os.path.join(os.path.dirname(__file__), 'last_execution_plan.json')
        with open(plan_path, 'w') as f:
            json.dump(execution_plan, f, default=json_serial, indent=2)
        logger.info(f"将执行计划保存到文件: {plan_path}")
        
        # 添加ready标记文件
        ready_path = f"{plan_path}.ready"
        with open(ready_path, 'w') as f:
            f.write(datetime.now().isoformat())
        logger.info(f"已创建ready标记文件: {ready_path}")
    except Exception as file_e:
        logger.error(f"保存执行计划到文件时出错: {str(file_e)}")
    
    return inserted_count

def check_execution_plan_file(**kwargs):
    """
    检查执行计划文件是否存在且有效
    返回False将阻止所有下游任务执行
    """
    logger.info("检查执行计划文件是否存在且有效")
    plan_path = os.path.join(os.path.dirname(__file__), 'last_execution_plan.json')
    ready_path = f"{plan_path}.ready"
    
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

#############################################
# 第二阶段: 数据处理阶段(Data Processing Phase)的函数
#############################################

def get_latest_date():
    """获取数据库中包含记录的最近日期"""
    conn = get_pg_conn()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT DISTINCT exec_date
            FROM airflow_dag_schedule 
            ORDER BY exec_date DESC
            LIMIT 1
        """)
        result = cursor.fetchone()
        if result:
            latest_date = result[0]
            logger.info(f"找到最近的包含记录的日期: {latest_date}")
            return latest_date
        else:
            logger.warning("未找到包含记录的日期，将使用当前日期")
            return get_today_date()
    except Exception as e:
        logger.error(f"查找最近日期时出错: {str(e)}")
        return get_today_date()
    finally:
        cursor.close()
        conn.close()

def get_all_tasks(exec_date):
    """获取所有需要执行的任务（DataResource和DataModel）"""
    conn = get_pg_conn()
    cursor = conn.cursor()
    try:
        # 查询数据表中记录总数
        cursor.execute("""
            SELECT COUNT(*) 
            FROM airflow_dag_schedule 
            WHERE exec_date = %s
        """, (exec_date,))
        total_count = cursor.fetchone()[0]
        logger.info(f"执行日期 {exec_date} 在airflow_dag_schedule表中共有 {total_count} 条记录")
        
        # 查询所有资源表任务
        cursor.execute("""
            SELECT source_table, target_table, target_table_label, script_name, script_exec_mode
            FROM airflow_dag_schedule 
            WHERE exec_date = %s AND target_table_label = 'DataResource' AND script_name IS NOT NULL
        """, (exec_date,))
        resource_results = cursor.fetchall()
        logger.info(f"查询到 {len(resource_results)} 条DataResource记录")
        
        # 查询所有模型表任务
        cursor.execute("""
            SELECT source_table, target_table, target_table_label, script_name, script_exec_mode
            FROM airflow_dag_schedule 
            WHERE exec_date = %s AND target_table_label = 'DataModel' AND script_name IS NOT NULL
        """, (exec_date,))
        model_results = cursor.fetchall()
        logger.info(f"查询到 {len(model_results)} 条DataModel记录")
        
        # 整理资源表信息
        resource_tasks = []
        for row in resource_results:
            source_table, target_table, target_table_label, script_name, script_exec_mode = row
            if script_name:  # 确保脚本名称不为空
                resource_tasks.append({
                    "source_table": source_table,
                    "target_table": target_table,
                    "target_table_label": target_table_label,
                    "script_name": script_name,
                    "script_exec_mode": script_exec_mode or "append"
                })
        
        # 整理模型表信息
        model_tasks = []
        for row in model_results:
            source_table, target_table, target_table_label, script_name, script_exec_mode = row
            if script_name:  # 确保脚本名称不为空
                model_tasks.append({
                    "source_table": source_table,
                    "target_table": target_table,
                    "target_table_label": target_table_label,
                    "script_name": script_name,
                    "script_exec_mode": script_exec_mode or "append"
                })
        
        logger.info(f"获取到 {len(resource_tasks)} 个资源表任务和 {len(model_tasks)} 个模型表任务")
        return resource_tasks, model_tasks
    except Exception as e:
        logger.error(f"获取任务信息时出错: {str(e)}")
        return [], []
    finally:
        cursor.close()
        conn.close()

def get_table_dependencies(table_names):
    """获取表之间的依赖关系"""
    driver = get_neo4j_driver()
    dependency_dict = {name: [] for name in table_names}
    
    try:
        with driver.session() as session:
            # 获取所有模型表之间的依赖关系
            query = """
                MATCH (source:DataModel)-[:DERIVED_FROM]->(target)
                WHERE source.en_name IN $table_names
                RETURN source.en_name AS source, target.en_name AS target, labels(target) AS target_labels
            """
            result = session.run(query, table_names=table_names)
            
            for record in result:
                source = record.get("source")
                target = record.get("target")
                target_labels = record.get("target_labels", [])
                
                if source and target:
                    # 将目标表添加到源表的依赖列表中
                    dependency_dict[source].append({
                        "table_name": target,
                        "table_type": next((label for label in target_labels if label in ["DataModel", "DataResource"]), None)
                    })
                    logger.debug(f"依赖关系: {source} 依赖于 {target}")
    except Exception as e:
        logger.error(f"从Neo4j获取依赖关系时出错: {str(e)}")
    finally:
        driver.close()
    
    return dependency_dict

def create_execution_plan(**kwargs):
    """准备执行计划的函数，使用从准备阶段传递的数据"""
    try:
        # 从XCom获取执行计划
        execution_plan = kwargs['ti'].xcom_pull(task_ids='prepare_phase.prepare_dag_schedule', key='execution_plan')
        
        # 如果找不到执行计划，则从数据库获取
        if not execution_plan:
            # 获取执行日期
            exec_date = get_latest_date()
            logger.info(f"未找到执行计划，从数据库获取。使用执行日期: {exec_date}")
            
            # 获取所有任务
            resource_tasks, model_tasks = get_all_tasks(exec_date)
            
            if not resource_tasks and not model_tasks:
                logger.warning(f"执行日期 {exec_date} 没有找到任务")
                return 0
            
            # 为所有模型表获取依赖关系
            model_table_names = [task["target_table"] for task in model_tasks]
            dependencies = get_table_dependencies(model_table_names)
            
            # 创建执行计划
            new_execution_plan = {
                "exec_date": exec_date,
                "resource_tasks": resource_tasks,
                "model_tasks": model_tasks,
                "dependencies": dependencies
            }
            
            # 保存执行计划
            kwargs['ti'].xcom_push(key='execution_plan', value=json.dumps(new_execution_plan, default=json_serial))
            logger.info(f"创建新的执行计划，包含 {len(resource_tasks)} 个资源表任务和 {len(model_tasks)} 个模型表任务")
            
            # 保存执行计划到文件
            try:
                plan_path = os.path.join(os.path.dirname(__file__), 'last_execution_plan.json')
                with open(plan_path, 'w') as f:
                    json.dump(new_execution_plan, f, default=json_serial, indent=2)
                logger.info(f"将执行计划保存到文件: {plan_path}")
                
                # 创建ready标记文件
                ready_path = f"{plan_path}.ready"
                with open(ready_path, 'w') as f:
                    f.write(datetime.now().isoformat())
                logger.info(f"已创建ready标记文件: {ready_path}")
            except Exception as file_e:
                logger.error(f"保存执行计划到文件时出错: {str(file_e)}")
            
            return json.dumps(new_execution_plan, default=json_serial)
        
        # 如果从XCom获取到了执行计划，也保存到文件
        try:
            plan_json = json.loads(execution_plan) if isinstance(execution_plan, str) else execution_plan
            plan_path = os.path.join(os.path.dirname(__file__), 'last_execution_plan.json')
            with open(plan_path, 'w') as f:
                json.dump(plan_json, f, default=json_serial, indent=2)
            logger.info(f"将从XCom获取的执行计划保存到文件: {plan_path}")
            
            # 创建ready标记文件
            ready_path = f"{plan_path}.ready"
            with open(ready_path, 'w') as f:
                f.write(datetime.now().isoformat())
            logger.info(f"已创建ready标记文件: {ready_path}")
        except Exception as file_e:
            logger.error(f"保存从XCom获取的执行计划到文件时出错: {str(file_e)}")
        
        logger.info(f"成功获取执行计划")
        return execution_plan
    except Exception as e:
        logger.error(f"创建执行计划时出错: {str(e)}")
        # 返回空执行计划
        empty_plan = {
            "exec_date": get_today_date(),
            "resource_tasks": [],
            "model_tasks": [],
            "dependencies": {}
        }
        
        # 尝试保存空执行计划到文件
        try:
            plan_path = os.path.join(os.path.dirname(__file__), 'last_execution_plan.json')
            with open(plan_path, 'w') as f:
                json.dump(empty_plan, f, default=json_serial, indent=2)
            logger.info(f"将空执行计划保存到文件: {plan_path}")
            
            # 创建ready标记文件
            ready_path = f"{plan_path}.ready"
            with open(ready_path, 'w') as f:
                f.write(datetime.now().isoformat())
            logger.info(f"已创建ready标记文件: {ready_path}")
        except Exception as file_e:
            logger.error(f"保存空执行计划到文件时出错: {str(file_e)}")
            
        return json.dumps(empty_plan, default=json_serial)

def process_resource(target_table, script_name, script_exec_mode, exec_date):
    """处理单个资源表"""
    logger.info(f"执行资源表 {target_table} 的脚本 {script_name}")
    # 检查exec_date是否是JSON字符串
    if isinstance(exec_date, str) and exec_date.startswith('{'):
        try:
            # 尝试解析JSON字符串
            exec_date_data = json.loads(exec_date)
            exec_date = exec_date_data.get("exec_date")
            logger.info(f"从JSON中提取执行日期: {exec_date}")
        except Exception as e:
            logger.error(f"解析exec_date JSON时出错: {str(e)}")
    
    try:
        # 正常调用执行监控函数
        result = execute_with_monitoring(
            target_table=target_table,
            script_name=script_name,
            script_exec_mode=script_exec_mode,
            exec_date=exec_date
        )
        logger.info(f"资源表 {target_table} 处理完成，结果: {result}")
        return result
    except Exception as e:
        logger.error(f"处理资源表 {target_table} 时出错: {str(e)}")
        # 确保即使出错也返回结果，不会阻塞DAG
        return False

def process_model(target_table, script_name, script_exec_mode, exec_date):
    """处理单个模型表"""
    logger.info(f"执行模型表 {target_table} 的脚本 {script_name}")
    # 检查exec_date是否是JSON字符串
    if isinstance(exec_date, str) and exec_date.startswith('{'):
        try:
            # 尝试解析JSON字符串
            exec_date_data = json.loads(exec_date)
            exec_date = exec_date_data.get("exec_date")
            logger.info(f"从JSON中提取执行日期: {exec_date}")
        except Exception as e:
            logger.error(f"解析exec_date JSON时出错: {str(e)}")
    
    try:
        result = execute_with_monitoring(
            target_table=target_table,
            script_name=script_name,
            script_exec_mode=script_exec_mode,
            exec_date=exec_date
        )
        logger.info(f"模型表 {target_table} 处理完成，结果: {result}")
        return result
    except Exception as e:
        logger.error(f"处理模型表 {target_table} 时出错: {str(e)}")
        # 确保即使出错也返回结果，不会阻塞DAG
        return False

#############################################
# 第三阶段: 汇总阶段(Summary Phase)的函数
#############################################

def get_execution_stats(exec_date):
    """获取当日执行统计信息"""
    conn = get_pg_conn()
    cursor = conn.cursor()
    try:
        # 查询总任务数
        cursor.execute("""
            SELECT COUNT(*) FROM airflow_dag_schedule WHERE exec_date = %s
        """, (exec_date,))
        result = cursor.fetchone()
        total_tasks = result[0] if result else 0
        
        # 查询每种类型的任务数
        cursor.execute("""
            SELECT target_table_label, COUNT(*) 
            FROM airflow_dag_schedule 
            WHERE exec_date = %s 
            GROUP BY target_table_label
        """, (exec_date,))
        type_counts = {row[0]: row[1] for row in cursor.fetchall()}
        
        # 查询执行结果统计
        cursor.execute("""
            SELECT COUNT(*) 
            FROM airflow_dag_schedule 
            WHERE exec_date = %s AND exec_result IS TRUE
        """, (exec_date,))
        result = cursor.fetchone()
        success_count = result[0] if result else 0
        
        cursor.execute("""
            SELECT COUNT(*) 
            FROM airflow_dag_schedule 
            WHERE exec_date = %s AND exec_result IS FALSE
        """, (exec_date,))
        result = cursor.fetchone()
        fail_count = result[0] if result else 0
        
        cursor.execute("""
            SELECT COUNT(*) 
            FROM airflow_dag_schedule 
            WHERE exec_date = %s AND exec_result IS NULL
        """, (exec_date,))
        result = cursor.fetchone()
        pending_count = result[0] if result else 0
        
        # 计算执行时间统计
        cursor.execute("""
            SELECT AVG(exec_duration), MIN(exec_duration), MAX(exec_duration)
            FROM airflow_dag_schedule 
            WHERE exec_date = %s AND exec_duration IS NOT NULL
        """, (exec_date,))
        time_stats = cursor.fetchone()
        
        # 确保时间统计不为None
        if time_stats and time_stats[0] is not None:
            avg_duration = float(time_stats[0])
            min_duration = float(time_stats[1]) if time_stats[1] is not None else None
            max_duration = float(time_stats[2]) if time_stats[2] is not None else None
        else:
            avg_duration = None
            min_duration = None
            max_duration = None
        
        # 查询失败任务详情
        cursor.execute("""
            SELECT target_table, script_name, target_table_label, exec_duration
            FROM airflow_dag_schedule 
            WHERE exec_date = %s AND exec_result IS FALSE
        """, (exec_date,))
        failed_tasks = []
        for row in cursor.fetchall():
            task_dict = {
                "target_table": row[0],
                "script_name": row[1],
                "target_table_label": row[2],
            }
            if row[3] is not None:
                task_dict["exec_duration"] = float(row[3])
            else:
                task_dict["exec_duration"] = None
            failed_tasks.append(task_dict)
        
        # 计算成功率，避免除零错误
        success_rate = 0
        if total_tasks > 0:
            success_rate = (success_count / total_tasks) * 100
        
        # 汇总统计信息
        stats = {
            "exec_date": exec_date,
            "total_tasks": total_tasks,
            "type_counts": type_counts,
            "success_count": success_count,
            "fail_count": fail_count,
            "pending_count": pending_count,
            "success_rate": success_rate,
            "avg_duration": avg_duration,
            "min_duration": min_duration,
            "max_duration": max_duration,
            "failed_tasks": failed_tasks
        }
        
        return stats
    except Exception as e:
        logger.error(f"获取执行统计信息时出错: {str(e)}")
        return {}
    finally:
        cursor.close()
        conn.close()

def update_missing_results(exec_date):
    """更新缺失的执行结果信息"""
    conn = get_pg_conn()
    cursor = conn.cursor()
    try:
        # 查询所有缺失执行结果的任务
        cursor.execute("""
            SELECT target_table, script_name
            FROM airflow_dag_schedule 
            WHERE exec_date = %s AND exec_result IS NULL
        """, (exec_date,))
        missing_results = cursor.fetchall()
        
        update_count = 0
        for row in missing_results:
            target_table, script_name = row
            
            # 如果有开始时间但没有结束时间，假设执行失败
            cursor.execute("""
                SELECT exec_start_time
                FROM airflow_dag_schedule
                WHERE exec_date = %s AND target_table = %s AND script_name = %s
            """, (exec_date, target_table, script_name))
            
            start_time = cursor.fetchone()
            
            if start_time and start_time[0]:
                # 有开始时间但无结果，标记为失败
                now = datetime.now()
                duration = (now - start_time[0]).total_seconds()
                
                cursor.execute("""
                    UPDATE airflow_dag_schedule
                    SET exec_result = FALSE, exec_end_time = %s, exec_duration = %s
                    WHERE exec_date = %s AND target_table = %s AND script_name = %s
                """, (now, duration, exec_date, target_table, script_name))
                
                logger.warning(f"任务 {target_table} 的脚本 {script_name} 标记为失败，开始时间: {start_time[0]}")
                update_count += 1
            else:
                # 没有开始时间且无结果，假设未执行
                logger.warning(f"任务 {target_table} 的脚本 {script_name} 未执行")
        
        conn.commit()
        logger.info(f"更新了 {update_count} 个缺失结果的任务")
        return update_count
    except Exception as e:
        logger.error(f"更新缺失执行结果时出错: {str(e)}")
        conn.rollback()
        return 0
    finally:
        cursor.close()
        conn.close()

def generate_execution_report(exec_date, stats):
    """生成执行报告"""
    # 构建报告
    report = []
    report.append(f"========== 数据运维系统执行报告 ==========")
    report.append(f"执行日期: {exec_date}")
    report.append(f"总任务数: {stats['total_tasks']}")
    
    # 任务类型分布
    report.append("\n--- 任务类型分布 ---")
    for label, count in stats.get('type_counts', {}).items():
        report.append(f"{label} 任务: {count} 个")
    
    # 执行结果统计
    report.append("\n--- 执行结果统计 ---")
    report.append(f"成功任务: {stats.get('success_count', 0)} 个")
    report.append(f"失败任务: {stats.get('fail_count', 0)} 个")
    report.append(f"未执行任务: {stats.get('pending_count', 0)} 个")
    report.append(f"成功率: {stats.get('success_rate', 0):.2f}%")
    
    # 执行时间统计
    report.append("\n--- 执行时间统计 (秒) ---")
    avg_duration = stats.get('avg_duration')
    min_duration = stats.get('min_duration')
    max_duration = stats.get('max_duration')
    
    report.append(f"平均执行时间: {avg_duration:.2f}" if avg_duration is not None else "平均执行时间: N/A")
    report.append(f"最短执行时间: {min_duration:.2f}" if min_duration is not None else "最短执行时间: N/A")
    report.append(f"最长执行时间: {max_duration:.2f}" if max_duration is not None else "最长执行时间: N/A")
    
    # 失败任务详情
    failed_tasks = stats.get('failed_tasks', [])
    if failed_tasks:
        report.append("\n--- 失败任务详情 ---")
        for i, task in enumerate(failed_tasks, 1):
            report.append(f"{i}. 表名: {task['target_table']}")
            report.append(f"   脚本: {task['script_name']}")
            report.append(f"   类型: {task['target_table_label']}")
            exec_duration = task.get('exec_duration')
            if exec_duration is not None:
                report.append(f"   执行时间: {exec_duration:.2f} 秒")
            else:
                report.append("   执行时间: N/A")
    
    report.append("\n========== 报告结束 ==========")
    
    # 将报告转换为字符串
    report_str = "\n".join(report)
    
    # 记录到日志
    logger.info("\n" + report_str)
    
    return report_str

def summarize_execution(**kwargs):
    """汇总执行情况的主函数"""
    try:
        exec_date = kwargs.get('ds') or get_today_date()
        logger.info(f"开始汇总执行日期 {exec_date} 的统一执行情况")
        
        # 1. 更新缺失的执行结果
        try:
            update_count = update_missing_results(exec_date)
            logger.info(f"更新了 {update_count} 个缺失的执行结果")
        except Exception as e:
            logger.error(f"更新缺失执行结果时出错: {str(e)}")
            update_count = 0
        
        # 2. 获取执行统计信息
        try:
            stats = get_execution_stats(exec_date)
            if not stats:
                logger.warning("未能获取执行统计信息，将使用默认值")
                stats = {
                    "exec_date": exec_date,
                    "total_tasks": 0,
                    "type_counts": {},
                    "success_count": 0,
                    "fail_count": 0,
                    "pending_count": 0,
                    "success_rate": 0,
                    "avg_duration": None,
                    "min_duration": None,
                    "max_duration": None,
                    "failed_tasks": []
                }
        except Exception as e:
            logger.error(f"获取执行统计信息时出错: {str(e)}")
            stats = {
                "exec_date": exec_date,
                "total_tasks": 0,
                "type_counts": {},
                "success_count": 0,
                "fail_count": 0,
                "pending_count": 0,
                "success_rate": 0,
                "avg_duration": None,
                "min_duration": None,
                "max_duration": None,
                "failed_tasks": []
            }
        
        # 3. 生成执行报告
        try:
            report = generate_execution_report(exec_date, stats)
        except Exception as e:
            logger.error(f"生成执行报告时出错: {str(e)}")
            report = f"生成执行报告时出错: {str(e)}\n基础统计: 总任务数: {stats.get('total_tasks', 0)}, 成功: {stats.get('success_count', 0)}, 失败: {stats.get('fail_count', 0)}"
        
        # 将报告和统计信息传递给下一个任务
        try:
            kwargs['ti'].xcom_push(key='execution_stats', value=json.dumps(stats, cls=DecimalEncoder))
            kwargs['ti'].xcom_push(key='execution_report', value=report)
        except Exception as e:
            logger.error(f"保存报告到XCom时出错: {str(e)}")
        
        return report
    except Exception as e:
        logger.error(f"汇总执行情况时出现未处理的错误: {str(e)}")
        # 返回一个简单的错误报告，确保任务不会失败
        return f"执行汇总时出现错误: {str(e)}"

# 创建DAG
with DAG(
    "dag_dataops_pipeline_data_scheduler", 
    start_date=datetime(2024, 1, 1), 
    schedule_interval="@daily", 
    catchup=False,
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    # 添加DAG级别参数，确保任务运行时有正确的环境
    params={
        "scripts_path": SCRIPTS_BASE_PATH,
        "airflow_base_path": os.path.dirname(os.path.dirname(__file__))
    }
) as dag:
    
    #############################################
    # 阶段1: 准备阶段(Prepare Phase)
    #############################################
    with TaskGroup("prepare_phase") as prepare_group:
        # 任务开始标记
        start_preparation = EmptyOperator(
            task_id="start_preparation"
        )
        
        # 准备调度任务
        prepare_task = PythonOperator(
            task_id="prepare_dag_schedule",
            python_callable=prepare_dag_schedule,
            provide_context=True
        )
        
        # 验证执行计划有效性
        check_plan = ShortCircuitOperator(
            task_id="check_execution_plan_file",
            python_callable=check_execution_plan_file,
            provide_context=True
        )
        
        # 创建执行计划 
        create_plan = PythonOperator(
            task_id="create_execution_plan",
            python_callable=create_execution_plan,
            provide_context=True
        )
        
        # 准备完成标记
        preparation_completed = EmptyOperator(
            task_id="preparation_completed"
        )
        
        # 设置任务依赖
        start_preparation >> prepare_task >> check_plan >> create_plan >> preparation_completed
    
    #############################################
    # 阶段2: 数据处理阶段(Data Processing Phase)
    #############################################
    with TaskGroup("data_processing_phase") as data_group:
        # 数据处理开始任务
        start_processing = EmptyOperator(
            task_id="start_processing"
        )
        
        # 数据处理完成标记
        processing_completed = EmptyOperator(
            task_id="processing_completed",
            trigger_rule="none_failed_min_one_success"  # 只要有一个任务成功且没有失败的任务就标记为完成
        )
        
        # 设置依赖
        start_processing >> processing_completed
    
    #############################################
    # 阶段3: 汇总阶段(Summary Phase)
    #############################################
    with TaskGroup("summary_phase") as summary_group:
        # 汇总执行情况
        summarize_task = PythonOperator(
            task_id="summarize_execution",
            python_callable=summarize_execution,
            provide_context=True
        )
        
        # 总结完成标记
        summary_completed = EmptyOperator(
            task_id="summary_completed"
        )
        
        # 设置任务依赖
        summarize_task >> summary_completed
    
    # 设置三个阶段之间的依赖关系
    prepare_group >> data_group >> summary_group

    # 尝试从执行计划文件中获取信息 - 这部分在DAG解析时执行
    try:
        # 尝试从文件中读取最新的执行计划，用于构建DAG图
        plan_path = os.path.join(os.path.dirname(__file__), 'last_execution_plan.json')
        ready_path = f"{plan_path}.ready"
        
        if os.path.exists(plan_path) and os.path.exists(ready_path):
            try:
                # 读取ready文件中的时间戳
                with open(ready_path, 'r') as f:
                    ready_timestamp = f.read().strip()
                    logger.info(f"执行计划ready标记时间: {ready_timestamp}")
                
                # 读取执行计划文件
                with open(plan_path, 'r') as f:
                    execution_plan_json = f.read()
                    execution_plan = json.loads(execution_plan_json)
                    logger.info(f"从文件加载执行计划: {plan_path}")
                    
                    # 提取信息
                    exec_date = execution_plan.get("exec_date", get_today_date())
                    resource_tasks = execution_plan.get("resource_tasks", [])
                    model_tasks = execution_plan.get("model_tasks", [])
                    dependencies = execution_plan.get("dependencies", {})
                    
                    logger.info(f"执行计划: exec_date={exec_date}, resource_tasks数量={len(resource_tasks)}, model_tasks数量={len(model_tasks)}")
                    
                    # 动态创建处理任务
                    task_dict = {}
                    
                    # 1. 创建资源表任务
                    for task_info in resource_tasks:
                        table_name = task_info["target_table"]
                        script_name = task_info["script_name"]
                        exec_mode = task_info.get("script_exec_mode", "append")
                        
                        # 创建安全的任务ID
                        safe_table_name = table_name.replace(".", "_").replace("-", "_")
                        
                        # 确保所有任务都是data_processing_phase的一部分
                        with data_group:
                            resource_task = PythonOperator(
                                task_id=f"resource_{safe_table_name}",
                                python_callable=process_resource,
                                op_kwargs={
                                    "target_table": table_name,
                                    "script_name": script_name,
                                    "script_exec_mode": exec_mode,
                                    "exec_date": exec_date
                                },
                                retries=TASK_RETRY_CONFIG["retries"],
                                retry_delay=timedelta(minutes=TASK_RETRY_CONFIG["retry_delay_minutes"])
                            )
                        
                        # 将任务添加到字典
                        task_dict[table_name] = resource_task
                        
                        # 设置与start_processing的依赖
                        start_processing >> resource_task
                    
                    # 创建有向图，用于检测模型表之间的依赖关系
                    G = nx.DiGraph()
                    
                    # 将所有模型表添加为节点
                    for task_info in model_tasks:
                        table_name = task_info["target_table"]
                        G.add_node(table_name)
                    
                    # 添加模型表之间的依赖边
                    for source, deps in dependencies.items():
                        for dep in deps:
                            if dep.get("table_type") == "DataModel" and dep.get("table_name") in G.nodes():
                                G.add_edge(dep.get("table_name"), source)  # 依赖方向：依赖项 -> 目标
                    
                    # 检测循环依赖并处理
                    try:
                        cycles = list(nx.simple_cycles(G))
                        if cycles:
                            logger.warning(f"检测到循环依赖: {cycles}")
                            for cycle in cycles:
                                G.remove_edge(cycle[-1], cycle[0])
                                logger.info(f"打破循环依赖: 移除 {cycle[-1]} -> {cycle[0]} 的依赖")
                    except Exception as e:
                        logger.error(f"检测循环依赖时出错: {str(e)}")
                    
                    # 生成拓扑排序，确定执行顺序
                    execution_order = []
                    try:
                        execution_order = list(nx.topological_sort(G))
                    except Exception as e:
                        logger.error(f"生成拓扑排序失败: {str(e)}")
                        execution_order = [task_info["target_table"] for task_info in model_tasks]
                    
                    # 2. 按拓扑排序顺序创建模型表任务
                    for table_name in execution_order:
                        task_info = next((t for t in model_tasks if t["target_table"] == table_name), None)
                        if not task_info:
                            continue
                            
                        script_name = task_info["script_name"]
                        exec_mode = task_info.get("script_exec_mode", "append")
                        
                        # 创建安全的任务ID
                        safe_table_name = table_name.replace(".", "_").replace("-", "_")
                        
                        # 确保所有任务都是data_processing_phase的一部分
                        with data_group:
                            model_task = PythonOperator(
                                task_id=f"model_{safe_table_name}",
                                python_callable=process_model,
                                op_kwargs={
                                    "target_table": table_name,
                                    "script_name": script_name,
                                    "script_exec_mode": exec_mode,
                                    "exec_date": exec_date
                                },
                                retries=TASK_RETRY_CONFIG["retries"],
                                retry_delay=timedelta(minutes=TASK_RETRY_CONFIG["retry_delay_minutes"])
                            )
                        
                        # 将任务添加到字典
                        task_dict[table_name] = model_task
                        
                        # 设置依赖关系
                        deps = dependencies.get(table_name, [])
                        has_dependency = False
                        
                        # 处理模型表之间的依赖
                        for dep in deps:
                            dep_table = dep.get("table_name")
                            dep_type = dep.get("table_type")
                            
                            if dep_table in task_dict:
                                task_dict[dep_table] >> model_task
                                has_dependency = True
                                logger.info(f"设置依赖: {dep_table} >> {table_name}")
                        
                        # 如果没有依赖，则依赖于start_processing和资源表任务
                        if not has_dependency:
                            # 从start_processing任务直接连接
                            start_processing >> model_task
                            
                            # 同时从所有资源表任务连接
                            resource_count = 0
                            for resource_table in resource_tasks:
                                if resource_count >= 5:  # 最多设置5个依赖
                                    break
                                
                                resource_name = resource_table["target_table"]
                                if resource_name in task_dict:
                                    task_dict[resource_name] >> model_task
                                    resource_count += 1
                    
                    # 找出所有终端任务（没有下游依赖的任务）
                    terminal_tasks = []
                    
                    # 检查所有模型表任务
                    for table_name in execution_order:
                        # 检查是否有下游任务
                        has_downstream = False
                        for source, deps in dependencies.items():
                            if source == table_name:  # 跳过自身
                                continue
                            for dep in deps:
                                if dep.get("table_name") == table_name:
                                    has_downstream = True
                                    break
                            if has_downstream:
                                break
                        
                        # 如果没有下游任务，添加到终端任务列表
                        if not has_downstream and table_name in task_dict:
                            terminal_tasks.append(table_name)
                    
                    # 如果没有模型表任务，将所有资源表任务视为终端任务
                    if not model_tasks and resource_tasks:
                        terminal_tasks = [task["target_table"] for task in resource_tasks]
                        logger.info(f"没有模型表任务，将所有资源表任务视为终端任务: {terminal_tasks}")
                    
                    # 如果既没有模型表任务也没有资源表任务，已有默认依赖链
                    if not terminal_tasks:
                        logger.warning("未找到任何任务，使用默认依赖链")
                    else:
                        # 将所有终端任务连接到完成标记
                        for table_name in terminal_tasks:
                            if table_name in task_dict:
                                task_dict[table_name] >> processing_completed
                                logger.info(f"设置终端任务: {table_name} >> processing_completed")
            
            except Exception as plan_e:
                logger.error(f"解析执行计划文件时出错: {str(plan_e)}")
                import traceback
                logger.error(traceback.format_exc())
        else:
            if not os.path.exists(plan_path):
                logger.warning(f"执行计划文件不存在: {plan_path}")
            if not os.path.exists(ready_path):
                logger.warning(f"执行计划ready标记文件不存在: {ready_path}")
            logger.warning("将使用默认DAG结构")
    except Exception as e:
        logger.error(f"加载执行计划文件时出错: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
logger.info(f"DAG dag_dataops_pipeline_data_scheduler 定义完成")