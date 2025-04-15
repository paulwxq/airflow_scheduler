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
import pendulum
from decimal import Decimal
from common import (
    get_pg_conn, 
    get_neo4j_driver,
    get_today_date
)
from config import TASK_RETRY_CONFIG, SCRIPTS_BASE_PATH, PG_CONFIG, NEO4J_CONFIG
import pytz

# 创建日志记录器
logger = logging.getLogger(__name__)

# 开启详细诊断日志记录
ENABLE_DEBUG_LOGGING = True

def log_debug(message):
    """记录调试日志，但只在启用调试模式时"""
    if ENABLE_DEBUG_LOGGING:
        logger.info(f"[DEBUG] {message}")

# 在DAG启动时输出诊断信息
log_debug("======== 诊断信息 ========")
log_debug(f"当前工作目录: {os.getcwd()}")
log_debug(f"SCRIPTS_BASE_PATH: {SCRIPTS_BASE_PATH}")
log_debug(f"导入的common模块路径: {get_pg_conn.__module__}")

# 检查数据库连接
def validate_database_connection():
    """验证数据库连接是否正常"""
    try:
        conn = get_pg_conn()
        cursor = conn.cursor()
        cursor.execute("SELECT version()")
        version = cursor.fetchone()
        log_debug(f"数据库连接正常，PostgreSQL版本: {version[0]}")
        
        # 检查airflow_exec_plans表是否存在
        cursor.execute("""
            SELECT EXISTS (
               SELECT FROM information_schema.tables 
               WHERE table_name = 'airflow_exec_plans'
            )
        """)
        table_exists = cursor.fetchone()[0]
        if table_exists:
            # 检查表结构
            cursor.execute("""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = 'airflow_exec_plans'
            """)
            columns = cursor.fetchall()
            log_debug(f"airflow_exec_plans表存在，列信息:")
            for col in columns:
                log_debug(f"  - {col[0]}: {col[1]}")
            
            # 查询最新记录数量
            cursor.execute("SELECT COUNT(*) FROM airflow_exec_plans")
            count = cursor.fetchone()[0]
            log_debug(f"airflow_exec_plans表中有 {count} 条记录")
            
            # 检查最近的执行记录
            cursor.execute("""
                SELECT exec_date, COUNT(*) as record_count
                FROM airflow_exec_plans
                GROUP BY exec_date
                ORDER BY exec_date DESC
                LIMIT 3
            """)
            recent_dates = cursor.fetchall()
            log_debug(f"最近的执行日期及记录数:")
            for date_info in recent_dates:
                log_debug(f"  - {date_info[0]}: {date_info[1]} 条记录")
        else:
            log_debug("airflow_exec_plans表不存在！")
        
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        log_debug(f"数据库连接验证失败: {str(e)}")
        import traceback
        log_debug(f"错误堆栈: {traceback.format_exc()}")
        return False

# 执行数据库连接验证
try:
    validate_database_connection()
except Exception as e:
    log_debug(f"验证数据库连接时出错: {str(e)}")

log_debug("======== 诊断信息结束 ========")

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
# 新的工具函数
#############################################

def execute_python_script(target_table, script_name, script_exec_mode, exec_date, **kwargs):
    """
    执行Python脚本并返回执行结果
    
    参数:
        target_table: 目标表名
        script_name: 脚本名称 
        script_exec_mode: 脚本执行模式
        exec_date: 执行日期
        
    返回:
        bool: 脚本执行结果
    """
    # 添加详细日志
    logger.info(f"===== 开始执行脚本 =====")
    logger.info(f"target_table: {target_table}, 类型: {type(target_table)}")
    logger.info(f"script_name: {script_name}, 类型: {type(script_name)}")
    logger.info(f"script_exec_mode: {script_exec_mode}, 类型: {type(script_exec_mode)}")
    logger.info(f"exec_date: {exec_date}, 类型: {type(exec_date)}")

    # 检查script_name是否为空
    if not script_name:
        logger.error(f"表 {target_table} 的script_name为空，无法执行")
        return False
        
    # 记录执行开始时间
    start_time = datetime.now()
    
    try:
        # 导入和执行脚本模块
        import importlib.util
        import sys
        
        script_path = os.path.join(SCRIPTS_BASE_PATH, script_name)
        
        if not os.path.exists(script_path):
            logger.error(f"脚本文件不存在: {script_path}")
            return False
            
        # 动态导入模块
        spec = importlib.util.spec_from_file_location("dynamic_module", script_path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        
        # 检查并调用标准入口函数run
        if hasattr(module, "run"):
            logger.info(f"调用脚本 {script_name} 的标准入口函数 run()")
            result = module.run(table_name=target_table, execution_mode=script_exec_mode)
            logger.info(f"脚本执行完成，原始返回值: {result}, 类型: {type(result)}")
            
            # 确保result是布尔值
            if result is None:
                logger.warning(f"脚本返回值为None，转换为False")
                result = False
            elif not isinstance(result, bool):
                original_result = result
                result = bool(result)
                logger.warning(f"脚本返回非布尔值 {original_result}，转换为布尔值: {result}")
            
            # 记录结束时间和结果
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            logger.info(f"脚本 {script_name} 执行完成，结果: {result}, 耗时: {duration:.2f}秒")
            
            return result
        else:
            logger.error(f"脚本 {script_name} 中未定义标准入口函数 run()，无法执行")
            return False
    except Exception as e:
        # 处理异常
        logger.error(f"执行任务出错: {str(e)}")
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        logger.error(f"脚本 {script_name} 执行失败，耗时: {duration:.2f}秒")
        logger.info(f"===== 脚本执行异常结束 =====")
        import traceback
        logger.error(traceback.format_exc())
        
        # 确保不会阻塞DAG
        return False

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

def prepare_dag_schedule(**kwargs):
    """准备DAG调度任务的主函数"""
    dag_run = kwargs.get('dag_run')
    logical_date = dag_run.logical_date
    local_logical_date = pendulum.instance(logical_date).in_timezone('Asia/Shanghai')
    exec_date = local_logical_date.strftime('%Y-%m-%d')
    
    # 记录重要的时间参数
    logger.info(f"【时间参数】prepare_dag_schedule: exec_date={exec_date}, logical_date={logical_date}, local_logical_date={local_logical_date}")
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
    
    # 已删除对 airflow_dag_schedule 表的写入操作
    # 只记录准备了多少个表
    logger.info(f"处理了 {len(valid_tables)} 个有效表")
    
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
        "logical_date": logical_date,
        "local_logical_date": local_logical_date,
        "resource_tasks": resource_tasks,
        "model_tasks": model_tasks,
        "dependencies": dependencies
    }
    
    # 将执行计划保存到XCom
    kwargs['ti'].xcom_push(key='execution_plan', value=execution_plan)
    logger.info(f"准备了执行计划，包含 {len(resource_tasks)} 个资源表任务和 {len(model_tasks)} 个模型表任务")
    
    return len(valid_tables)

def check_execution_plan(**kwargs):
    """
    检查执行计划是否存在且有效
    返回False将阻止所有下游任务执行
    """
    dag_run = kwargs.get('dag_run')
    logical_date = dag_run.logical_date
    local_logical_date = pendulum.instance(logical_date).in_timezone('Asia/Shanghai')
    exec_date = local_logical_date.strftime('%Y-%m-%d')
    
    # 记录重要的时间参数
    logger.info(f"【时间参数】check_execution_plan: exec_date={exec_date}, logical_date={logical_date}, local_logical_date={local_logical_date}")
    logger.info("检查数据库中的执行计划是否存在且有效")
    
    # 从数据库获取执行计划
    execution_plan = get_execution_plan_from_db(exec_date)
    
    # 检查是否成功获取到执行计划
    if not execution_plan:
        logger.error(f"未找到执行日期 {exec_date} 的执行计划")
        return False
    
    # 检查执行计划是否包含必要字段
    if "exec_date" not in execution_plan:
        logger.error("执行计划缺少exec_date字段")
        return False
        
    if not isinstance(execution_plan.get("resource_tasks", []), list):
        logger.error("执行计划的resource_tasks字段无效")
        return False
        
    if not isinstance(execution_plan.get("model_tasks", []), list):
        logger.error("执行计划的model_tasks字段无效")
        return False
    
    # 检查是否有任务数据
    resource_tasks = execution_plan.get("resource_tasks", [])
    model_tasks = execution_plan.get("model_tasks", [])
    
    if not resource_tasks and not model_tasks:
        logger.warning("执行计划不包含任何任务")
        # 如果没有任务，则阻止下游任务执行
        return False
    
    logger.info(f"执行计划验证成功: 包含 {len(resource_tasks)} 个资源任务和 {len(model_tasks)} 个模型任务")
    return True

#############################################
# 第二阶段: 数据处理阶段(Data Processing Phase)的函数
#############################################

def get_all_tasks(exec_date):
    """
    获取所有需要执行的任务（DataResource和DataModel）
    直接从执行计划获取任务信息，不再查询数据库
    """
    # 从数据库获取执行计划
    execution_plan = get_execution_plan_from_db(exec_date)
    
    if not execution_plan:
        logger.warning(f"未找到执行日期 {exec_date} 的执行计划")
        return [], []
    
    # 提取资源任务和模型任务
    resource_tasks = execution_plan.get("resource_tasks", [])
    model_tasks = execution_plan.get("model_tasks", [])
    
    logger.info(f"获取到 {len(resource_tasks)} 个资源任务和 {len(model_tasks)} 个模型任务")
    return resource_tasks, model_tasks

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
        dag_run = kwargs.get('dag_run')
        logical_date = dag_run.logical_date
        local_logical_date = pendulum.instance(logical_date).in_timezone('Asia/Shanghai')
        exec_date = local_logical_date.strftime('%Y-%m-%d')
        
        # 记录重要的时间参数
        logger.info(f"【时间参数】create_execution_plan: exec_date={exec_date}, logical_date={logical_date}, local_logical_date={local_logical_date}")
        
        # 从XCom获取执行计划
        execution_plan = kwargs['ti'].xcom_pull(task_ids='prepare_phase.prepare_dag_schedule', key='execution_plan')
        
        # 如果找不到执行计划，则从数据库获取
        if not execution_plan:
            # 获取执行日期
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
            
            # 保存执行计划到XCom
            kwargs['ti'].xcom_push(key='execution_plan', value=new_execution_plan)
            logger.info(f"创建新的执行计划，包含 {len(resource_tasks)} 个资源表任务和 {len(model_tasks)} 个模型表任务")
            
            return new_execution_plan
        
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
        
        return empty_plan

def process_resource(target_table, script_name, script_exec_mode, exec_date):
    """处理单个资源表"""
    task_id = f"resource_{target_table}"
    logger.info(f"===== 开始执行 {task_id} =====")
    logger.info(f"执行资源表 {target_table} 的脚本 {script_name}")
    
    # 确保exec_date是字符串
    if not isinstance(exec_date, str):
        exec_date = str(exec_date)
        logger.info(f"将exec_date转换为字符串: {exec_date}")
    
    try:
        # 使用新的函数执行脚本，不依赖数据库
        logger.info(f"调用execute_python_script: target_table={target_table}, script_name={script_name}")
        result = execute_python_script(
            target_table=target_table,
            script_name=script_name,
            script_exec_mode=script_exec_mode,
            exec_date=exec_date
        )
        logger.info(f"资源表 {target_table} 处理完成，结果: {result}")
        return result
    except Exception as e:
        logger.error(f"处理资源表 {target_table} 时出错: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        logger.info(f"===== 结束执行 {task_id} (失败) =====")
        return False
    finally:
        logger.info(f"===== 结束执行 {task_id} =====")

def process_model(target_table, script_name, script_exec_mode, exec_date):
    """处理单个模型表"""
    task_id = f"model_{target_table}"
    logger.info(f"===== 开始执行 {task_id} =====")
    logger.info(f"执行模型表 {target_table} 的脚本 {script_name}")
    
    # 确保exec_date是字符串
    if not isinstance(exec_date, str):
        exec_date = str(exec_date)
        logger.info(f"将exec_date转换为字符串: {exec_date}")
    
    try:
        # 使用新的函数执行脚本，不依赖数据库
        logger.info(f"调用execute_python_script: target_table={target_table}, script_name={script_name}")
        result = execute_python_script(
            target_table=target_table,
            script_name=script_name,
            script_exec_mode=script_exec_mode,
            exec_date=exec_date
        )
        logger.info(f"模型表 {target_table} 处理完成，结果: {result}")
        return result
    except Exception as e:
        logger.error(f"处理模型表 {target_table} 时出错: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        logger.info(f"===== 结束执行 {task_id} (失败) =====")
        return False
    finally:
        logger.info(f"===== 结束执行 {task_id} =====")

#############################################
# 第三阶段: 汇总阶段(Summary Phase)的函数
#############################################

def get_execution_stats(exec_date):
    """
    获取执行统计信息，使用Airflow的API获取执行状态
    不再依赖airflow_dag_schedule表
    """
    from airflow.models import DagRun, TaskInstance
    from airflow.utils.state import State
    from sqlalchemy import desc
    from airflow import settings
    
    # 记录原始输入参数，仅供参考
    logger.debug(f"【执行日期】get_execution_stats接收到 exec_date: {exec_date}, 类型: {type(exec_date)}")
    logger.debug(f"获取执行日期 {exec_date} 的执行统计信息")
    
    # 当前DAG ID
    dag_id = "dag_dataops_pipeline_data_scheduler"
    
    try:
        # 直接查询最近的DAG运行，不依赖于精确的执行日期
        logger.debug("忽略精确的执行日期，直接查询最近的DAG运行")
        session = settings.Session()
        
        # 查询最近的DAG运行，按updated_at降序排序
        dag_runs = session.query(DagRun).filter(
            DagRun.dag_id == dag_id
        ).order_by(desc(DagRun.updated_at)).limit(1).all()
        
        if not dag_runs:
            logger.warning(f"未找到DAG {dag_id} 的任何运行记录")
            session.close()
            return {
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
        
        dag_run = dag_runs[0]
        logical_date = dag_run.logical_date
        local_logical_date = pendulum.instance(logical_date).in_timezone('Asia/Shanghai')
        logger.debug(f"找到最近的DAG运行: logical_date={logical_date}, local_logical_date={local_logical_date}, updated_at={dag_run.updated_at}, state={dag_run.state}")
        
        # 直接查询最近更新的任务实例，不再通过execution_date过滤
        # 只通过dag_id过滤，按更新时间降序排序
        task_instances = session.query(TaskInstance).filter(
            TaskInstance.dag_id == dag_id
        ).order_by(desc(TaskInstance.updated_at)).limit(100).all()  # 获取最近100条记录
        
        # 日志记录找到的任务实例数量
        logger.debug(f"找到 {len(task_instances)} 个最近的任务实例")
        
        # 关闭会话
        session.close()
        
        # 统计任务状态
        total_tasks = len(task_instances)
        success_count = len([ti for ti in task_instances if ti.state == State.SUCCESS])
        fail_count = len([ti for ti in task_instances if ti.state in (State.FAILED, State.UPSTREAM_FAILED)])
        pending_count = total_tasks - success_count - fail_count
        
        # 计算成功率
        success_rate = (success_count / total_tasks * 100) if total_tasks > 0 else 0
        
        # 计算执行时间
        durations = []
        for ti in task_instances:
            if ti.start_date and ti.end_date:
                duration = (ti.end_date - ti.start_date).total_seconds()
                durations.append(duration)
        
        avg_duration = sum(durations) / len(durations) if durations else None
        min_duration = min(durations) if durations else None
        max_duration = max(durations) if durations else None
        
        # 分类统计信息
        type_counts = {
            "resource": len([ti for ti in task_instances if ti.task_id.startswith("resource_")]),
            "model": len([ti for ti in task_instances if ti.task_id.startswith("model_")])
        }
        
        # 获取失败任务详情
        failed_tasks = []
        for ti in task_instances:
            if ti.state in (State.FAILED, State.UPSTREAM_FAILED):
                task_dict = {
                    "task_id": ti.task_id,
                    "state": ti.state,
                }
                
                if ti.start_date and ti.end_date:
                    task_dict["exec_duration"] = (ti.end_date - ti.start_date).total_seconds()
                
                failed_tasks.append(task_dict)
        
        # 汇总统计信息
        stats = {
            "exec_date": exec_date,
            "logical_date": logical_date,
            "local_logical_date": local_logical_date,
            "dag_run_logical_date": dag_run.logical_date,
            "dag_run_updated_at": dag_run.updated_at,
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
        import traceback
        logger.error(traceback.format_exc())
        # 在出错时显式抛出异常，确保任务失败
        raise

def generate_execution_report(exec_date, stats):
    """生成简化的执行报告，只包含基本信息"""
    # 构建简化报告
    report = []
    report.append(f"========== 数据运维系统执行报告 ==========")
    report.append(f"执行日期: {exec_date}")
    report.append(f"总任务数: {stats['total_tasks']}")
    
    # 将报告转换为字符串
    report_str = "\n".join(report)
    
    # 记录到日志
    logger.info("\n" + report_str)
    
    return report_str

def summarize_execution(**kwargs):
    """简化的汇总执行情况函数，只判断整个作业是否成功"""
    dag_run = kwargs.get('dag_run')
    logical_date = dag_run.logical_date
    local_logical_date = pendulum.instance(logical_date).in_timezone('Asia/Shanghai')
    exec_date = local_logical_date.strftime('%Y-%m-%d')
    
    # 记录重要的时间参数
    logger.debug(f"【时间参数】summarize_execution: exec_date={exec_date}, logical_date={logical_date}, local_logical_date={local_logical_date}")
    logger.debug(f"开始汇总执行日期 {exec_date} 的执行情况")
    
    # 获取任务实例对象
    task_instance = kwargs.get('ti')
    dag_id = task_instance.dag_id
    
    # 获取DAG运行状态信息 - 直接查询最近的运行
    from airflow.models import DagRun
    from airflow.utils.state import State
    from sqlalchemy import desc
    from airflow import settings
    
    logger.debug("直接查询最近更新的DAG运行记录，不依赖执行日期")
    session = settings.Session()
    
    # 查询最近的DAG运行，按updated_at降序排序
    dag_runs = session.query(DagRun).filter(
        DagRun.dag_id == dag_id
    ).order_by(desc(DagRun.updated_at)).limit(1).all()
    
    session.close()
    
    if not dag_runs or len(dag_runs) == 0:
        logger.warning(f"未找到DAG {dag_id} 的任何运行记录")
        state = "UNKNOWN"
        success = False
    else:
        # 获取状态
        dag_run = dag_runs[0]  # 取最近更新的DAG运行
        state = dag_run.state
        logger.debug(f"找到最近的DAG运行: logical_date={dag_run.logical_date}, updated_at={dag_run.updated_at}, state={state}")
        logger.debug(f"DAG {dag_id} 的状态为: {state}")
        
        # 判断是否成功
        success = (state == State.SUCCESS)
    
    # 获取更详细的执行统计信息 - 直接调用get_execution_stats而不关心具体日期
    stats = get_execution_stats(exec_date)
    
    # 创建简单的报告
    if success:
        report = f"DAG {dag_id} 在 {exec_date} 的执行成功完成。"
        if stats:
            report += f" 总共有 {stats.get('total_tasks', 0)} 个任务，" \
                      f"其中成功 {stats.get('success_count', 0)} 个，" \
                      f"失败 {stats.get('fail_count', 0)} 个。"
    else:
        report = f"DAG {dag_id} 在 {exec_date} 的执行未成功完成，状态为: {state}。"
        if stats and stats.get('failed_tasks'):
            report += f" 有 {len(stats.get('failed_tasks', []))} 个任务失败。"
    
    # 记录执行结果
    logger.info(report)
    
    # 如果 stats 为空或缺少total_tasks字段，创建一个完整的状态信息
    if not stats or 'total_tasks' not in stats:
        stats = {
            "exec_date": exec_date,
            "logical_date": logical_date,
            "local_logical_date": local_logical_date,
            "total_tasks": 0,
            "type_counts": {},
            "success_count": 0,
            "fail_count": 0,
            "pending_count": 0,
            "success_rate": 0,
            "avg_duration": None,
            "min_duration": None,
            "max_duration": None,
            "failed_tasks": [],
            "success": success,
            "dag_id": dag_id,
            "dag_run_state": state
        }
    else:
        # 添加success状态到stats
        stats["success"] = success
    
    # 将结果推送到XCom
    task_instance.xcom_push(key='execution_stats', value=stats)
    task_instance.xcom_push(key='execution_report', value=report)
    task_instance.xcom_push(key='execution_success', value=success)
    
    # 生成简化的执行报告
    simple_report = generate_execution_report(exec_date, stats)
    
    return simple_report

# 添加新函数，用于从数据库获取执行计划
def get_execution_plan_from_db(ds):
    """
    从数据库airflow_exec_plans表中获取执行计划
    
    参数:
        ds (str): 执行日期，格式为'YYYY-MM-DD'
        
    返回:
        dict: 执行计划字典，如果找不到则返回None
    """
    # 记录输入参数详细信息
    if isinstance(ds, datetime):
        if ds.tzinfo:
            logger.debug(f"【执行日期】get_execution_plan_from_db接收到datetime对象: {ds}, 带时区: {ds.tzinfo}")
        else:
            logger.debug(f"【执行日期】get_execution_plan_from_db接收到datetime对象: {ds}, 无时区")
    else:
        logger.debug(f"【执行日期】get_execution_plan_from_db接收到: {ds}, 类型: {type(ds)}")
    
    logger.info(f"尝试从数据库获取执行日期 {ds} 的执行计划")
    conn = get_pg_conn()
    cursor = conn.cursor()
    execution_plan = None
    
    try:
        # 查询条件a: 当前日期=表的exec_date，如果有多条记录，取insert_time最大的一条
        cursor.execute("""
            SELECT plan, run_id, insert_time
            FROM airflow_exec_plans
            WHERE dag_id = 'dag_dataops_pipeline_prepare_scheduler' AND exec_date = %s
            ORDER BY insert_time DESC
            LIMIT 1
        """, (ds,))
        result = cursor.fetchone()
        
        if result:
            # 获取计划、run_id和insert_time
            plan_json, run_id, insert_time = result
            logger.info(f"找到当前日期 exec_date={ds} 的执行计划记录，run_id: {run_id}, insert_time: {insert_time}")
            
            # 处理plan_json可能已经是dict的情况
            if isinstance(plan_json, dict):
                execution_plan = plan_json
            else:
                execution_plan = json.loads(plan_json)
                
            return execution_plan
        
        # 查询条件b: 找不到当前日期的记录，查找exec_date<当前ds的最新记录
        logger.info(f"未找到当前日期 exec_date={ds} 的执行计划记录，尝试查找历史记录")
        cursor.execute("""
            SELECT plan, run_id, insert_time, exec_date
            FROM airflow_exec_plans
            WHERE dag_id = 'dag_dataops_pipeline_prepare_scheduler' AND exec_date < %s
            ORDER BY exec_date DESC, insert_time DESC
            LIMIT 1
        """, (ds,))
        result = cursor.fetchone()
        
        if result:
            # 获取计划、run_id、insert_time和exec_date
            plan_json, run_id, insert_time, plan_ds = result
            logger.info(f"找到历史执行计划记录，exec_date: {plan_ds}, run_id: {run_id}, insert_time: {insert_time}")
            
            # 处理plan_json可能已经是dict的情况
            if isinstance(plan_json, dict):
                execution_plan = plan_json
            else:
                execution_plan = json.loads(plan_json)
                
            return execution_plan
        
        # 找不到任何执行计划记录
        logger.error(f"在数据库中未找到任何执行计划记录，当前DAG exec_date={ds}")
        return None
        
    except Exception as e:
        logger.error(f"从数据库获取执行计划时出错: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return None
    finally:
        cursor.close()
        conn.close()

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
    
    # 记录DAG实例化时的重要信息
    now = datetime.now()
    now_with_tz = now.replace(tzinfo=pytz.timezone('Asia/Shanghai'))
    default_exec_date = get_today_date()
    logger.info(f"【DAG初始化】当前时间: {now} / {now_with_tz}, 默认执行日期: {default_exec_date}")
    
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
            task_id="check_execution_plan",
            python_callable=check_execution_plan,
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

    # 尝试从数据库获取执行计划
    try:
        # 获取当前DAG的执行日期
        exec_date = get_today_date()  # 使用当天日期作为默认值
        logger.info(f"当前DAG执行日期 ds={exec_date}，尝试从数据库获取执行计划")
        
        # 记录实际使用的执行日期的时区信息和原始格式
        if isinstance(exec_date, datetime):
            logger.info(f"【执行日期详情】类型: datetime, 时区: {exec_date.tzinfo}, 值: {exec_date}")
        else:
            logger.info(f"【执行日期详情】类型: {type(exec_date)}, 值: {exec_date}")
        
        # 从数据库获取执行计划
        execution_plan = get_execution_plan_from_db(exec_date)
        
        # 检查是否成功获取到执行计划
        if execution_plan is None:
            error_msg = f"无法从数据库获取有效的执行计划，当前DAG exec_date={exec_date}"
            logger.error(error_msg)
            # 使用全局变量而不是异常来强制DAG失败
            raise ValueError(error_msg)
        
        # 如果获取到了执行计划，处理它
        logger.info(f"成功从数据库获取执行计划")
        
        # 提取信息
        exec_date = execution_plan.get("exec_date", exec_date)
        resource_tasks = execution_plan.get("resource_tasks", [])
        model_tasks = execution_plan.get("model_tasks", [])
        dependencies = execution_plan.get("dependencies", {})
        
        logger.info(f"执行计划: exec_date={exec_date}, resource_tasks数量={len(resource_tasks)}, model_tasks数量={len(model_tasks)}")
        
        # 如果执行计划为空（没有任务），也应该失败
        if not resource_tasks and not model_tasks:
            error_msg = f"执行计划中没有任何任务，当前DAG exec_date={exec_date}"
            logger.error(error_msg)
            raise ValueError(error_msg)
        
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
                        # 确保使用字符串而不是可能是默认（非字符串）格式的执行日期
                        "exec_date": str(exec_date)
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
                        # 确保使用字符串而不是可能是默认（非字符串）格式的执行日期
                        "exec_date": str(exec_date)
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
    except Exception as e:
        logger.error(f"加载执行计划时出错: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
logger.info(f"DAG dag_dataops_pipeline_data_scheduler 定义完成")