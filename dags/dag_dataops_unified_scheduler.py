# dag_dataops_unified_scheduler.py
# 合并了prepare, data和summary三个DAG的功能
from airflow import DAG
from airflow.operators.python import PythonOperator
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
from config import TASK_RETRY_CONFIG, PG_CONFIG, NEO4J_CONFIG, AIRFLOW_BASE_PATH, SCRIPTS_BASE_PATH

# 创建日志记录器
logger = logging.getLogger(__name__)

# 添加日期序列化器
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
        # 重新抛出异常，确保错误被正确传播
        raise
    finally:
        cursor.close()
        conn.close()

def prepare_dag_schedule(**kwargs):
    """准备调度任务的主函数"""
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
    
    return inserted_count

def create_execution_plan(**kwargs):
    """准备执行计划的函数，使用从prepare_phase传递的数据，并生成JSON文件"""
    try:
        # 从prepare_dag_schedule获取执行计划
        execution_plan_json = kwargs['ti'].xcom_pull(task_ids='prepare_dag_schedule', key='execution_plan')
        
        if not execution_plan_json:
            # 如果没有获取到，可能是因为推送到XCom失败，尝试从数据库获取
            exec_date = kwargs.get('ds') or get_today_date()
            logger.info(f"未从XCom获取到执行计划，尝试从数据库构建。使用执行日期: {exec_date}")
            
            # 获取所有任务
            resource_tasks, model_tasks = get_all_tasks(exec_date)
            
            if not resource_tasks and not model_tasks:
                logger.warning(f"执行日期 {exec_date} 没有找到任务")
                # 创建空执行计划
                execution_plan = {
                    "exec_date": exec_date,
                    "resource_tasks": [],
                    "model_tasks": [],
                    "dependencies": {}
                }
            else:
                # 为所有模型表获取依赖关系
                model_table_names = [task["target_table"] for task in model_tasks]
                dependencies = get_table_dependencies_for_data_phase(model_table_names)
                
                # 创建执行计划
                execution_plan = {
                    "exec_date": exec_date,
                    "resource_tasks": resource_tasks,
                    "model_tasks": model_tasks,
                    "dependencies": dependencies
                }
            
            # 转换为JSON
            execution_plan_json = json.dumps(execution_plan, default=json_serial)
        else:
            # 如果是字符串，解析一下确保格式正确
            if isinstance(execution_plan_json, str):
                execution_plan = json.loads(execution_plan_json)
            else:
                execution_plan = execution_plan_json
                execution_plan_json = json.dumps(execution_plan, default=json_serial)
        
        # 将执行计划保存为JSON文件，使用临时文件确保写入完整
        try:
            import os
            import time
            import tempfile
            from datetime import datetime
            
            # 设置文件路径
            plan_dir = os.path.join(AIRFLOW_BASE_PATH, 'dags')
            plan_path = os.path.join(plan_dir, 'last_execution_plan.json')
            temp_plan_path = os.path.join(plan_dir, f'temp_last_execution_plan_{int(time.time())}.json')
            ready_flag_path = os.path.join(plan_dir, 'last_execution_plan.json.ready')
            
            logger.info(f"=== 开始创建执行计划文件 - 时间戳: {datetime.now().isoformat()} ===")
            logger.info(f"计划目录: {plan_dir}")
            logger.info(f"最终文件路径: {plan_path}")
            logger.info(f"临时文件路径: {temp_plan_path}")
            logger.info(f"就绪标志文件路径: {ready_flag_path}")
            
            # 获取目录中的现有文件
            existing_files = os.listdir(plan_dir)
            plan_related_files = [f for f in existing_files if 'execution_plan' in f or f.endswith('.ready')]
            logger.info(f"创建前目录中相关文件数: {len(plan_related_files)}")
            for f in plan_related_files:
                file_path = os.path.join(plan_dir, f)
                file_size = os.path.getsize(file_path) if os.path.exists(file_path) else 0
                file_time = datetime.fromtimestamp(os.path.getmtime(file_path)).isoformat() if os.path.exists(file_path) else 'unknown'
                logger.info(f"已存在文件: {f} (大小: {file_size}字节, 修改时间: {file_time})")
            
            # 首先写入临时文件
            with open(temp_plan_path, 'w') as f:
                if isinstance(execution_plan_json, str):
                    f.write(execution_plan_json)
                else:
                    json.dump(execution_plan_json, f, indent=2, default=json_serial)
                f.flush()
                os.fsync(f.fileno())  # 确保写入磁盘
            
            # 验证临时文件
            temp_size = os.path.getsize(temp_plan_path)
            temp_time = datetime.fromtimestamp(os.path.getmtime(temp_plan_path)).isoformat()
            logger.info(f"已创建临时文件: {temp_plan_path} (大小: {temp_size}字节, 修改时间: {temp_time})")
            
            with open(temp_plan_path, 'r') as f:
                test_content = json.load(f)  # 测试是否能正确读取
                logger.info(f"临时文件验证成功，内容可正确解析为JSON")
            
            # 重命名为正式文件
            if os.path.exists(plan_path):
                old_size = os.path.getsize(plan_path)
                old_time = datetime.fromtimestamp(os.path.getmtime(plan_path)).isoformat()
                logger.info(f"删除已有文件: {plan_path} (大小: {old_size}字节, 修改时间: {old_time})")
                os.remove(plan_path)  # 先删除已有文件
            
            logger.info(f"重命名临时文件: {temp_plan_path} -> {plan_path}")
            os.rename(temp_plan_path, plan_path)
            
            # 确认正式文件
            if os.path.exists(plan_path):
                final_size = os.path.getsize(plan_path)
                final_time = datetime.fromtimestamp(os.path.getmtime(plan_path)).isoformat()
                logger.info(f"正式文件创建成功: {plan_path} (大小: {final_size}字节, 修改时间: {final_time})")
            else:
                logger.error(f"正式文件未成功创建: {plan_path}")
            
            # 写入就绪标志文件
            with open(ready_flag_path, 'w') as f:
                flag_content = f"Generated at {datetime.now().isoformat()}"
                f.write(flag_content)
                f.flush()
                os.fsync(f.fileno())  # 确保写入磁盘
            
            # 确认就绪标志文件
            if os.path.exists(ready_flag_path):
                flag_size = os.path.getsize(ready_flag_path)
                flag_time = datetime.fromtimestamp(os.path.getmtime(ready_flag_path)).isoformat()
                logger.info(f"就绪标志文件创建成功: {ready_flag_path} (大小: {flag_size}字节, 修改时间: {flag_time}, 内容: {flag_content})")
            else:
                logger.error(f"就绪标志文件未成功创建: {ready_flag_path}")
            
            # 再次检查目录
            final_files = os.listdir(plan_dir)
            final_plan_files = [f for f in final_files if 'execution_plan' in f or f.endswith('.ready')]
            logger.info(f"创建完成后目录中相关文件数: {len(final_plan_files)}")
            for f in final_plan_files:
                file_path = os.path.join(plan_dir, f)
                file_size = os.path.getsize(file_path) if os.path.exists(file_path) else 0
                file_time = datetime.fromtimestamp(os.path.getmtime(file_path)).isoformat() if os.path.exists(file_path) else 'unknown'
                logger.info(f"最终文件: {f} (大小: {file_size}字节, 修改时间: {file_time})")
            
            logger.info(f"=== 执行计划文件创建完成 - 时间戳: {datetime.now().isoformat()} ===")
        except Exception as e:
            logger.error(f"保存执行计划到文件时出错: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            raise  # 抛出异常，确保任务失败
        
        return execution_plan_json
    except Exception as e:
        logger.error(f"创建执行计划时出错: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        raise  # 抛出异常，确保任务失败

def bridge_prepare_to_data_func(**kwargs):
    """桥接prepare和data阶段，确保执行计划文件已就绪"""
    import os
    import time
    from datetime import datetime
    
    logger.info(f"=== 开始验证执行计划文件 - 时间戳: {datetime.now().isoformat()} ===")
    
    plan_dir = os.path.join(AIRFLOW_BASE_PATH, 'dags')
    plan_path = os.path.join(plan_dir, 'last_execution_plan.json')
    ready_flag_path = os.path.join(plan_dir, 'last_execution_plan.json.ready')
    
    logger.info(f"计划目录: {plan_dir}")
    logger.info(f"计划文件路径: {plan_path}")
    logger.info(f"就绪标志文件路径: {ready_flag_path}")
    
    # 获取目录中的文件列表
    all_files = os.listdir(plan_dir)
    related_files = [f for f in all_files if 'execution_plan' in f or f.endswith('.ready')]
    logger.info(f"目录中的相关文件总数: {len(related_files)}")
    for idx, file in enumerate(related_files, 1):
        file_path = os.path.join(plan_dir, file)
        file_size = os.path.getsize(file_path) if os.path.exists(file_path) else 0
        file_time = datetime.fromtimestamp(os.path.getmtime(file_path)).isoformat() if os.path.exists(file_path) else 'unknown'
        logger.info(f"相关文件{idx}: {file} (大小: {file_size}字节, 修改时间: {file_time})")
    
    # 等待就绪标志文件出现
    logger.info(f"开始等待就绪标志文件: {ready_flag_path}")
    waiting_start = datetime.now()
    max_attempts = 30  # 最多等待5分钟
    for attempt in range(max_attempts):
        if os.path.exists(ready_flag_path):
            wait_duration = (datetime.now() - waiting_start).total_seconds()
            file_size = os.path.getsize(ready_flag_path)
            file_time = datetime.fromtimestamp(os.path.getmtime(ready_flag_path)).isoformat()
            
            # 读取就绪文件内容
            try:
                with open(ready_flag_path, 'r') as f:
                    ready_content = f.read()
            except Exception as e:
                ready_content = f"[读取错误: {str(e)}]"
            
            logger.info(f"发现执行计划就绪标志: {ready_flag_path} (尝试次数: {attempt+1}, 等待时间: {wait_duration:.2f}秒, 大小: {file_size}字节, 修改时间: {file_time}, 内容: {ready_content})")
            break
        
        logger.info(f"等待执行计划就绪 (尝试: {attempt+1}/{max_attempts}, 已等待: {(datetime.now() - waiting_start).total_seconds():.2f}秒)...")
        time.sleep(10)  # 等待10秒
    
    if not os.path.exists(ready_flag_path):
        error_msg = f"执行计划就绪标志文件不存在: {ready_flag_path}，等待超时 (等待时间: {(datetime.now() - waiting_start).total_seconds():.2f}秒)"
        logger.error(error_msg)
        raise Exception(error_msg)
    
    # 验证执行计划文件
    logger.info(f"开始验证执行计划文件: {plan_path}")
    if not os.path.exists(plan_path):
        error_msg = f"执行计划文件不存在: {plan_path}"
        logger.error(error_msg)
        raise Exception(error_msg)
    
    try:
        file_size = os.path.getsize(plan_path)
        file_time = datetime.fromtimestamp(os.path.getmtime(plan_path)).isoformat()
        logger.info(f"准备读取执行计划文件: {plan_path} (大小: {file_size}字节, 修改时间: {file_time})")
        
        with open(plan_path, 'r') as f:
            execution_plan = json.load(f)
            logger.info(f"成功读取并解析执行计划文件 JSON 内容")
        
        # 验证基本结构
        if not isinstance(execution_plan, dict):
            logger.error(f"执行计划格式错误: 不是有效的字典，而是 {type(execution_plan)}")
            raise ValueError("执行计划不是有效的字典")
        else:
            logger.info(f"执行计划基本结构验证: 是有效的字典对象")
        
        # 验证关键字段
        required_fields = ["exec_date", "resource_tasks", "model_tasks"]
        missing_fields = [field for field in required_fields if field not in execution_plan]
        
        if missing_fields:
            error_msg = f"执行计划缺少必要字段: {missing_fields}"
            logger.error(error_msg)
            raise ValueError(error_msg)
        else:
            logger.info(f"执行计划必要字段验证通过: 包含所有必要字段 {required_fields}")
        
        # 记录执行计划基本信息
        resource_tasks = execution_plan.get("resource_tasks", [])
        model_tasks = execution_plan.get("model_tasks", [])
        exec_date = execution_plan.get("exec_date", "未知")
        
        logger.info(f"执行计划内容摘要: 日期={exec_date}, 资源任务数={len(resource_tasks)}, 模型任务数={len(model_tasks)}")
        
        # 如果任务很少，记录具体内容
        if len(resource_tasks) + len(model_tasks) < 10:
            for idx, task in enumerate(resource_tasks, 1):
                logger.info(f"资源任务{idx}: 表={task.get('target_table')}, 脚本={task.get('script_name')}")
            
            for idx, task in enumerate(model_tasks, 1):
                logger.info(f"模型任务{idx}: 表={task.get('target_table')}, 脚本={task.get('script_name')}")
        
        # 如果没有任何任务，发出警告
        if not resource_tasks and not model_tasks:
            logger.warning(f"执行计划不包含任何任务，可能导致数据处理阶段没有实际工作")
        
        logger.info(f"=== 执行计划文件验证成功 - 时间戳: {datetime.now().isoformat()} ===")
        return True
    except Exception as e:
        error_msg = f"验证执行计划文件时出错: {str(e)}"
        logger.error(error_msg)
        import traceback
        logger.error(traceback.format_exc())
        logger.info(f"=== 执行计划文件验证失败 - 时间戳: {datetime.now().isoformat()} ===")
        raise Exception(error_msg)

def init_data_processing_phase(**kwargs):
    """数据处理阶段的初始化函数，重新加载执行计划文件"""
    import os
    from datetime import datetime
    
    logger.info(f"=== 开始数据处理阶段初始化 - 时间戳: {datetime.now().isoformat()} ===")
    
    plan_dir = os.path.join(AIRFLOW_BASE_PATH, 'dags')
    plan_path = os.path.join(plan_dir, 'last_execution_plan.json')
    ready_flag_path = os.path.join(plan_dir, 'last_execution_plan.json.ready')
    
    logger.info(f"计划目录: {plan_dir}")
    logger.info(f"计划文件路径: {plan_path}")
    logger.info(f"就绪标志文件路径: {ready_flag_path}")
    
    # 检查目录中的文件
    all_files = os.listdir(plan_dir)
    related_files = [f for f in all_files if 'execution_plan' in f or f.endswith('.ready')]
    logger.info(f"目录中的相关文件总数: {len(related_files)}")
    for idx, file in enumerate(related_files, 1):
        file_path = os.path.join(plan_dir, file)
        file_size = os.path.getsize(file_path) if os.path.exists(file_path) else 0
        file_time = datetime.fromtimestamp(os.path.getmtime(file_path)).isoformat() if os.path.exists(file_path) else 'unknown'
        logger.info(f"相关文件{idx}: {file} (大小: {file_size}字节, 修改时间: {file_time})")
    
    # 验证文件是否存在
    if not os.path.exists(plan_path):
        error_msg = f"执行计划文件不存在: {plan_path}"
        logger.error(error_msg)
        logger.info(f"=== 数据处理阶段初始化失败 - 时间戳: {datetime.now().isoformat()} ===")
        raise Exception(error_msg)
    
    file_size = os.path.getsize(plan_path)
    file_time = datetime.fromtimestamp(os.path.getmtime(plan_path)).isoformat()
    logger.info(f"准备读取执行计划文件: {plan_path} (大小: {file_size}字节, 修改时间: {file_time})")
    
    try:
        # 记录读取开始时间
        read_start = datetime.now()
        
        with open(plan_path, 'r') as f:
            file_content = f.read()
            logger.info(f"成功读取文件内容，大小为 {len(file_content)} 字节")
            
            # 解析JSON
            parse_start = datetime.now()
            execution_plan = json.loads(file_content)
            parse_duration = (datetime.now() - parse_start).total_seconds()
            logger.info(f"成功解析JSON内容，耗时 {parse_duration:.4f} 秒")
        
        read_duration = (datetime.now() - read_start).total_seconds()
        logger.info(f"文件读取和解析总耗时: {read_duration:.4f} 秒")
        
        # 验证执行计划基本结构
        if not isinstance(execution_plan, dict):
            error_msg = f"执行计划不是有效的字典，实际类型: {type(execution_plan)}"
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        # 存储到XCom中，以便后续任务使用
        push_start = datetime.now()
        
        # 先序列化为JSON字符串
        execution_plan_json = json.dumps(execution_plan, default=json_serial)
        logger.info(f"序列化执行计划为JSON字符串，大小为 {len(execution_plan_json)} 字节")
        
        # 推送到XCom
        kwargs['ti'].xcom_push(key='data_phase_execution_plan', value=execution_plan_json)
        push_duration = (datetime.now() - push_start).total_seconds()
        logger.info(f"成功推送执行计划到XCom，耗时 {push_duration:.4f} 秒")
        
        # 记录执行计划基本信息
        resource_tasks = execution_plan.get("resource_tasks", [])
        model_tasks = execution_plan.get("model_tasks", [])
        exec_date = execution_plan.get("exec_date", "未知")
        
        logger.info(f"执行计划内容摘要: 日期={exec_date}, 资源任务数={len(resource_tasks)}, 模型任务数={len(model_tasks)}")
        
        # 如果任务较少，记录详细信息
        if len(resource_tasks) + len(model_tasks) < 10:
            for idx, task in enumerate(resource_tasks, 1):
                logger.info(f"资源任务{idx}: 表={task.get('target_table')}, 脚本={task.get('script_name')}")
            
            for idx, task in enumerate(model_tasks, 1):
                logger.info(f"模型任务{idx}: 表={task.get('target_table')}, 脚本={task.get('script_name')}")
        
        result = {
            "exec_date": exec_date,
            "resource_count": len(resource_tasks),
            "model_count": len(model_tasks)
        }
        
        logger.info(f"=== 数据处理阶段初始化完成 - 时间戳: {datetime.now().isoformat()} ===")
        return result
    except json.JSONDecodeError as e:
        error_msg = f"执行计划文件JSON解析失败: {str(e)}"
        logger.error(error_msg)
        
        # 记录文件内容摘要以帮助调试
        try:
            with open(plan_path, 'r') as f:
                content = f.read(1000)  # 只读取前1000个字符
                logger.error(f"文件内容前1000个字符: {content}...")
        except Exception as read_error:
            logger.error(f"尝试读取文件内容时出错: {str(read_error)}")
        
        import traceback
        logger.error(traceback.format_exc())
        logger.info(f"=== 数据处理阶段初始化失败 - 时间戳: {datetime.now().isoformat()} ===")
        raise Exception(error_msg)
    except Exception as e:
        error_msg = f"数据处理阶段初始化失败: {str(e)}"
        logger.error(error_msg)
        import traceback
        logger.error(traceback.format_exc())
        logger.info(f"=== 数据处理阶段初始化失败 - 时间戳: {datetime.now().isoformat()} ===")
        raise Exception(error_msg)

#############################################
# 第二阶段: 数据处理阶段(Data Processing Phase)
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
        # 查询所有资源表任务
        cursor.execute("""
            SELECT source_table, target_table, target_table_label, script_name, script_exec_mode
            FROM airflow_dag_schedule 
            WHERE exec_date = %s AND target_table_label = 'DataResource' AND script_name IS NOT NULL
        """, (exec_date,))
        resource_results = cursor.fetchall()
        
        # 查询所有模型表任务
        cursor.execute("""
            SELECT source_table, target_table, target_table_label, script_name, script_exec_mode
            FROM airflow_dag_schedule 
            WHERE exec_date = %s AND target_table_label = 'DataModel' AND script_name IS NOT NULL
        """, (exec_date,))
        model_results = cursor.fetchall()
        
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

def get_table_dependencies_for_data_phase(table_names):
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
        # 直接调用执行监控函数，确保脚本得到执行
        result = execute_with_monitoring(
            target_table=target_table,
            script_name=script_name,
            script_exec_mode=script_exec_mode,
            exec_date=exec_date
        )
        logger.info(f"资源表 {target_table} 处理完成，结果: {result}")
        return f"处理资源表 {target_table} 完成，结果: {result}"
    except Exception as e:
        logger.error(f"处理资源表 {target_table} 时出错: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        # 返回错误信息，但不抛出异常，确保DAG可以继续执行
        return f"处理资源表 {target_table} 失败: {str(e)}"

def execute_with_monitoring(target_table, script_name, script_exec_mode, exec_date, **kwargs):
    """执行脚本并监控执行情况"""
    from pathlib import Path
    import importlib.util
    import sys
    
    logger.info(f"=== 开始执行任务 {target_table} 的脚本 {script_name} - 时间戳: {datetime.now().isoformat()} ===")

    # 检查script_name是否为空
    if not script_name:
        logger.error(f"表 {target_table} 的script_name为空，无法执行")
        # 记录执行失败到数据库
        now = datetime.now()
        update_task_completion(exec_date, target_table, script_name or "", False, now, 0)
        return False
    
    # 记录执行开始时间
    start_time = datetime.now()
    update_task_start_time(exec_date, target_table, script_name, start_time)
    logger.info(f"任务开始时间: {start_time.isoformat()}")
    
    try:
        # 执行实际脚本
        script_path = Path(SCRIPTS_BASE_PATH) / script_name
        logger.info(f"脚本完整路径: {script_path}")
        
        if not script_path.exists():
            logger.error(f"脚本文件不存在: {script_path}")
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            update_task_completion(exec_date, target_table, script_name, False, end_time, duration)
            return False
        
        try:
            # 动态导入模块
            module_name = f"dynamic_module_{abs(hash(script_name))}"
            spec = importlib.util.spec_from_file_location(module_name, script_path)
            module = importlib.util.module_from_spec(spec)
            sys.modules[module_name] = module
            spec.loader.exec_module(module)
            
            # 使用标准入口函数run
            if hasattr(module, "run"):
                logger.info(f"执行脚本 {script_name} 的标准入口函数 run()")
                result = module.run(table_name=target_table, execution_mode=script_exec_mode)
                logger.info(f"脚本 {script_name} 执行结果: {result}")
                success = True if result else False
            else:
                logger.warning(f"脚本 {script_name} 未定义标准入口函数 run()，尝试使用main函数")
                if hasattr(module, "main"):
                    logger.info(f"执行脚本 {script_name} 的main函数")
                    result = module.main(table_name=target_table, execution_mode=script_exec_mode)
                    logger.info(f"脚本 {script_name} 执行结果: {result}")
                    success = True if result else False
                else:
                    logger.error(f"脚本 {script_name} 没有定义标准入口函数 run() 或 main()")
                    success = False
        except Exception as script_e:
            logger.error(f"执行脚本 {script_name} 时出错: {str(script_e)}")
            import traceback
            logger.error(traceback.format_exc())
            success = False
        
        # 记录结束时间和结果
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        update_task_completion(exec_date, target_table, script_name, success, end_time, duration)
        
        logger.info(f"任务结束时间: {end_time.isoformat()}, 执行时长: {duration:.2f}秒, 结果: {success}")
        logger.info(f"=== 完成执行任务 {target_table} 的脚本 {script_name} - 时间戳: {datetime.now().isoformat()} ===")
        
        return success
    except Exception as e:
        # 处理异常
        logger.error(f"执行任务出错: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        update_task_completion(exec_date, target_table, script_name, False, end_time, duration)
        
        logger.info(f"=== 执行任务 {target_table} 的脚本 {script_name} 失败 - 时间戳: {datetime.now().isoformat()} ===")
        return False

def update_task_start_time(exec_date, target_table, script_name, start_time):
    """更新任务开始时间"""
    conn = get_pg_conn()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            UPDATE airflow_dag_schedule 
            SET exec_start_time = %s
            WHERE exec_date = %s AND target_table = %s AND script_name = %s
        """, (start_time, exec_date, target_table, script_name))
        conn.commit()
        logger.info(f"已更新表 {target_table} 的脚本 {script_name} 的开始时间: {start_time}")
    except Exception as e:
        logger.error(f"更新任务开始时间失败: {str(e)}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

def update_task_completion(exec_date, target_table, script_name, success, end_time, duration):
    """更新任务完成信息"""
    conn = get_pg_conn()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            UPDATE airflow_dag_schedule 
            SET exec_result = %s, exec_end_time = %s, exec_duration = %s
            WHERE exec_date = %s AND target_table = %s AND script_name = %s
        """, (success, end_time, duration, exec_date, target_table, script_name))
        conn.commit()
        logger.info(f"已更新表 {target_table} 的脚本 {script_name} 的完成状态: 结果={success}, 结束时间={end_time}, 耗时={duration}秒")
    except Exception as e:
        logger.error(f"更新任务完成信息失败: {str(e)}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

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
        # 直接调用执行监控函数，确保脚本得到执行
        result = execute_with_monitoring(
            target_table=target_table,
            script_name=script_name,
            script_exec_mode=script_exec_mode,
            exec_date=exec_date
        )
        logger.info(f"模型表 {target_table} 处理完成，结果: {result}")
        return f"处理模型表 {target_table} 完成，结果: {result}"
    except Exception as e:
        logger.error(f"处理模型表 {target_table} 时出错: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        # 返回错误信息，但不抛出异常，确保DAG可以继续执行
        return f"处理模型表 {target_table} 失败: {str(e)}"

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

def generate_unified_execution_report(exec_date, stats):
    """生成统一执行报告"""
    # 构建报告
    report = []
    report.append(f"========== 统一数据运维系统执行报告 ==========")
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

def summarize_execution(**context):
    """
    汇总执行计划的执行情况，生成报告
    """
    logger.info(f"=== 开始汇总执行情况 - 时间戳: {datetime.now().isoformat()} ===")
    try:
        # 获取执行日期
        execution_date = context.get('execution_date', datetime.now())
        exec_date = execution_date.strftime('%Y-%m-%d')
        
        # 从本地文件加载执行计划
        plan_dir = os.path.join(AIRFLOW_BASE_PATH, 'dags')
        plan_path = os.path.join(plan_dir, 'last_execution_plan.json')
        
        if not os.path.exists(plan_path):
            logger.warning(f"执行计划文件不存在: {plan_path}")
            return "执行计划文件不存在，无法生成汇总报告"
        
        with open(plan_path, 'r') as f:
            execution_plan = json.loads(f.read())
        
        # 获取任务列表
        resource_tasks = execution_plan.get("resource_tasks", [])
        model_tasks = execution_plan.get("model_tasks", [])
        all_tasks = resource_tasks + model_tasks
        
        # 连接数据库，获取任务执行状态
        conn = get_pg_conn()
        cursor = conn.cursor()
        
        # 分析任务执行状态
        successful_tasks = []
        failed_tasks = []
        skipped_tasks = []
        
        for task in all_tasks:
            table_name = task["target_table"]
            table_type = "资源表" if task in resource_tasks else "模型表"
            
            # 查询任务执行状态
            cursor.execute("""
                SELECT status FROM airflow_task_execution 
                WHERE table_name = %s AND exec_date = %s
                ORDER BY execution_time DESC LIMIT 1
            """, (table_name, exec_date))
            
            result = cursor.fetchone()
            status = result[0] if result else "未执行"
            
            task_info = {
                "table_name": table_name,
                "table_type": table_type,
                "script_name": task["script_name"],
                "status": status
            }
            
            if status == "成功":
                successful_tasks.append(task_info)
            elif status == "失败":
                failed_tasks.append(task_info)
            else:
                skipped_tasks.append(task_info)
        
        # 生成汇总报告
        total_tasks = len(all_tasks)
        success_count = len(successful_tasks)
        fail_count = len(failed_tasks)
        skip_count = len(skipped_tasks)
        
        summary = f"""
执行日期: {exec_date}
总任务数: {total_tasks}
成功任务数: {success_count}
失败任务数: {fail_count}
跳过任务数: {skip_count}

=== 成功任务 ===
"""
        
        for task in successful_tasks:
            summary += f"{task['table_type']} {task['table_name']} ({task['script_name']})\n"
        
        if failed_tasks:
            summary += "\n=== 失败任务 ===\n"
            for task in failed_tasks:
                summary += f"{task['table_type']} {task['table_name']} ({task['script_name']})\n"
        
        if skipped_tasks:
            summary += "\n=== 跳过任务 ===\n"
            for task in skipped_tasks:
                summary += f"{task['table_type']} {task['table_name']} ({task['script_name']})\n"
        
        # 更新汇总表
        cursor.execute("""
            INSERT INTO airflow_execution_summary
            (exec_date, total_tasks, success_count, fail_count, skip_count, summary_text, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (exec_date) 
            DO UPDATE SET
                total_tasks = EXCLUDED.total_tasks,
                success_count = EXCLUDED.success_count,
                fail_count = EXCLUDED.fail_count,
                skip_count = EXCLUDED.skip_count,
                summary_text = EXCLUDED.summary_text,
                updated_at = CURRENT_TIMESTAMP
        """, (
            exec_date, total_tasks, success_count, fail_count, skip_count, 
            summary, datetime.now()
        ))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        logger.info(f"=== 执行情况汇总完成 - 时间戳: {datetime.now().isoformat()} ===")
        return summary
    except Exception as e:
        logger.error(f"汇总执行情况时出现未处理的错误: {str(e)}")
        # 返回一个简单的错误报告，确保任务不会失败
        return f"执行汇总时出现错误: {str(e)}"


# 创建DAG
with DAG(
    "dag_dataops_unified_scheduler", 
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
    }
) as dag:
    
    # 初始化全局变量，避免在DAG解析时出现未定义错误
    globals()['_resource_tasks'] = []
    globals()['_task_dict'] = {}
    
    # DAG开始任务
    dag_start = EmptyOperator(task_id="dag_start")
    
    # DAG结束任务
    dag_end = EmptyOperator(
        task_id="dag_end",
        trigger_rule="all_done"  # 确保DAG无论上游任务成功与否都能完成
    )
    
    # 准备阶段任务
    prepare_task = PythonOperator(
        task_id="prepare_dag_schedule",
        python_callable=prepare_dag_schedule,
        provide_context=True
    )
    
    # 汇总执行情况任务
    summarize_task = PythonOperator(
        task_id='summarize_execution',
        python_callable=summarize_execution,
        provide_context=True,
        trigger_rule='all_done',  # 无论之前的任务成功还是失败都执行
        retries=2,  # 增加重试次数
        retry_delay=timedelta(minutes=1)  # 重试延迟
    )
    
    # 数据处理阶段
    # 获取所有需要执行的任务（实际任务，不是TaskGroup包装的任务）
    exec_date = get_latest_date()
    resource_tasks, model_tasks = get_all_tasks(exec_date)
    
    # 创建任务字典，用于设置依赖关系
    task_dict = {}
    
    # 创建资源表任务
    for task_info in resource_tasks:
        table_name = task_info["target_table"]
        script_name = task_info["script_name"]
        exec_mode = task_info.get("script_exec_mode", "append")
        
        # 创建安全的任务ID
        safe_table_name = table_name.replace(".", "_").replace("-", "_")
        task_id = f"resource_{safe_table_name}"
        
        # 直接使用 execute_with_monitoring 函数，确保执行脚本
        resource_task = PythonOperator(
            task_id=task_id,
            python_callable=execute_with_monitoring,
            op_kwargs={
                "target_table": table_name,
                "script_name": script_name,
                "script_exec_mode": exec_mode,
                "exec_date": exec_date
            },
            retries=2,
            retry_delay=timedelta(minutes=1),
            trigger_rule="all_done"  # 确保无论上游任务成功或失败都会执行
        )
        
        # 将任务添加到字典
        task_dict[table_name] = resource_task
        
        # 设置依赖关系：prepare_task -> resource_task
        prepare_task >> resource_task
        
    # 为所有模型表获取依赖关系
    model_table_names = [task["target_table"] for task in model_tasks]
    dependencies = get_table_dependencies_for_data_phase(model_table_names)
    
    # 创建有向图，用于确定执行顺序
    G = nx.DiGraph()
    
    # 将所有模型表添加为节点
    for task_info in model_tasks:
        G.add_node(task_info["target_table"])
    
    # 添加依赖边
    for source, deps in dependencies.items():
        for dep in deps:
            if dep.get("table_type") == "DataModel" and dep.get("table_name") in G.nodes():
                G.add_edge(dep.get("table_name"), source)
    
    # 处理循环依赖
    cycles = list(nx.simple_cycles(G))
    if cycles:
        for cycle in cycles:
            G.remove_edge(cycle[-1], cycle[0])
    
    # 获取执行顺序
    try:
        execution_order = list(nx.topological_sort(G))
    except Exception as e:
        execution_order = [task["target_table"] for task in model_tasks]
    
    # 创建模型表任务
    for table_name in execution_order:
        task_info = next((t for t in model_tasks if t["target_table"] == table_name), None)
        if not task_info:
            continue
        
        script_name = task_info["script_name"]
        exec_mode = task_info.get("script_exec_mode", "append")
        
        # 创建安全的任务ID
        safe_table_name = table_name.replace(".", "_").replace("-", "_")
        task_id = f"model_{safe_table_name}"
        
        # 直接使用 execute_with_monitoring 函数执行脚本
        model_task = PythonOperator(
            task_id=task_id,
            python_callable=execute_with_monitoring,
            op_kwargs={
                "target_table": table_name,
                "script_name": script_name,
                "script_exec_mode": exec_mode,
                "exec_date": exec_date
            },
            retries=2,
            retry_delay=timedelta(minutes=1),
            trigger_rule="all_done"  # 确保无论上游任务成功或失败都会执行
        )
        
        # 将任务添加到字典
        task_dict[table_name] = model_task
        
        # 设置依赖关系
        deps = dependencies.get(table_name, [])
        has_dependency = False
        
        # 处理模型表之间的依赖
        for dep in deps:
            dep_table = dep.get("table_name")
            if dep_table in task_dict:
                task_dict[dep_table] >> model_task
                has_dependency = True
        
        # 如果没有依赖，则依赖于所有资源表任务
        if not has_dependency and resource_tasks:
            for resource_task_info in resource_tasks:
                resource_name = resource_task_info["target_table"]
                if resource_name in task_dict:
                    task_dict[resource_name] >> model_task
        
        # 如果没有依赖，也没有资源表，则直接依赖于prepare_task
        if not has_dependency and not resource_tasks:
            prepare_task >> model_task
    
    # 所有处理任务都是summarize_task的上游
    for task in task_dict.values():
        task >> summarize_task
    
    # 设置主要流程
    dag_start >> prepare_task
    
    # 创建执行计划文件任务
    create_plan_task = PythonOperator(
        task_id="create_execution_plan",
        python_callable=create_execution_plan,
        provide_context=True
    )
    
    # 设置依赖关系
    prepare_task >> create_plan_task >> summarize_task >> dag_end
    
    logger.info(f"DAG dag_dataops_unified_scheduler 定义完成，创建了 {len(task_dict)} 个脚本执行任务")
    
    # 尝试从数据库获取最新的执行计划，用于WebUI展示
    try:
        # 使用一个只在DAG加载时执行一次的简单查询来获取表信息
        # 这只用于UI展示，不影响实际执行
        conn = get_pg_conn()
        cursor = conn.cursor()
        try:
            cursor.execute("""
                SELECT COUNT(*) FROM airflow_dag_schedule
            """)
            count = cursor.fetchone()
            if count and count[0] > 0:
                logger.info(f"数据库中有 {count[0]} 条任务记录可用于调度")
            else:
                logger.info("数据库中没有找到任务记录，DAG的第一次运行将创建初始计划")
        except Exception as e:
            logger.warning(f"查询数据库时出错: {str(e)}, 这不会影响DAG的实际执行")
        finally:
            cursor.close()
            conn.close()
    except Exception as e:
        logger.warning(f"初始化DAG时发生错误: {str(e)}, 这不会影响DAG的实际执行")
        # 确保即使出错，也有清晰的执行路径
        # 已经有默认依赖链，不需要额外添加 