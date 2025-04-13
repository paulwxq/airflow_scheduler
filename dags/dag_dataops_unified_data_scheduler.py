# dag_dataops_unified_data_scheduler.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta, date
import logging
import networkx as nx
import json
import os
from common import (
    get_pg_conn, 
    get_neo4j_driver,
    execute_with_monitoring,
    get_today_date
)
from config import TASK_RETRY_CONFIG, SCRIPTS_BASE_PATH, AIRFLOW_BASE_PATH

# 创建日志记录器
logger = logging.getLogger(__name__)

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

def json_serial(obj):
    """将日期对象序列化为ISO格式字符串的JSON序列化器"""
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError(f"类型 {type(obj)} 不能被序列化为JSON")

def prepare_unified_execution_plan(**kwargs):
    """准备统一执行计划的主函数"""
    # 获取执行日期
    exec_date = get_latest_date()
    logger.info(f"使用执行日期: {exec_date}")
    
    # 获取所有任务
    resource_tasks, model_tasks = get_all_tasks(exec_date)
    
    if not resource_tasks and not model_tasks:
        logger.warning(f"执行日期 {exec_date} 没有找到任务")
        return 0
    
    # 为所有模型表获取依赖关系
    model_table_names = [task["target_table"] for task in model_tasks]
    dependencies = get_table_dependencies(model_table_names)
    
    # 创建执行计划
    execution_plan = {
        "exec_date": exec_date,
        "resource_tasks": resource_tasks,
        "model_tasks": model_tasks,
        "dependencies": dependencies
    }
    
    # 记录资源任务和模型任务的名称，便于调试
    resource_names = [task["target_table"] for task in resource_tasks]
    model_names = [task["target_table"] for task in model_tasks]
    logger.info(f"资源表任务: {resource_names}")
    logger.info(f"模型表任务: {model_names}")
    
    # 已经不需要推送到XCom，因为我们直接从文件读取
    # 这里仅用于验证执行计划与文件中的是否一致
    plan_path = os.path.join(os.path.dirname(__file__), 'last_execution_plan.json')
    ready_path = f"{plan_path}.ready"
    
    if os.path.exists(plan_path) and os.path.exists(ready_path):
        try:
            with open(plan_path, 'r') as f:
                existing_plan = json.load(f)
            
            # 比较执行计划是否有变化
            existing_resources = sorted([t.get("target_table") for t in existing_plan.get("resource_tasks", [])])
            current_resources = sorted(resource_names)
            
            existing_models = sorted([t.get("target_table") for t in existing_plan.get("model_tasks", [])])
            current_models = sorted(model_names)
            
            if existing_resources == current_resources and existing_models == current_models:
                logger.info("执行计划无变化，继续使用现有任务结构")
            else:
                logger.warning("执行计划与现有文件不一致，但DAG结构已固定，需等待下次解析")
                logger.warning(f"现有资源表: {existing_resources}")
                logger.warning(f"当前资源表: {current_resources}")
                logger.warning(f"现有模型表: {existing_models}")
                logger.warning(f"当前模型表: {current_models}")
        except Exception as e:
            logger.error(f"比较执行计划时出错: {str(e)}")
    
    logger.info(f"准备了执行计划，包含 {len(resource_tasks)} 个资源表任务和 {len(model_tasks)} 个模型表任务")
    return len(resource_tasks) + len(model_tasks)

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

# 修改预先加载数据以创建任务的逻辑
try:
    logger.info("预先加载执行计划数据用于构建DAG")
    plan_path = os.path.join(os.path.dirname(__file__), 'last_execution_plan.json')
    ready_path = f"{plan_path}.ready"
    execution_plan = {"exec_date": get_today_date(), "resource_tasks": [], "model_tasks": [], "dependencies": {}}
    
    # 首先检查ready文件是否存在，确保JSON文件已完整生成
    if os.path.exists(ready_path) and os.path.exists(plan_path):
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
        except Exception as e:
            logger.warning(f"读取执行计划文件出错: {str(e)}")
    else:
        if not os.path.exists(ready_path):
            logger.warning(f"执行计划ready标记文件不存在: {ready_path}")
        if not os.path.exists(plan_path):
            logger.warning(f"执行计划文件不存在: {plan_path}")
        logger.warning("将创建基础DAG结构")
    
    # 提取信息
    exec_date = execution_plan.get("exec_date", get_today_date())
    resource_tasks = execution_plan.get("resource_tasks", [])
    model_tasks = execution_plan.get("model_tasks", [])
    dependencies = execution_plan.get("dependencies", {})
    
    logger.info(f"预加载执行计划: exec_date={exec_date}, resource_tasks数量={len(resource_tasks)}, model_tasks数量={len(model_tasks)}")
except Exception as e:
    logger.error(f"预加载执行计划数据时出错: {str(e)}")
    exec_date = get_today_date()
    resource_tasks = []
    model_tasks = []
    dependencies = {}

# 定义处理DAG失败的回调函数
def handle_dag_failure(context):
    logger.error(f"DAG执行失败: {context.get('exception')}")

# 创建DAG
with DAG(
    "dag_dataops_unified_data_scheduler", 
    start_date=datetime(2024, 1, 1), 
    # 修改调度间隔为每15分钟检查一次，以便及时响应执行计划变化
    schedule_interval="*/10 * * * *",
    catchup=False,
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    on_failure_callback=handle_dag_failure,
    # 添加DAG级别参数，确保任务运行时有正确的环境
    params={
        "scripts_path": SCRIPTS_BASE_PATH,
        "airflow_base_path": AIRFLOW_BASE_PATH
    }
) as dag:
    
    # 准备执行计划
    prepare_plan = PythonOperator(
        task_id="prepare_execution_plan",
        python_callable=prepare_unified_execution_plan,
        provide_context=True,
        dag=dag
    )
    
    # 处理完成标记
    processing_completed = EmptyOperator(
        task_id="processing_completed",
        trigger_rule="none_failed_min_one_success",  # 只要有一个任务成功且没有失败的任务就标记为完成
        dag=dag
    )
    
    # 任务字典，用于设置依赖关系
    task_dict = {}
    
    # 添加一个空任务作为下游任务的起始点，确保即使没有资源表和模型表，DAG也能正常执行
    start_processing = EmptyOperator(
        task_id="start_processing",
        dag=dag
    )
    
    # 设置基本依赖
    prepare_plan >> start_processing
    
    # 1. 预先创建资源表任务
    for task_info in resource_tasks:
        table_name = task_info["target_table"]
        script_name = task_info["script_name"]
        exec_mode = task_info.get("script_exec_mode", "append")
        
        # 创建安全的任务ID
        safe_table_name = table_name.replace(".", "_").replace("-", "_")
        task_id = f"resource_{safe_table_name}"
        
        resource_task = PythonOperator(
            task_id=task_id,
            python_callable=process_resource,
            op_kwargs={
                "target_table": table_name,
                "script_name": script_name,
                "script_exec_mode": exec_mode,
                "exec_date": exec_date  # 直接使用解析出的exec_date，不使用XCom
            },
            retries=TASK_RETRY_CONFIG["retries"],
            retry_delay=timedelta(minutes=TASK_RETRY_CONFIG["retry_delay_minutes"]),
            dag=dag
        )
        
        # 将任务添加到字典
        task_dict[table_name] = resource_task
        
        # 设置与start_processing的依赖
        start_processing >> resource_task
        logger.info(f"设置基本依赖: start_processing >> {task_id}")
    
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
    cycles = list(nx.simple_cycles(G))
    if cycles:
        logger.warning(f"检测到循环依赖: {cycles}")
        for cycle in cycles:
            G.remove_edge(cycle[-1], cycle[0])
            logger.info(f"打破循环依赖: 移除 {cycle[-1]} -> {cycle[0]} 的依赖")
    
    # 生成拓扑排序，确定执行顺序
    execution_order = []
    try:
        execution_order = list(nx.topological_sort(G))
        logger.info(f"预加载计算的执行顺序: {execution_order}")
    except Exception as e:
        logger.error(f"生成拓扑排序失败: {str(e)}, 使用原始顺序")
        execution_order = [task_info["target_table"] for task_info in model_tasks]
    
    # 2. 预先创建模型表任务
    for table_name in execution_order:
        task_info = next((t for t in model_tasks if t["target_table"] == table_name), None)
        if not task_info:
            continue
            
        script_name = task_info["script_name"]
        exec_mode = task_info.get("script_exec_mode", "append")
        
        # 创建安全的任务ID
        safe_table_name = table_name.replace(".", "_").replace("-", "_")
        task_id = f"model_{safe_table_name}"
        
        model_task = PythonOperator(
            task_id=task_id,
            python_callable=process_model,
            op_kwargs={
                "target_table": table_name,
                "script_name": script_name,
                "script_exec_mode": exec_mode,
                "exec_date": exec_date  # 直接使用解析出的exec_date，不使用XCom
            },
            retries=TASK_RETRY_CONFIG["retries"],
            retry_delay=timedelta(minutes=TASK_RETRY_CONFIG["retry_delay_minutes"]),
            dag=dag
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
                logger.info(f"预先设置依赖: {dep_table} >> {table_name}")
        
        # 如果没有依赖，则依赖于start_processing和资源表任务
        if not has_dependency:
            # 从start_processing任务直接连接
            start_processing >> model_task
            logger.info(f"设置基本依赖: start_processing >> {task_id}")
            
            # 同时从所有资源表任务连接 - 限制每个模型表最多依赖5个资源表，避免过度复杂的依赖关系
            resource_count = 0
            for resource_table in resource_tasks:
                if resource_count >= 5:
                    break
                
                resource_name = resource_table["target_table"]
                if resource_name in task_dict:
                    task_dict[resource_name] >> model_task
                    logger.info(f"预先设置资源依赖: {resource_name} >> {table_name}")
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
    
    # 如果既没有模型表任务也没有资源表任务，直接连接start_processing到完成标记
    if not terminal_tasks:
        start_processing >> processing_completed
        logger.warning("未找到任何任务，直接连接start_processing到完成标记")
    else:
        # 将所有终端任务连接到完成标记
        for table_name in terminal_tasks:
            if table_name in task_dict:
                task_dict[table_name] >> processing_completed
                logger.info(f"设置终端任务: {table_name} >> processing_completed")

logger.info(f"DAG dag_dataops_unified_data_scheduler 定义完成，预创建了 {len(task_dict)} 个任务")