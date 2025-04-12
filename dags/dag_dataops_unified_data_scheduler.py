# dag_dataops_unified_data_scheduler.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta, date
import logging
import networkx as nx
import json
from common import (
    get_pg_conn, 
    get_neo4j_driver,
    execute_with_monitoring,
    get_today_date
)
from config import TASK_RETRY_CONFIG

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
    
    # 将执行计划保存到XCom，使用自定义序列化器处理日期对象
    kwargs['ti'].xcom_push(key='execution_plan', value=json.dumps(execution_plan, default=json_serial))
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
    
    return execute_with_monitoring(
        target_table=target_table,
        script_name=script_name,
        script_exec_mode=script_exec_mode,
        exec_date=exec_date
    )

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
    
    return execute_with_monitoring(
        target_table=target_table,
        script_name=script_name,
        script_exec_mode=script_exec_mode,
        exec_date=exec_date
    )

# 创建DAG
with DAG(
    "dag_dataops_unified_data_scheduler", 
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
    
    # 等待准备DAG完成
    wait_for_prepare = ExternalTaskSensor(
        task_id="wait_for_prepare",
        external_dag_id="dag_dataops_unified_prepare_scheduler",
        external_task_id="preparation_completed",
        mode="poke",
        timeout=3600,
        poke_interval=30,
        dag=dag
    )
    
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
        dag=dag
    )
    
    # 设置初始任务依赖
    wait_for_prepare >> prepare_plan
    
    # 从执行计划JSON中获取信息
    execution_plan_json = '''{"exec_date": "2025-04-12", "resource_tasks": [], "model_tasks": [], "dependencies": {}}'''
    
    try:
        # 从文件或数据库中获取执行计划作为默认值
        import os
        plan_path = os.path.join(os.path.dirname(__file__), 'last_execution_plan.json')
        if os.path.exists(plan_path):
            with open(plan_path, 'r') as f:
                execution_plan_json = f.read()
    except Exception as e:
        logger.warning(f"读取执行计划默认值时出错: {str(e)}")
    
    # 解析执行计划获取任务信息
    try:
        execution_plan = json.loads(execution_plan_json)
        exec_date = execution_plan.get("exec_date", get_today_date())
        resource_tasks = execution_plan.get("resource_tasks", [])
        model_tasks = execution_plan.get("model_tasks", [])
        dependencies = execution_plan.get("dependencies", {})
        
        # 任务字典，用于设置依赖关系
        task_dict = {}
        
        # 1. 创建资源表任务
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
                    "exec_date": """{{ ti.xcom_pull(task_ids='prepare_execution_plan', key='execution_plan') }}"""
                },
                retries=TASK_RETRY_CONFIG["retries"],
                retry_delay=timedelta(minutes=TASK_RETRY_CONFIG["retry_delay_minutes"]),
                dag=dag
            )
            
            # 将任务添加到字典
            task_dict[table_name] = resource_task
            
            # 设置与prepare_plan的依赖
            prepare_plan >> resource_task
        
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
        try:
            execution_order = list(nx.topological_sort(G))
            logger.info(f"计算出的执行顺序: {execution_order}")
        except Exception as e:
            logger.error(f"生成拓扑排序失败: {str(e)}, 使用原始顺序")
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
            task_id = f"model_{safe_table_name}"
            
            model_task = PythonOperator(
                task_id=task_id,
                python_callable=process_model,
                op_kwargs={
                    "target_table": table_name,
                    "script_name": script_name,
                    "script_exec_mode": exec_mode,
                    "exec_date": """{{ ti.xcom_pull(task_ids='prepare_execution_plan', key='execution_plan') }}"""
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
                    logger.info(f"设置依赖: {dep_table} >> {table_name}")
            
            # 如果没有依赖，则依赖于准备任务和所有资源表任务
            if not has_dependency:
                # 从prepare_plan任务直接连接
                prepare_plan >> model_task
                
                # 同时从所有资源表任务连接
                for resource_table in resource_tasks:
                    resource_name = resource_table["target_table"]
                    if resource_name in task_dict:
                        task_dict[resource_name] >> model_task
                        logger.info(f"设置资源依赖: {resource_name} >> {table_name}")
        
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
        
        # 如果既没有模型表任务也没有资源表任务，直接连接准备任务到完成标记
        if not terminal_tasks:
            prepare_plan >> processing_completed
            logger.warning("未找到任何任务，直接连接准备任务到完成标记")
        else:
            # 将所有终端任务连接到完成标记
            for table_name in terminal_tasks:
                if table_name in task_dict:
                    task_dict[table_name] >> processing_completed
                    logger.info(f"设置终端任务: {table_name} >> processing_completed")
    
    except Exception as e:
        logger.error(f"构建任务DAG时出错: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        
        # 确保即使出错，DAG也能正常完成
        prepare_plan >> processing_completed