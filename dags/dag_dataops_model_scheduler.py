# dag_dataops_model_scheduler.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta
import logging
from common import (
    get_pg_conn, execute_with_monitoring, get_datamodel_dependency_from_neo4j,
    generate_optimized_execution_order, get_today_date
)

# 创建日志记录器
logger = logging.getLogger(__name__)

def get_latest_date_with_models():
    """
    获取数据库中包含DataModel记录的最近日期
    
    用于查找数据库中最近的日期，以确保能够获取到数据
    """
    conn = get_pg_conn()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT DISTINCT exec_date
            FROM airflow_dag_schedule 
            WHERE target_table_label = 'DataModel'
            ORDER BY exec_date DESC
            LIMIT 1
        """)
        result = cursor.fetchone()
        if result:
            latest_date = result[0]
            logger.info(f"找到最近的包含DataModel记录的日期: {latest_date}")
            return latest_date
        else:
            logger.warning("未找到包含DataModel记录的日期，将使用当前日期")
            return get_today_date()
    except Exception as e:
        logger.error(f"查找最近日期时出错: {str(e)}")
        return get_today_date()
    finally:
        cursor.close()
        conn.close()

def get_datamodel_tasks(exec_date):
    """从airflow_dag_schedule表获取DataModel任务"""
    conn = get_pg_conn()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT target_table, script_name, script_exec_mode
            FROM airflow_dag_schedule 
            WHERE exec_date = %s AND target_table_label = 'DataModel'
        """, (exec_date,))
        
        results = cursor.fetchall()
        
        tasks = []
        for row in results:
            target_table, script_name, script_exec_mode = row
            tasks.append({
                "target_table": target_table,
                "script_name": script_name,
                "script_exec_mode": script_exec_mode or "append"  # 默认为append
            })
        
        logger.info(f"使用日期 {exec_date} 获取到 {len(tasks)} 个DataModel任务")
        return tasks
    except Exception as e:
        logger.error(f"获取DataModel任务时出错: {str(e)}")
        return []
    finally:
        cursor.close()
        conn.close()

# 创建DAG
with DAG(
    "dag_dataops_model_scheduler", 
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
    
    # 等待resource DAG完成
    wait_for_resource = ExternalTaskSensor(
        task_id="wait_for_resource",
        external_dag_id="dag_dataops_resource_scheduler",
        external_task_id="resource_processing_completed",
        mode="poke",
        timeout=3600,
        poke_interval=30,
        dag=dag
    )
    
    # 处理完成标记
    model_processing_completed = EmptyOperator(
        task_id="model_processing_completed",
        dag=dag
    )
    
    try:
        # 获取最近的日期
        latest_date = get_latest_date_with_models()
        logger.info(f"使用最近的日期 {latest_date} 查询模型任务")
        
        # 获取所有DataModel任务
        model_tasks = get_datamodel_tasks(latest_date)
        
        if model_tasks:
            # 获取表名列表
            table_names = [task["target_table"] for task in model_tasks]
            
            # 获取依赖关系
            dependency_dict = get_datamodel_dependency_from_neo4j(table_names)
            
            # 生成优化的执行顺序
            execution_order = generate_optimized_execution_order(table_names, dependency_dict)
            logger.info(f"生成的优化执行顺序: {execution_order}")
            
            # 创建任务字典
            task_dict = {}
            
            # 为每个表创建处理任务
            for table_name in execution_order:
                # 查找表任务信息
                task_info = next((t for t in model_tasks if t["target_table"] == table_name), None)
                
                if task_info and task_info.get("script_name"):
                    process_task = PythonOperator(
                        task_id=f"process_model_{table_name.replace('.', '_')}",
                        python_callable=execute_with_monitoring,
                        op_kwargs={
                            "target_table": table_name,
                            "script_name": task_info["script_name"],
                            "script_exec_mode": task_info.get("script_exec_mode", "append"), 
                            "exec_date": latest_date  # 使用从数据库获取的最近日期
                        },
                        dag=dag
                    )
                    task_dict[table_name] = process_task
                    logger.info(f"创建处理任务: {table_name}")
                else:
                    logger.warning(f"表 {table_name} 没有script_name，跳过任务创建")
            
            # 设置任务间的依赖关系
            for target_table, task in task_dict.items():
                # 获取上游依赖
                upstream_tables = dependency_dict.get(target_table, [])
                upstream_tables = [t for t in upstream_tables if t in task_dict]
                
                if not upstream_tables:
                    # 如果没有上游依赖，直接连接到wait_for_resource
                    logger.info(f"表 {target_table} 没有上游依赖，连接到wait_for_resource")
                    wait_for_resource >> task
                else:
                    # 设置与上游表的依赖关系
                    for upstream_table in upstream_tables:
                        logger.info(f"设置依赖: {upstream_table} >> {target_table}")
                        task_dict[upstream_table] >> task
                
                # 检查是否是末端节点（没有下游任务）
                is_terminal = True
                for downstream, upstreams in dependency_dict.items():
                    if target_table in upstreams and downstream in task_dict:
                        is_terminal = False
                        break
                
                # 如果是末端节点，连接到model_processing_completed
                if is_terminal:
                    logger.info(f"表 {target_table} 是末端节点，连接到model_processing_completed")
                    task >> model_processing_completed
            
            # 处理特殊情况
            # 检查是否有任务连接到model_processing_completed
            has_connection_to_completed = False
            for task in task_dict.values():
                for downstream in task.downstream_list:
                    if downstream.task_id == model_processing_completed.task_id:
                        has_connection_to_completed = True
                        break
                
                if has_connection_to_completed:
                    break
            
            # 如果没有任务连接到model_processing_completed，连接所有任务到完成标记
            if not has_connection_to_completed and task_dict:
                logger.info("没有任务连接到model_processing_completed，连接所有任务到完成标记")
                for task in task_dict.values():
                    task >> model_processing_completed
            
            # 检查是否有任务连接到wait_for_resource
            has_connection_from_wait = False
            for task in task_dict.values():
                for upstream in task.upstream_list:
                    if upstream.task_id == wait_for_resource.task_id:
                        has_connection_from_wait = True
                        break
                
                if has_connection_from_wait:
                    break
            
            # 如果没有任务连接到wait_for_resource，连接wait_for_resource到所有任务
            if not has_connection_from_wait and task_dict:
                logger.info("没有任务连接到wait_for_resource，连接wait_for_resource到所有任务")
                for task in task_dict.values():
                    wait_for_resource >> task
        else:
            # 如果没有任务，直接将等待节点连接到完成
            wait_for_resource >> model_processing_completed
            logger.warning("没有找到DataModel任务，直接将等待节点连接到完成")
    except Exception as e:
        logger.error(f"创建模型处理DAG时出错: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        # 确保在出错时也有完整的执行流程
        wait_for_resource >> model_processing_completed