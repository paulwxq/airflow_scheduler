# dag_dataops_resource_scheduler.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta
import logging
from common import (
    get_pg_conn, execute_with_monitoring, get_today_date
)

# 创建日志记录器
logger = logging.getLogger(__name__)

# 添加获取最近日期的函数
def get_latest_date_with_resources():
    """
    获取数据库中包含DataResource记录的最近日期
    
    用于查找数据库中最近的日期，以确保能够获取到数据
    """
    conn = get_pg_conn()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT DISTINCT exec_date
            FROM airflow_dag_schedule 
            WHERE target_table_label = 'DataResource'
            ORDER BY exec_date DESC
            LIMIT 1
        """)
        result = cursor.fetchone()
        if result:
            latest_date = result[0]
            logger.info(f"找到最近的包含DataResource记录的日期: {latest_date}")
            return latest_date
        else:
            logger.warning("未找到包含DataResource记录的日期，将使用当前日期")
            return get_today_date()
    except Exception as e:
        logger.error(f"查找最近日期时出错: {str(e)}")
        return get_today_date()
    finally:
        cursor.close()
        conn.close()

def get_dataresource_tasks(exec_date):
    """从airflow_dag_schedule表获取DataResource任务"""
    conn = get_pg_conn()
    cursor = conn.cursor()
    try:
        # 构建SQL查询
        sql = """
            SELECT source_table, target_table, script_name, script_exec_mode
            FROM airflow_dag_schedule 
            WHERE exec_date = %s AND target_table_label = 'DataResource'
        """
        # 记录查询信息
        logger.info(f"查询资源任务，使用日期: {exec_date}")
        
        # 执行查询
        cursor.execute(sql, (exec_date,))
        results = cursor.fetchall()
        logger.info(f"使用日期 {exec_date} 查询到 {len(results)} 个DataResource任务")
        
        # 处理去重
        unique_tasks = {}
        for row in results:
            source_table, target_table, script_name, script_exec_mode = row
            # 使用目标表名作为键进行去重
            if target_table not in unique_tasks:
                unique_tasks[target_table] = {
                    "source_table": source_table,
                    "target_table": target_table,
                    "script_name": script_name,
                    "script_exec_mode": script_exec_mode or "append"  # 默认值
                }
        
        logger.info(f"获取到 {len(results)} 个DataResource任务，去重后剩余 {len(unique_tasks)} 个")
        return list(unique_tasks.values())
    except Exception as e:
        logger.error(f"获取DataResource任务时出错: {str(e)}")
        return []
    finally:
        cursor.close()
        conn.close()

def process_resource(target_table, script_name, script_exec_mode, **kwargs):
    """处理单个资源表的函数"""
    exec_date = kwargs.get('ds')
    logger.info(f"开始处理资源表: {target_table}, 脚本: {script_name}")
    
    try:
        # 调用执行函数
        result = execute_with_monitoring(
            target_table=target_table,
            script_name=script_name,
            script_exec_mode=script_exec_mode,
            exec_date=exec_date
        )
        logger.info(f"资源表 {target_table} 处理完成")
        return result
    except Exception as e:
        logger.error(f"处理资源表 {target_table} 时出错: {str(e)}")
        raise

def generate_no_task_message(**kwargs):
    """当没有任务时执行的函数"""
    logger.info("没有资源需要处理")
    return "没有资源需要处理"

# 创建DAG
with DAG(
    "dag_dataops_resource_scheduler", 
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
    
    # 等待prepare DAG完成
    wait_for_prepare = ExternalTaskSensor(
        task_id="wait_for_prepare",
        external_dag_id="dag_dataops_prepare_scheduler",
        external_task_id="preparation_completed",
        mode="poke",
        timeout=3600,
        poke_interval=30,
        dag=dag
    )
    
    # 处理完成标记
    resource_processing_completed = EmptyOperator(
        task_id="resource_processing_completed",
        dag=dag
    )
    
    # 在DAG运行时获取最近日期和资源任务
    latest_date = get_latest_date_with_resources()
    logger.info(f"使用最近的日期 {latest_date} 查询资源任务")
    
    # 获取资源任务
    resource_tasks = get_dataresource_tasks(latest_date)
    
    if resource_tasks:
        for i, task_info in enumerate(resource_tasks):
            target_table = task_info["target_table"]
            script_name = task_info["script_name"]
            script_exec_mode = task_info["script_exec_mode"]
            
            if not script_name:
                logger.warning(f"资源表 {target_table} 没有关联脚本，跳过")
                continue
            
            # 为每个资源表创建单独的处理任务
            task_id = f"process_resource_{target_table.replace('.', '_')}"
            process_task = PythonOperator(
                task_id=task_id,
                python_callable=process_resource,
                op_kwargs={
                    "target_table": target_table,
                    "script_name": script_name,
                    "script_exec_mode": script_exec_mode
                },
                provide_context=True,
                dag=dag
            )
            
            # 设置依赖 - 直接从wait_for_prepare连接到处理任务
            wait_for_prepare >> process_task >> resource_processing_completed
    else:
        # 如果没有任务，添加一个空任务
        empty_task = PythonOperator(
            task_id="no_resources_to_process",
            python_callable=generate_no_task_message,
            dag=dag
        )
        wait_for_prepare >> empty_task >> resource_processing_completed