# dag_dataops_summary_scheduler.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta
import logging
import json
from decimal import Decimal
from common import get_pg_conn, get_today_date

# 创建日志记录器
logger = logging.getLogger(__name__)

# 添加自定义JSON编码器解决Decimal序列化问题
class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        # 处理日期类型
        elif isinstance(obj, datetime):
            return obj.isoformat()
        # 让父类处理其他类型
        return super(DecimalEncoder, self).default(obj)

def get_execution_stats(exec_date):
    """获取当日执行统计信息"""
    conn = get_pg_conn()
    cursor = conn.cursor()
    try:
        # 查询总任务数
        cursor.execute("""
            SELECT COUNT(*) FROM airflow_dag_schedule WHERE exec_date = %s
        """, (exec_date,))
        total_tasks = cursor.fetchone()[0]
        
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
        success_count = cursor.fetchone()[0]
        
        cursor.execute("""
            SELECT COUNT(*) 
            FROM airflow_dag_schedule 
            WHERE exec_date = %s AND exec_result IS FALSE
        """, (exec_date,))
        fail_count = cursor.fetchone()[0]
        
        cursor.execute("""
            SELECT COUNT(*) 
            FROM airflow_dag_schedule 
            WHERE exec_date = %s AND exec_result IS NULL
        """, (exec_date,))
        pending_count = cursor.fetchone()[0]
        
        # 计算执行时间统计
        cursor.execute("""
            SELECT AVG(exec_duration), MIN(exec_duration), MAX(exec_duration)
            FROM airflow_dag_schedule 
            WHERE exec_date = %s AND exec_duration IS NOT NULL
        """, (exec_date,))
        time_stats = cursor.fetchone()
        avg_duration, min_duration, max_duration = time_stats if time_stats else (None, None, None)
        
        # 将Decimal转换为float
        if avg_duration is not None:
            avg_duration = float(avg_duration)
        if min_duration is not None:
            min_duration = float(min_duration)
        if max_duration is not None:
            max_duration = float(max_duration)
        
        # 查询失败任务详情
        cursor.execute("""
            SELECT target_table, script_name, target_table_label, exec_duration
            FROM airflow_dag_schedule 
            WHERE exec_date = %s AND exec_result IS FALSE
        """, (exec_date,))
        failed_tasks = [
            {
                "target_table": row[0],
                "script_name": row[1],
                "target_table_label": row[2],
                "exec_duration": float(row[3]) if row[3] is not None else None
            }
            for row in cursor.fetchall()
        ]
        
        # 汇总统计信息
        stats = {
            "exec_date": exec_date,
            "total_tasks": total_tasks,
            "type_counts": type_counts,
            "success_count": success_count,
            "fail_count": fail_count,
            "pending_count": pending_count,
            "success_rate": (success_count / total_tasks * 100) if total_tasks > 0 else 0,
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
    report.append(f"平均执行时间: {stats.get('avg_duration', 0):.2f}")
    report.append(f"最短执行时间: {stats.get('min_duration', 0):.2f}")
    report.append(f"最长执行时间: {stats.get('max_duration', 0):.2f}")
    
    # 失败任务详情
    failed_tasks = stats.get('failed_tasks', [])
    if failed_tasks:
        report.append("\n--- 失败任务详情 ---")
        for i, task in enumerate(failed_tasks, 1):
            report.append(f"{i}. 表名: {task['target_table']}")
            report.append(f"   脚本: {task['script_name']}")
            report.append(f"   类型: {task['target_table_label']}")
            report.append(f"   执行时间: {task.get('exec_duration', 'N/A'):.2f} 秒")
    
    report.append("\n========== 报告结束 ==========")
    
    # 将报告转换为字符串
    report_str = "\n".join(report)
    
    # 记录到日志
    logger.info("\n" + report_str)
    
    return report_str

def summarize_execution(**kwargs):
    """汇总执行情况的主函数"""
    exec_date = kwargs.get('ds') or get_today_date()
    logger.info(f"开始汇总执行日期 {exec_date} 的执行情况")
    
    # 1. 更新缺失的执行结果
    update_count = update_missing_results(exec_date)
    logger.info(f"更新了 {update_count} 个缺失的执行结果")
    
    # 2. 获取执行统计信息
    stats = get_execution_stats(exec_date)
    
    # 3. 生成执行报告
    report = generate_execution_report(exec_date, stats)
    
    # 将报告和统计信息传递给下一个任务
    kwargs['ti'].xcom_push(key='execution_stats', value=json.dumps(stats, cls=DecimalEncoder))
    kwargs['ti'].xcom_push(key='execution_report', value=report)
    
    return report

# 创建DAG
with DAG(
    "dag_dataops_summary_scheduler", 
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
    
    # 等待model DAG完成
    wait_for_model = ExternalTaskSensor(
        task_id="wait_for_model",
        external_dag_id="dag_dataops_model_scheduler",
        external_task_id="model_processing_completed",
        mode="poke",
        timeout=3600,
        poke_interval=30,
        dag=dag
    )
    
    # 汇总执行情况
    summarize_task = PythonOperator(
        task_id="summarize_execution",
        python_callable=summarize_execution,
        provide_context=True,
        dag=dag
    )
    
    # 总结完成标记
    summary_completed = EmptyOperator(
        task_id="summary_completed",
        dag=dag
    )
    
    # 设置任务依赖
    wait_for_model >> summarize_task >> summary_completed