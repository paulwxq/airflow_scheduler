# dag_dataops_pipeline_summary_scheduler.py
"""
数据管道执行统计汇总 DAG

功能：
1. 依赖主数据处理 DAG (dag_dataops_pipeline_data_scheduler) 的完成
2. 收集主 DAG 的执行统计信息
3. 生成执行报告
4. 无论主 DAG 执行成功与否都会运行
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta
import logging
import json
import pendulum
import pytz
from airflow.models import DagRun, TaskInstance
from airflow.utils.state import State
from sqlalchemy import desc
from airflow import settings
from common import get_today_date

# 创建日志记录器
logger = logging.getLogger(__name__)

# 开启详细日志记录
ENABLE_DEBUG_LOGGING = True

def log_debug(message):
    """记录调试日志，但只在启用调试模式时"""
    if ENABLE_DEBUG_LOGGING:
        logger.info(f"[DEBUG] {message}")

def print_target_date(dt):
    """
    打印并返回执行日期信息，用于 ExternalTaskSensor
    """
    # 转换为中国时区
    local_dt = pendulum.instance(dt).in_timezone('Asia/Shanghai')

    logger.info(f"===== ExternalTaskSensor等待的目标日期信息 =====")
    logger.info(f"源DAG: dag_dataops_pipeline_summary_scheduler")
    logger.info(f"目标DAG: dag_dataops_pipeline_data_scheduler")
    logger.info(f"目标任务: data_processing_phase.processing_completed")
    logger.info(f"查找的执行日期(UTC): {dt}")
    logger.info(f"查找的执行日期(北京时间): {local_dt}")
    logger.info(f"日期字符串格式(UTC): {dt.strftime('%Y-%m-%dT%H:%M:%S')}")
    logger.info(f"日期字符串格式(北京时间): {local_dt.strftime('%Y-%m-%dT%H:%M:%S')}")
    logger.info(f"日期UTC时区: {dt.tzinfo}")
    logger.info(f"日期类型: {type(dt)}")
    logger.info(f"=======================================")
    # 必须返回原始日期，不能修改
    return dt


def collect_pipeline_stats(**kwargs):
    """
    从 Airflow 元数据收集主 DAG 的执行统计信息
    """
    # 获取当前执行的日期和时间信息
    dag_run = kwargs.get('dag_run')
    logical_date = dag_run.logical_date
    local_logical_date = pendulum.instance(logical_date).in_timezone('Asia/Shanghai')
    exec_date = local_logical_date.strftime('%Y-%m-%d')
    
    # 记录重要的时间参数
    logger.info(f"【时间参数】collect_pipeline_stats: exec_date={exec_date}, logical_date={logical_date}, local_logical_date={local_logical_date}")
    logger.info(f"开始收集执行日期 {exec_date} 的管道执行统计信息")
    
    # 主 DAG 的 ID
    target_dag_id = "dag_dataops_pipeline_data_scheduler"
    
    try:
        # 创建数据库会话
        session = settings.Session()
        
        # 查询最近的 DAG 运行记录，按照创建时间降序排序
        dag_runs = session.query(DagRun).filter(
            DagRun.dag_id == target_dag_id,
            DagRun.execution_date == local_logical_date  # 只考虑当前执行日期及之前的运行
        ).order_by(desc(DagRun.execution_date)).limit(1).all()
        
        if not dag_runs:
            logger.warning(f"未找到 DAG {target_dag_id} 的运行记录")
            session.close()
            return {
                "exec_date": exec_date,
                "dag_id": target_dag_id,
                "status": "NOT_FOUND",
                "total_tasks": 0,
                "success_count": 0,
                "fail_count": 0,
                "skipped_count": 0,
                "upstream_failed_count": 0,
                "duration": None,
                "start_time": None,
                "end_time": None
            }
        
        # 获取最近的 DAG 运行
        dag_run = dag_runs[0]
        dag_run_id = dag_run.run_id
        dag_start_time = dag_run.start_date
        dag_end_time = dag_run.end_date
        dag_state = dag_run.state
        dag_execution_date = dag_run.execution_date
        
        # 计算 DAG 运行时间
        dag_duration = None
        if dag_start_time and dag_end_time:
            dag_duration = (dag_end_time - dag_start_time).total_seconds()

        
        # 时区转换
        if dag_start_time:
            dag_start_time_local = pendulum.instance(dag_start_time).in_timezone('Asia/Shanghai')
            dag_start_time_str = dag_start_time_local.strftime('%Y-%m-%d %H:%M:%S')
        else:
            dag_start_time_str = 'N/A'
            
        if dag_end_time:
            dag_end_time_local = pendulum.instance(dag_end_time).in_timezone('Asia/Shanghai')
            dag_end_time_str = dag_end_time_local.strftime('%Y-%m-%d %H:%M:%S')
        else:
            dag_end_time_str = 'N/A'

            
        # 获取所有相关的任务实例
        task_instances = session.query(TaskInstance).filter(
            TaskInstance.dag_id == target_dag_id,
            TaskInstance.run_id == dag_run_id
        ).all()
        
        # 关闭会话
        session.close()
        
        # 统计任务状态信息
        total_tasks = len(task_instances)
        success_count = sum(1 for ti in task_instances if ti.state == State.SUCCESS)
        fail_count = sum(1 for ti in task_instances if ti.state == State.FAILED)
        skipped_count = sum(1 for ti in task_instances if ti.state == State.SKIPPED)
        upstream_failed_count = sum(1 for ti in task_instances if ti.state == State.UPSTREAM_FAILED)
        
        # 统计各任务类型的数量
        resource_task_count = sum(1 for ti in task_instances if "resource_" in ti.task_id)
        model_task_count = sum(1 for ti in task_instances if "model_" in ti.task_id)
        
        # 获取执行时间最长的几个任务
        task_durations = []
        for ti in task_instances:
            if ti.start_date and ti.end_date:
                duration = (ti.end_date - ti.start_date).total_seconds()
                task_durations.append({
                    "task_id": ti.task_id,
                    "duration": duration,
                    "state": ti.state
                })
        
        # 按持续时间降序排序
        task_durations.sort(key=lambda x: x["duration"] if x["duration"] is not None else 0, reverse=True)
        top_tasks_by_duration = task_durations[:5]  # 取前5个
        
        # 获取失败的任务
        failed_tasks = []
        for ti in task_instances:
            if ti.state in [State.FAILED, State.UPSTREAM_FAILED]:
                failed_task = {
                    "task_id": ti.task_id,
                    "state": ti.state,
                    "try_number": ti.try_number,
                }
                if ti.start_date and ti.end_date:
                    failed_task["duration"] = (ti.end_date - ti.start_date).total_seconds()
                failed_tasks.append(failed_task)
        
        # 构建统计结果
        stats = {
            "exec_date": exec_date,
            "dag_id": target_dag_id,
            "dag_execution_date": dag_execution_date.isoformat() if dag_execution_date else None,
            "dag_run_id": dag_run_id,
            "status": dag_state,
            "total_tasks": total_tasks,
            "success_count": success_count,
            "fail_count": fail_count,
            "skipped_count": skipped_count,
            "upstream_failed_count": upstream_failed_count,
            "resource_task_count": resource_task_count,
            "model_task_count": model_task_count,
            "duration": dag_duration,
            "start_time": dag_start_time_str,
            "end_time": dag_end_time_str,
            "top_tasks_by_duration": top_tasks_by_duration,
            "failed_tasks": failed_tasks
        }
        
        # 将统计结果保存到 XCom
        kwargs['ti'].xcom_push(key='pipeline_stats', value=stats)
        
        logger.info(f"成功收集管道执行统计信息: 总任务数={total_tasks}, 成功={success_count}, 失败={fail_count}")
        return stats
    except Exception as e:
        logger.error(f"收集管道执行统计信息时出错: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        # 返回一个基本的错误信息
        error_stats = {
            "exec_date": exec_date,
            "dag_id": target_dag_id,
            "status": "ERROR",
            "error": str(e),
            "total_tasks": 0,
            "success_count": 0,
            "fail_count": 0
        }
        kwargs['ti'].xcom_push(key='pipeline_stats', value=error_stats)
        return error_stats

def generate_execution_report(**kwargs):
    """
    基于收集的统计信息生成执行报告
    """
    try:
        # 从 XCom 获取统计信息
        ti = kwargs['ti']
        stats = ti.xcom_pull(task_ids='collect_pipeline_stats')
        
        if not stats:
            logger.warning("未找到管道执行统计信息，无法生成报告")
            report = "未找到管道执行统计信息，无法生成报告。"
            ti.xcom_push(key='execution_report', value=report)
            return report
        
        # 构建报告
        report = []
        report.append(f"\n========== Data pipeline 执行报告 ==========")
        report.append(f"执行日期: {stats['exec_date']}")
        report.append(f"DAG ID: {stats['dag_id']}")
        report.append(f"Runn ID: {stats.get('dag_run_id', 'N/A')}")
        report.append(f"状态: {stats['status']}")
        report.append(f"总任务数: {stats['total_tasks']}")
        
        # 任务状态统计
        report.append("\n--- 任务状态统计 ---")
        report.append(f"成功任务: {stats['success_count']} 个")
        report.append(f"失败任务: {stats['fail_count']} 个")
        report.append(f"跳过任务: {stats.get('skipped_count', 0)} 个")
        report.append(f"上游失败任务: {stats.get('upstream_failed_count', 0)} 个")
        
        # 任务类型统计
        report.append("\n--- 任务类型统计 ---")
        report.append(f"资源任务: {stats.get('resource_task_count', 0)} 个")
        report.append(f"模型任务: {stats.get('model_task_count', 0)} 个")
        
        # 执行时间统计
        report.append("\n--- 执行时间统计 ---")
        if stats.get('duration') is not None:
            hours, remainder = divmod(stats['duration'], 3600)
            minutes, seconds = divmod(remainder, 60)
            report.append(f"总执行时间: {int(hours)}小时 {int(minutes)}分钟 {int(seconds)}秒")
        else:
            report.append("总执行时间: N/A")
            
        report.append(f"开始时间(北京时间): {stats.get('start_time', 'N/A')}")
        report.append(f"结束时间(北京时间): {stats.get('end_time', 'N/A')}")
        
        # 执行时间最长的任务
        top_tasks = stats.get('top_tasks_by_duration', [])
        if top_tasks:
            report.append("\n--- 执行时间最长的任务 ---")
            for i, task in enumerate(top_tasks, 1):
                duration_secs = task.get('duration', 0)
                minutes, seconds = divmod(duration_secs, 60)
                report.append(f"{i}. {task['task_id']}: {int(minutes)}分钟 {int(seconds)}秒 ({task['state']})")
        
        # 失败任务详情
        failed_tasks = stats.get('failed_tasks', [])
        if failed_tasks:
            report.append("\n--- 失败任务详情 ---")
            for i, task in enumerate(failed_tasks, 1):
                report.append(f"{i}. 任务ID: {task['task_id']}")
                report.append(f"   状态: {task['state']}")
                report.append(f"   尝试次数: {task.get('try_number', 'N/A')}")
                
                if 'duration' in task:
                    minutes, seconds = divmod(task['duration'], 60)
                    report.append(f"   执行时间: {int(minutes)}分钟 {int(seconds)}秒")
                else:
                    report.append("   执行时间: N/A")
        
        # 总结
        success_rate = 0
        if stats['total_tasks'] > 0:
            success_rate = (stats['success_count'] / stats['total_tasks']) * 100
            
        report.append("\n--- 总结 ---")
        report.append(f"任务成功率: {success_rate:.2f}%")
        
        if stats['status'] == 'success':
            report.append("管道执行成功完成！")
        elif stats['status'] == 'failed':
            report.append(f"管道执行失败。有 {stats['fail_count']} 个任务失败。")
        else:
            report.append(f"管道当前状态: {stats['status']}")
        
        report.append("\n========== 报告结束 ==========")
        
        # 将报告转换为字符串
        report_str = "\n".join(report)
        
        # 记录到日志
        logger.info("\n" + report_str)
        
        # 保存到 XCom
        ti.xcom_push(key='execution_report', value=report_str)
        
        return report_str
    except Exception as e:
        logger.error(f"生成执行报告时出错: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        # 返回一个简单的错误报告
        error_report = f"生成执行报告时出错: {str(e)}"
        kwargs['ti'].xcom_push(key='execution_report', value=error_report)
        return error_report

# 创建 DAG
with DAG(
    "dag_dataops_pipeline_summary_scheduler", 
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
    
    # 记录 DAG 实例化时的信息
    now = datetime.now()
    now_with_tz = now.replace(tzinfo=pytz.timezone('Asia/Shanghai'))
    default_exec_date = get_today_date()
    logger.info(f"【DAG初始化】当前时间: {now} / {now_with_tz}, 默认执行日期: {default_exec_date}")
    
    #############################################
    # 等待阶段: 等待主 DAG 完成
    #############################################
    wait_for_pipeline_completion = ExternalTaskSensor(
        task_id="wait_for_pipeline_completion",
        external_dag_id="dag_dataops_pipeline_data_scheduler",
        external_task_id="data_processing_phase.processing_completed",
        mode="reschedule",  # 使用 reschedule 模式，不会占用 worker
        timeout=7200,  # 等待超时时间为 2 小时
        poke_interval=30,  # 每15秒检查一次
        allowed_states=["success", "failed", "skipped"],  # 允许的状态包括成功、失败和跳过
        failed_states=None,  # 不设置失败状态，确保无论主 DAG 状态如何都会继续执行
        execution_date_fn=print_target_date,  # 用于调试的日期打印函数
        dag=dag
    )
    
    #############################################
    # 统计阶段: 收集和生成统计信息
    #############################################
    collect_stats = PythonOperator(
        task_id="collect_pipeline_stats",
        python_callable=collect_pipeline_stats,
        provide_context=True,
        dag=dag
    )
    
    generate_report = PythonOperator(
        task_id="generate_execution_report",
        python_callable=generate_execution_report,
        provide_context=True,
        dag=dag
    )
    
    #############################################
    # 完成阶段: 标记汇总完成
    #############################################
    summary_completed = EmptyOperator(
        task_id="summary_completed",
        dag=dag
    )
    
    # 设置任务依赖
    wait_for_pipeline_completion >> collect_stats >> generate_report >> summary_completed