"""
统一数据产品线完成器 DAG

功能：
1. 由dataops_productline_execute_dag触发，不自行调度
2. 收集执行DAG的执行统计信息
3. 生成执行报告
4. 无论execute DAG执行成功与否都会运行
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta, date
import logging
import json
import pendulum
import pytz
import os
import sys
from utils import get_today_date
from airflow.models import DagRun, TaskInstance
from airflow.utils.state import State
from sqlalchemy import desc
from airflow import settings

from decimal import Decimal

# 创建日志记录器
logger = logging.getLogger(__name__)

# 开启详细日志记录
ENABLE_DEBUG_LOGGING = True

def log_debug(message):
    """记录调试日志，但只在启用调试模式时"""
    if ENABLE_DEBUG_LOGGING:
        logger.info(f"[DEBUG] {message}")

# 在DAG启动时输出诊断信息
log_debug("======== 诊断信息 ========")
log_debug(f"DAG dataops_productline_finalize_dag 初始化")
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
# 统计和报告生成函数
#############################################

def collect_execution_stats(**kwargs):
    """
    从Airflow元数据收集执行DAG的执行统计信息
    """
    # 获取当前执行的日期和时间信息
    dag_run = kwargs.get('dag_run')
    logical_date = dag_run.logical_date
    local_logical_date = pendulum.instance(logical_date).in_timezone('Asia/Shanghai')
    exec_date = local_logical_date.strftime('%Y-%m-%d')
    
    # 获取触发此DAG的配置信息（如果有）
    conf = dag_run.conf or {}
    parent_exec_date = conf.get('execution_date', exec_date)
    parent_run_id = conf.get('parent_run_id')
    
    # 记录完整的conf内容
    logger.info(f"【从上游DAG接收的配置】complete conf: {conf}")
    
    # 记录重要的时间参数
    logger.info(f"【时间参数】collect_execution_stats: exec_date={exec_date}, logical_date={logical_date}, parent_exec_date={parent_exec_date}")
    logger.info(f"【上游DAG信息】parent_run_id={parent_run_id}")
    logger.info(f"开始收集执行日期 {parent_exec_date} 的脚本执行统计信息")
    
    # 执行DAG的ID
    target_dag_id = "dataops_productline_execute_dag"
    
    try:
        # 创建数据库会话
        session = settings.Session()
        
        # 首先通过run_id查询（如果提供了）
        dag_runs = []
        if parent_run_id:
            logger.info(f"使用parent_run_id={parent_run_id}查询DAG运行记录")
            dag_runs = session.query(DagRun).filter(
                DagRun.dag_id == target_dag_id,
                DagRun.run_id == parent_run_id
            ).all()
            
            if dag_runs:
                logger.info(f"通过run_id找到匹配的DAG运行记录")
            else:
                logger.warning(f"未通过run_id找到匹配的DAG运行记录，尝试使用执行日期")
        
        # 如果通过run_id未找到记录，尝试使用执行日期
        if not dag_runs and parent_exec_date:
            # 尝试解析父执行日期为datetime对象
            if isinstance(parent_exec_date, str):
                try:
                    parent_exec_date_dt = pendulum.parse(parent_exec_date)
                except:
                    parent_exec_date_dt = None
            else:
                parent_exec_date_dt = parent_exec_date
            
            # 记录解析结果
            logger.info(f"解析后的父执行日期: {parent_exec_date_dt}")
            
            # 如果成功解析，使用它查询
            if parent_exec_date_dt:
                logger.info(f"使用父执行日期 {parent_exec_date_dt} 查询DAG运行")
                dag_runs = session.query(DagRun).filter(
                    DagRun.dag_id == target_dag_id,
                    DagRun.execution_date == parent_exec_date_dt
                ).order_by(desc(DagRun.execution_date)).limit(1).all()
                
                if dag_runs:
                    logger.info(f"通过执行日期找到匹配的DAG运行记录")
                else:
                    logger.error(f"未通过执行日期找到匹配的DAG运行记录")
            else:
                logger.error(f"无法解析父执行日期 {parent_exec_date}")
        
        # 如果两种方法都无法找到记录，则报错
        if not dag_runs:
            error_msg = f"无法找到DAG {target_dag_id} 的相关运行记录。提供的run_id: {parent_run_id}, 执行日期: {parent_exec_date}"
            logger.error(error_msg)
            session.close()
            raise ValueError(error_msg)
        
        # 获取DAG运行记录
        dag_run = dag_runs[0]
        dag_run_id = dag_run.run_id
        dag_start_time = dag_run.start_date
        dag_end_time = dag_run.end_date
        dag_state = dag_run.state
        dag_execution_date = dag_run.execution_date
        
        # 记录匹配方式
        if parent_run_id and dag_run_id == parent_run_id:
            match_method = "run_id精确匹配"
        else:
            match_method = "执行日期匹配"
        
        logger.info(f"【匹配方式】成功通过{match_method}找到DAG运行记录")
        logger.info(f"【匹配结果】run_id={dag_run_id}, 执行日期={dag_execution_date}, 状态={dag_state}")
        
        # 计算DAG运行时间
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
        
        # 统计脚本类型任务数量
        script_task_count = sum(1 for ti in task_instances if "-TO-" in ti.task_id)
        
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
            "script_task_count": script_task_count,
            "duration": dag_duration,
            "start_time": dag_start_time_str,
            "end_time": dag_end_time_str,
            "top_tasks_by_duration": top_tasks_by_duration,
            "failed_tasks": failed_tasks
        }
        
        # 将统计结果保存到XCom
        kwargs['ti'].xcom_push(key='execution_stats', value=stats)
        
        logger.info(f"成功收集脚本执行统计信息: 总任务数={total_tasks}, 成功={success_count}, 失败={fail_count}")
        return stats
    except Exception as e:
        logger.error(f"收集脚本执行统计信息时出错: {str(e)}")
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
        kwargs['ti'].xcom_push(key='execution_stats', value=error_stats)
        return error_stats

def generate_execution_report(**kwargs):
    """
    基于收集的统计信息生成执行报告
    """
    try:
        # 从XCom获取统计信息
        ti = kwargs['ti']
        stats = ti.xcom_pull(task_ids='collect_execution_stats')
        
        if not stats:
            logger.warning("未找到脚本执行统计信息，无法生成报告")
            report = "未找到脚本执行统计信息，无法生成报告。"
            ti.xcom_push(key='execution_report', value=report)
            return report
        
        # 构建报告
        report = []
        report.append(f"\n========== 脚本执行报告 ==========")
        report.append(f"执行日期: {stats['exec_date']}")
        report.append(f"DAG ID: {stats['dag_id']}")
        report.append(f"Run ID: {stats.get('dag_run_id', 'N/A')}")
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
        report.append(f"脚本执行任务: {stats.get('script_task_count', 0)} 个")
        
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
            report.append("所有脚本执行成功完成！")
        elif stats['status'] == 'failed':
            report.append(f"脚本执行过程中出现失败。有 {stats['fail_count']} 个任务失败。")
        else:
            report.append(f"当前状态: {stats['status']}")
        
        report.append("\n========== 报告结束 ==========")
        
        # 将报告转换为字符串
        report_str = "\n".join(report)
        
        # 记录到日志
        logger.info("\n" + report_str)
        
        # 保存到XCom
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

#############################################
# 创建DAG
#############################################
with DAG(
    "dataops_productline_finalize_dag", 
    start_date=datetime(2024, 1, 1), 
    schedule_interval=None,  # 不自行调度，由dataops_productline_execute_dag触发
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
    
    # 记录DAG实例化时的信息
    now = datetime.now()
    now_with_tz = now.replace(tzinfo=pytz.timezone('Asia/Shanghai'))
    default_exec_date = get_today_date()
    logger.info(f"【DAG初始化】当前时间: {now} / {now_with_tz}, 默认执行日期: {default_exec_date}")
    
    #############################################
    # 收集统计信息
    #############################################
    collect_stats = PythonOperator(
        task_id="collect_execution_stats",
        python_callable=collect_execution_stats,
        provide_context=True,
        dag=dag
    )
    
    #############################################
    # 生成执行报告
    #############################################
    generate_report = PythonOperator(
        task_id="generate_execution_report",
        python_callable=generate_execution_report,
        provide_context=True,
        dag=dag
    )
    
    #############################################
    # 完成标记
    #############################################
    finalize_completed = EmptyOperator(
        task_id="finalize_completed",
        dag=dag
    )
    
    # 设置任务依赖
    collect_stats >> generate_report >> finalize_completed 