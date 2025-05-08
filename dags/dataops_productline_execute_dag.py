"""
统一数据产品线执行器 DAG

功能：
1. 面向脚本的作业编排，不再是面向表
2. 基于dataops_productline_prepare_dag生成的执行计划执行脚本
3. 支持对脚本执行顺序的优化
4. 提供详细的执行日志和错误处理

预期的执行计划模式：
{
    "version": "2.0",
    "exec_date": "YYYY-MM-DD",
    "scripts": [
        {
            "task_id": "唯一任务ID",
            "script_id": "唯一脚本ID",
            "script_name": "脚本文件名或标识符",
            "script_type": "python|sql|python_script",
            "target_type": "structure|null",
            "update_mode": "append|full_refresh",
            "target_table": "表名",
            "source_tables": ["表1", "表2"],
            "schedule_status": true,
            "storage_location": "/路径/模式" 或 null,
            "schedule_frequency": "daily|weekly|monthly|quarterly|yearly",
            "target_table_label": "DataModel|DataResource|DataSource"
        },
        ...
    ],
    "model_scripts": ["script_id1", "script_id2", ...],
    "resource_scripts": ["script_id3", "script_id4", ...],
    "execution_order": ["script_id1", "script_id3", "script_id2", "script_id4", ...],
    "script_dependencies": {
        "script_id1": ["script_id3", "script_id4"],
        "script_id2": [],
        ...
    }
}
"""
from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta, date
from airflow.models import Variable
import logging
import networkx as nx
import json
import os
import pendulum
from decimal import Decimal
from utils import (
    get_pg_conn, 
    get_neo4j_driver,
    get_today_date,
    get_cn_exec_date
)
from config import TASK_RETRY_CONFIG, SCRIPTS_BASE_PATH, PG_CONFIG, NEO4J_CONFIG
import pytz
import pandas as pd
import sys

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
# 脚本执行函数
#############################################

def execute_python_script(script_id, script_name, target_table, update_mode, schedule_frequency, **kwargs):
    """
    执行Python脚本文件并返回执行结果
    
    参数:
        script_id: 脚本ID
        script_name: 脚本文件名（.py文件）
        target_table: 目标表名
        update_mode: 执行模式
        schedule_frequency: 执行频率
        **kwargs: 其他参数，如source_tables、target_type等
    
    返回:
        bool: 脚本执行结果
    """
    # 获取执行日期
    logical_date = kwargs.get('logical_date', datetime.now())
    exec_date, local_logical_date = get_cn_exec_date(logical_date)
    
    # 添加详细日志
    logger.info(f"===== 开始执行Python脚本文件 {script_id} =====")
    logger.info(f"script_id: {script_id}, 类型: {type(script_id)}")
    logger.info(f"script_name: {script_name}, 类型: {type(script_name)}")
    logger.info(f"target_table: {target_table}, 类型: {type(target_table)}")
    logger.info(f"update_mode: {update_mode}, 类型: {type(update_mode)}")
    logger.info(f"schedule_frequency: {schedule_frequency}, 类型: {type(schedule_frequency)}")
    logger.info(f"【时间参数】execute_python_script: exec_date={exec_date}, logical_date={logical_date}, local_logical_date={local_logical_date}")

    # 记录额外参数
    for key, value in kwargs.items():
        logger.info(f"额外参数 - {key}: {value}, 类型: {type(value)}")

    # 检查script_name是否为空
    if not script_name:
        logger.error(f"脚本ID {script_id} 的script_name为空，无法执行")
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
            logger.info(f"调用脚本文件 {script_name} 的标准入口函数 run()")
            # 构建完整的参数字典
            run_params = {
                "table_name": target_table,
                "execution_mode": update_mode,
                "exec_date": exec_date,
                "schedule_frequency": schedule_frequency
            }

            ## 添加可能的额外参数
            for key in ['target_type', 'storage_location', 'source_tables']:
                if key in kwargs and kwargs[key] is not None:
                    run_params[key] = kwargs[key] 

            # 调用脚本的run函数
            logger.info(f"调用run函数并传递参数: {run_params}")
            result = module.run(**run_params)
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
        logger.error(f"执行脚本 {script_id} 出错: {str(e)}")
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        logger.error(f"脚本 {script_name} 执行失败，耗时: {duration:.2f}秒")
        logger.info(f"===== 脚本执行异常结束 =====")
        import traceback
        logger.error(traceback.format_exc())
        
        # 确保不会阻塞DAG
        return False

# 使用execute_sql函数代替之前的execute_sql_script
def execute_sql(script_id, script_name, target_table, update_mode, schedule_frequency, **kwargs):
    """
    执行SQL脚本并返回执行结果
    
    参数:
        script_id: 脚本ID
        script_name: 脚本名称(数据库中的名称)
        target_table: 目标表名
        update_mode: 执行模式
        schedule_frequency: 执行频率
        **kwargs: 其他参数
    
    返回:
        bool: 脚本执行结果
    """
    # 获取执行日期
    logical_date = kwargs.get('logical_date', datetime.now())
    exec_date, local_logical_date = get_cn_exec_date(logical_date)
    
    # 添加详细日志
    logger.info(f"===== 开始执行SQL脚本 {script_id} =====")
    logger.info(f"script_id: {script_id}, 类型: {type(script_id)}")
    logger.info(f"script_name: {script_name}, 类型: {type(script_name)}")
    logger.info(f"target_table: {target_table}, 类型: {type(target_table)}")
    logger.info(f"update_mode: {update_mode}, 类型: {type(update_mode)}")
    logger.info(f"schedule_frequency: {schedule_frequency}, 类型: {type(schedule_frequency)}")
    logger.info(f"【时间参数】execute_sql: exec_date={exec_date}, logical_date={logical_date}, local_logical_date={local_logical_date}")

    # 记录额外参数
    for key, value in kwargs.items():
        logger.info(f"额外参数 - {key}: {value}, 类型: {type(value)}")

    # 记录执行开始时间
    start_time = datetime.now()

    try:
        # 导入和执行execution_sql模块
        import importlib.util
        import sys
        exec_sql_path = os.path.join(SCRIPTS_BASE_PATH, "execution_sql.py")

        # 对于SQL类型的脚本，我们不检查它是否作为文件存在
        # 但是我们需要检查execution_sql.py是否存在
        if not os.path.exists(exec_sql_path):
            logger.error(f"SQL执行脚本文件不存在: {exec_sql_path}")
            return False

        # 动态导入execution_sql模块
        try:
            spec = importlib.util.spec_from_file_location("execution_sql", exec_sql_path)
            exec_sql_module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(exec_sql_module)
            logger.info(f"成功导入 execution_sql 模块")
        except Exception as import_err:
            logger.error(f"导入 execution_sql 模块时出错: {str(import_err)}")
            import traceback
            logger.error(traceback.format_exc())
            return False

        # 检查并调用标准入口函数run
        if hasattr(exec_sql_module, "run"):
            logger.info(f"调用执行SQL脚本的标准入口函数 run()")
            
            # 构建完整的参数字典
            run_params = {
                "script_type": "sql",
                "target_table": target_table,
                "script_name": script_name,
                "exec_date": exec_date,
                "schedule_frequency": schedule_frequency,
                "target_table_label": kwargs.get('target_table_label', ''), # 传递目标表标签，用于ETL幂等性判断
                "update_mode": update_mode  # 传递执行模式参数
            }

            # 添加可能的额外参数
            for key in ['target_type', 'storage_location', 'source_tables']:
                if key in kwargs and kwargs[key] is not None:
                    run_params[key] = kwargs[key]

            # 调用execution_sql.py的run函数
            logger.info(f"调用SQL执行脚本的run函数并传递参数: {run_params}")
            result = exec_sql_module.run(**run_params)
            logger.info(f"SQL脚本执行完成，原始返回值: {result}, 类型: {type(result)}")

            # 确保result是布尔值
            if result is None:
                logger.warning(f"SQL脚本返回值为None，转换为False")
                result = False
            elif not isinstance(result, bool):
                original_result = result
                result = bool(result)
                logger.warning(f"SQL脚本返回非布尔值 {original_result}，转换为布尔值: {result}")

            # 记录结束时间和结果
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            logger.info(f"SQL脚本 {script_name} 执行完成，结果: {result}, 耗时: {duration:.2f}秒")

            return result
        else:
            logger.error(f"执行SQL脚本 execution_sql.py 中未定义标准入口函数 run()，无法执行")
            return False

    except Exception as e:
        # 处理异常
        logger.error(f"执行SQL脚本 {script_id} 出错: {str(e)}")
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        logger.error(f"SQL脚本 {script_name} 执行失败，耗时: {duration:.2f}秒")
        logger.info(f"===== SQL脚本执行异常结束 =====")
        import traceback
        logger.error(traceback.format_exc())
        
        # 确保不会阻塞DAG
        return False

# 使用execute_python函数代替之前的execute_python_script
def execute_python(script_id, script_name, target_table, update_mode, schedule_frequency, **kwargs):
    """
    执行Python脚本并返回执行结果
    
    参数:
        script_id: 脚本ID
        script_name: 脚本名称(数据库中的名称)
        target_table: 目标表名
        update_mode: 执行模式
        schedule_frequency: 执行频率
        **kwargs: 其他参数
    
    返回:
        bool: 脚本执行结果
    """
    # 获取执行日期
    logical_date = kwargs.get('logical_date', datetime.now())
    exec_date, local_logical_date = get_cn_exec_date(logical_date)
    
    # 添加详细日志
    logger.info(f"===== 开始执行Python脚本 {script_id} =====")
    logger.info(f"script_id: {script_id}, 类型: {type(script_id)}")
    logger.info(f"script_name: {script_name}, 类型: {type(script_name)}")
    logger.info(f"target_table: {target_table}, 类型: {type(target_table)}")
    logger.info(f"update_mode: {update_mode}, 类型: {type(update_mode)}")
    logger.info(f"schedule_frequency: {schedule_frequency}, 类型: {type(schedule_frequency)}")
    logger.info(f"【时间参数】execute_python: exec_date={exec_date}, logical_date={logical_date}, local_logical_date={local_logical_date}")

    # 记录额外参数
    for key, value in kwargs.items():
        logger.info(f"额外参数 - {key}: {value}, 类型: {type(value)}")

    # 记录执行开始时间
    start_time = datetime.now()

    try:
        # 导入和执行execution_python模块
        import importlib.util
        import sys
        exec_python_path = os.path.join(SCRIPTS_BASE_PATH, "execution_python.py")

        # 对于Python类型的脚本，我们不检查它是否作为文件存在
        # 但是我们需要检查execution_python.py是否存在
        if not os.path.exists(exec_python_path):
            logger.error(f"Python执行脚本文件不存在: {exec_python_path}")
            return False

        # 动态导入execution_python模块
        try:
            spec = importlib.util.spec_from_file_location("execution_python", exec_python_path)
            exec_python_module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(exec_python_module)
            logger.info(f"成功导入 execution_python 模块")
        except Exception as import_err:
            logger.error(f"导入 execution_python 模块时出错: {str(import_err)}")
            import traceback
            logger.error(traceback.format_exc())
            return False

        # 检查并调用标准入口函数run
        if hasattr(exec_python_module, "run"):
            logger.info(f"调用执行Python脚本的标准入口函数 run()")
            
            # 构建完整的参数字典
            run_params = {
                "script_type": "python",
                "target_table": target_table,
                "script_name": script_name,
                "exec_date": exec_date,
                "schedule_frequency": schedule_frequency,
                "target_table_label": kwargs.get('target_table_label', ''), # 传递目标表标签
                "update_mode": update_mode  # 传递执行模式参数
            }

            # 添加可能的额外参数
            for key in ['target_type', 'storage_location', 'source_tables']:
                if key in kwargs and kwargs[key] is not None:
                    run_params[key] = kwargs[key]

            # 调用execution_python.py的run函数
            logger.info(f"调用Python执行脚本的run函数并传递参数: {run_params}")
            result = exec_python_module.run(**run_params)
            logger.info(f"Python脚本执行完成，原始返回值: {result}, 类型: {type(result)}")

            # 确保result是布尔值
            if result is None:
                logger.warning(f"Python脚本返回值为None，转换为False")
                result = False
            elif not isinstance(result, bool):
                original_result = result
                result = bool(result)
                logger.warning(f"Python脚本返回非布尔值 {original_result}，转换为布尔值: {result}")

            # 记录结束时间和结果
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            logger.info(f"Python脚本 {script_name} 执行完成，结果: {result}, 耗时: {duration:.2f}秒")

            return result
        else:
            logger.error(f"执行Python脚本 execution_python.py 中未定义标准入口函数 run()，无法执行")
            return False

    except Exception as e:
        # 处理异常
        logger.error(f"执行Python脚本 {script_id} 出错: {str(e)}")
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        logger.error(f"Python脚本 {script_name} 执行失败，耗时: {duration:.2f}秒")
        logger.info(f"===== Python脚本执行异常结束 =====")
        import traceback
        logger.error(traceback.format_exc())
        
        # 确保不会阻塞DAG
        return False

#############################################
# 执行计划获取和处理函数
#############################################

def get_execution_plan_from_db(ds):
    """
    从数据库获取产品线执行计划
    
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
    
    logger.info(f"尝试从数据库获取执行日期 {ds} 的产品线执行计划")
    conn = get_pg_conn()
    cursor = conn.cursor()
    execution_plan = None
    
    try:
        # 查询条件a: 当前日期=表的exec_date，如果有多条记录，取logical_date最大的一条
        cursor.execute("""
            SELECT plan
            FROM airflow_exec_plans
            WHERE dag_id = 'dataops_productline_prepare_dag' AND exec_date = %s
            ORDER BY logical_date DESC
            LIMIT 1
        """, (ds,))
        result = cursor.fetchone()
        
        if result:
            # 获取计划
            plan_json = result[0]
            
            # 处理plan_json可能已经是dict的情况
            if isinstance(plan_json, dict):
                execution_plan = plan_json
            else:
                execution_plan = json.loads(plan_json)
                
            logger.info(f"找到当前日期 exec_date={ds} 的执行计划记录")
            return execution_plan
        
        # 查询条件b: 找不到当前日期的记录，查找exec_date<当前ds的最新记录
        logger.info(f"未找到当前日期 exec_date={ds} 的执行计划记录，尝试查找历史记录")
        cursor.execute("""
            SELECT plan, exec_date
            FROM airflow_exec_plans
            WHERE dag_id = 'dataops_productline_prepare_dag' AND exec_date < %s
            ORDER BY exec_date DESC, logical_date DESC
            LIMIT 1
        """, (ds,))
        result = cursor.fetchone()
        
        if result:
            # 获取计划和exec_date
            plan_json, plan_ds = result
            
            # 处理plan_json可能已经是dict的情况
            if isinstance(plan_json, dict):
                execution_plan = plan_json
            else:
                execution_plan = json.loads(plan_json)
                
            logger.info(f"找到历史执行计划记录，exec_date: {plan_ds}")
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

def check_execution_plan(**kwargs):
    """
    检查执行计划是否存在且有效
    返回False将阻止所有下游任务执行
    """
    dag_run = kwargs.get('dag_run')
    logical_date = dag_run.logical_date

    exec_date, local_logical_date = get_cn_exec_date(logical_date)
    
    # 检查是否是手动触发
    dag_run = kwargs['dag_run']
    logger.info(f"This DAG run was triggered via: {dag_run.run_type}")

    if dag_run.external_trigger:
        logger.info(f"【手动触发】当前DAG是手动触发的，使用传入的logical_date: {logical_date}")
       
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
        
    if not isinstance(execution_plan.get("scripts", []), list):
        logger.error("执行计划的scripts字段无效")
        return False
        
    if not isinstance(execution_plan.get("resource_scripts", []), list):
        logger.error("执行计划的resource_scripts字段无效")
        return False

    if not isinstance(execution_plan.get("model_scripts", []), list):
        logger.error("执行计划的model_scripts字段无效")
        return False
    
    # 检查是否有脚本数据
    scripts = execution_plan.get("scripts", [])
    resource_scripts = execution_plan.get("resource_scripts", [])
    model_scripts = execution_plan.get("model_scripts", [])
    
    logger.info(f"执行计划验证成功: 包含 {len(scripts)} 个脚本，{len(resource_scripts)} 个资源脚本和 {len(model_scripts)} 个模型脚本")
    
    # 保存执行计划到XCom以便下游任务使用
    kwargs['ti'].xcom_push(key='execution_plan', value=execution_plan)
    
    return True

def save_execution_plan_to_db(execution_plan, dag_id, run_id, logical_date, ds):
    """
    将执行计划保存到airflow_exec_plans表
    
    参数:
        execution_plan (dict): 执行计划字典
        dag_id (str): DAG的ID
        run_id (str): DAG运行的ID
        logical_date (datetime): 逻辑日期
        ds (str): 日期字符串，格式为YYYY-MM-DD
    
    返回:
        bool: 操作是否成功
    """
    try:
        conn = get_pg_conn()
        cursor = conn.cursor()
        
        try:
            # 将执行计划转换为JSON字符串
            plan_json = json.dumps(execution_plan)
            
            # 获取本地时间
            local_logical_date = pendulum.instance(logical_date).in_timezone('Asia/Shanghai')
            
            # 插入记录
            cursor.execute("""
                INSERT INTO airflow_exec_plans
                (dag_id, run_id, logical_date, local_logical_date, exec_date, plan)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (dag_id, run_id, logical_date, local_logical_date, ds, plan_json))
            
            conn.commit()
            logger.info(f"成功将执行计划保存到airflow_exec_plans表，dag_id={dag_id}, run_id={run_id}, exec_date={ds}")
            return True
        except Exception as e:
            logger.error(f"保存执行计划到数据库时出错: {str(e)}")
            conn.rollback()
            raise Exception(f"PostgreSQL保存执行计划失败: {str(e)}")
        finally:
            cursor.close()
            conn.close()
    except Exception as e:
        logger.error(f"连接PostgreSQL数据库失败: {str(e)}")
        raise Exception(f"无法连接PostgreSQL数据库: {str(e)}")

def generate_task_id(script_name, source_tables, target_table):
    """
    根据脚本名和表名生成唯一任务ID
    
    参数:
        script_name (str): 脚本文件名
        source_tables (list): 源表列表
        target_table (str): 目标表名
        
    返回:
        str: 唯一的任务ID
    """
    # 移除脚本名的文件扩展名
    script_base = os.path.splitext(script_name)[0]
    
    # 对于特殊脚本如load_file.py，直接使用目标表名
    if script_name.lower() in ['load_file.py']:
        return f"{script_base}_{target_table}"
    
    # 处理源表部分
    if source_tables:
        # 将所有源表按字母顺序排序并连接
        source_part = "_".join(sorted(source_tables))
        # 生成任务ID: 脚本名_源表_to_目标表
        return f"{script_base}_{source_part}_to_{target_table}"
    else:
        # 没有源表时，只使用脚本名和目标表
        return f"{script_base}_{target_table}"

def prepare_scripts_from_tables(tables_info):
    """
    将表信息转换为脚本信息
    
    参数:
        tables_info (list): 表信息列表
        
    返回:
        list: 脚本信息列表
    """
    scripts = []
    
    for table in tables_info:
        target_table = table['target_table']
        target_table_label = table.get('target_table_label')
        schedule_frequency = table.get('schedule_frequency')
        
        # 处理表的脚本信息
        if 'scripts_info' in table and table['scripts_info']:
            # 表有多个脚本
            for script_name, script_info in table['scripts_info'].items():
                source_tables = script_info.get('sources', [])
                script_type = script_info.get('script_type', 'python')
                update_mode = script_info.get('script_exec_mode', 'append')
                
                # 生成任务ID
                task_id = generate_task_id(script_name, source_tables, target_table)
                
                # 创建脚本信息
                script = {
                    "script_id": task_id,
                    "script_name": script_name,
                    "source_tables": source_tables,
                    "target_table": target_table,
                    "target_table_label": target_table_label,
                    "script_type": script_type,
                    "update_mode": update_mode,
                    "schedule_frequency": schedule_frequency,
                    "task_id": task_id
                }
                
                # 为structure类型添加特殊属性
                if table.get('target_type') == "structure":
                    script["target_type"] = "structure"
                    script["storage_location"] = table.get('storage_location')
                
                scripts.append(script)
                logger.info(f"为表 {target_table} 创建脚本 {script_name}，任务ID: {task_id}")
        else:
            # 表只有单个脚本或没有明确指定脚本信息
            script_name = table.get('script_name')
            
            # 如果没有script_name，使用默认值
            if not script_name:
                script_name = f"{target_table}_script.py"
                logger.warning(f"表 {target_table} 没有指定脚本名，使用默认值: {script_name}")
            
            source_tables = table.get('source_tables', [])
            script_type = table.get('script_type', 'python')
            update_mode = table.get('update_mode', 'append')
            
            # 生成任务ID
            task_id = generate_task_id(script_name, source_tables, target_table)
            
            # 创建脚本信息
            script = {
                "script_id": task_id,
                "script_name": script_name,
                "source_tables": source_tables,
                "target_table": target_table,
                "target_table_label": target_table_label,
                "script_type": script_type,
                "update_mode": update_mode,
                "schedule_frequency": schedule_frequency,
                "task_id": task_id
            }
            
            # 为structure类型添加特殊属性
            if table.get('target_type') == "structure":
                script["target_type"] = "structure"
                script["storage_location"] = table.get('storage_location')
            
            scripts.append(script)
            logger.info(f"为表 {target_table} 创建脚本 {script_name}，任务ID: {task_id}")
    
    return scripts

def build_script_dependency_graph(scripts):
    """
    处理脚本间的依赖关系
    
    参数:
        scripts (list): 脚本信息列表
        
    返回:
        tuple: (依赖关系字典, 图对象)
    """
    # 打印所有脚本的源表信息，用于调试
    logger.info("构建脚本依赖图，当前脚本信息:")
    for script in scripts:
        script_id = script['script_id']
        script_name = script['script_name']
        target_table = script['target_table']
        source_tables = script['source_tables']
        logger.info(f"脚本: {script_id} ({script_name}), 目标表: {target_table}, 源表: {source_tables}")
    
    # 创建目标表到脚本ID的映射
    table_to_scripts = {}
    for script in scripts:
        target_table = script['target_table']
        if target_table not in table_to_scripts:
            table_to_scripts[target_table] = []
        table_to_scripts[target_table].append(script['script_id'])
    
    # 记录表到脚本的映射关系
    logger.info("表到脚本的映射关系:")
    for table, script_ids in table_to_scripts.items():
        logger.info(f"表 {table} 由脚本 {script_ids} 生成")
    
    # 创建脚本依赖关系
    script_dependencies = {}
    for script in scripts:
        script_id = script['script_id']
        source_tables = script['source_tables']
        target_table = script['target_table']
        
        # 初始化依赖列表
        script_dependencies[script_id] = []
        
        # 查找源表对应的脚本
        if source_tables:
            logger.info(f"处理脚本 {script_id} 的依赖关系，源表: {source_tables}")
            for source_table in source_tables:
                if source_table in table_to_scripts:
                    # 添加所有生成源表的脚本作为依赖
                    for source_script_id in table_to_scripts[source_table]:
                        if source_script_id != script_id:  # 避免自我依赖
                            script_dependencies[script_id].append(source_script_id)
                            logger.info(f"添加依赖: {script_id} 依赖于 {source_script_id} (表 {target_table} 依赖于表 {source_table})")
                else:
                    logger.warning(f"源表 {source_table} 没有对应的脚本，无法为脚本 {script_id} 创建依赖")
        else:
            logger.info(f"脚本 {script_id} 没有源表依赖")
    
    # 尝试从Neo4j额外查询依赖关系（如果脚本没有显式的source_tables）
    try:
        driver = get_neo4j_driver()
    except Exception as e:
        logger.error(f"连接Neo4j数据库失败: {str(e)}")
        raise Exception(f"无法连接Neo4j数据库: {str(e)}")
    
    try:
        with driver.session() as session:
            # 验证连接
            try:
                test_result = session.run("RETURN 1 as test")
                test_record = test_result.single()
                if not test_record or test_record.get("test") != 1:
                    logger.error("Neo4j连接测试失败")
                    raise Exception("Neo4j连接测试失败")
            except Exception as e:
                logger.error(f"Neo4j连接测试失败: {str(e)}")
                raise Exception(f"Neo4j连接测试失败: {str(e)}")
                
            for script in scripts:
                script_id = script['script_id']
                target_table = script['target_table']
                
                # 只处理没有源表的脚本
                if not script['source_tables'] and not script_dependencies[script_id]:
                    logger.info(f"脚本 {script_id} 没有源表，尝试从Neo4j直接查询表 {target_table} 的依赖")
                    
                    # 查询表的直接依赖
                    query = """
                        MATCH (target {en_name: $table_name})-[rel]->(dep)
                        RETURN dep.en_name AS dep_name
                    """
                    
                    try:
                        result = session.run(query, table_name=target_table)
                        records = list(result)
                        
                        for record in records:
                            dep_name = record.get("dep_name")
                            if dep_name and dep_name in table_to_scripts:
                                for dep_script_id in table_to_scripts[dep_name]:
                                    if dep_script_id != script_id:  # 避免自我依赖
                                        script_dependencies[script_id].append(dep_script_id)
                                        logger.info(f"从Neo4j添加额外依赖: {script_id} 依赖于 {dep_script_id} (表 {target_table} 依赖于表 {dep_name})")
                    except Exception as e:
                        logger.warning(f"从Neo4j查询表 {target_table} 依赖时出错: {str(e)}")
                        raise Exception(f"Neo4j查询表依赖失败: {str(e)}")
    except Exception as e:
        if "Neo4j" in str(e):
            # 已经处理过的错误，直接抛出
            raise
        else:
            logger.error(f"访问Neo4j获取额外依赖时出错: {str(e)}")
            raise Exception(f"Neo4j依赖查询失败: {str(e)}")
    finally:
        driver.close()
    
    # 构建依赖图
    G = nx.DiGraph()
    
    # 添加所有脚本作为节点
    for script in scripts:
        G.add_node(script['script_id'])
    
    # 添加依赖边
    for script_id, dependencies in script_dependencies.items():
        if dependencies:
            for dep_id in dependencies:
                # 添加从script_id到dep_id的边，表示script_id依赖于dep_id
                G.add_edge(script_id, dep_id)
                logger.debug(f"添加依赖边: {script_id} -> {dep_id}")
        else:
            logger.info(f"脚本 {script_id} 没有依赖的上游脚本")
    
    # 确保所有脚本ID都在依赖关系字典中
    for script in scripts:
        script_id = script['script_id']
        if script_id not in script_dependencies:
            script_dependencies[script_id] = []
    
    # 记录每个脚本的依赖数量
    for script_id, deps in script_dependencies.items():
        logger.info(f"脚本 {script_id} 有 {len(deps)} 个依赖: {deps}")
    
    return script_dependencies, G

def optimize_script_execution_order(scripts, script_dependencies, G):
    """
    使用NetworkX优化脚本执行顺序
    
    参数:
        scripts (list): 脚本信息列表
        script_dependencies (dict): 脚本依赖关系字典
        G (nx.DiGraph): 依赖图对象
        
    返回:
        list: 优化后的脚本执行顺序（脚本ID列表）
    """
    # 检查是否有循环依赖
    try:
        cycles = list(nx.simple_cycles(G))
        if cycles:
            logger.warning(f"检测到循环依赖: {cycles}")
            # 处理循环依赖，可以通过删除一些边来打破循环
            for cycle in cycles:
                # 选择一条边删除，这里简单地选择第一条边
                if len(cycle) > 1:
                    G.remove_edge(cycle[0], cycle[1])
                    logger.warning(f"删除边 {cycle[0]} -> {cycle[1]} 以打破循环")
    except Exception as e:
        logger.error(f"检测循环依赖时出错: {str(e)}")
    
    # 使用拓扑排序获取执行顺序
    try:
        # 反转图，因为我们的边表示"依赖于"关系，而拓扑排序需要"优先于"关系
        reverse_G = G.reverse()
        execution_order = list(nx.topological_sort(reverse_G))
        
        # 反转结果，使上游任务先执行
        execution_order.reverse()
        
        logger.info(f"生成优化的脚本执行顺序: {execution_order}")
        return execution_order
    except Exception as e:
        logger.error(f"生成脚本执行顺序时出错: {str(e)}")
        # 出错时返回原始脚本ID列表，不进行优化
        return [script['script_id'] for script in scripts] 
    

def create_execution_plan(**kwargs):
    """
    创建或获取执行计划
    """
    try:
        dag_run = kwargs.get('dag_run')
        logical_date = dag_run.logical_date
        exec_date, local_logical_date = get_cn_exec_date(logical_date)

        logger.info(f"This DAG run was triggered via: {dag_run.run_type}")        
        # 检查是否是手动触发
        if dag_run.external_trigger:
            logger.info(f"【手动触发】当前DAG是手动触发的，使用传入的logical_date: {logical_date}")
        
        # 记录重要的时间参数
        logger.info(f"【时间参数】create_execution_plan: exec_date={exec_date}, logical_date={logical_date}")
        
        # 从XCom获取执行计划
        execution_plan = kwargs['ti'].xcom_pull(task_ids='check_execution_plan', key='execution_plan')
        
        # 如果找不到执行计划，则从数据库获取
        if not execution_plan:
            logger.info(f"未从XCom中找到执行计划，从数据库获取。使用执行日期: {exec_date}")
            execution_plan = get_execution_plan_from_db(exec_date)
            
            if not execution_plan:
                logger.error(f"执行日期 {exec_date} 没有找到执行计划")
                return None
        
        # 验证执行计划结构
        scripts = execution_plan.get("scripts", [])
        script_dependencies = execution_plan.get("script_dependencies", {})
        execution_order = execution_plan.get("execution_order", [])
        
        # 如果执行计划中没有execution_order或为空，使用NetworkX优化
        if not execution_order:
            logger.info("执行计划中没有execution_order，使用NetworkX进行优化")
            execution_order = optimize_script_execution_order(scripts, script_dependencies)
            execution_plan["execution_order"] = execution_order
        
        # 保存完整的执行计划到XCom
        kwargs['ti'].xcom_push(key='full_execution_plan', value=execution_plan)
        
        logger.info(f"成功处理执行计划，包含 {len(scripts)} 个脚本")
        return execution_plan
    except Exception as e:
        logger.error(f"创建执行计划时出错: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return None

# 创建DAG
with DAG(
    "dataops_productline_execute_dag", 
    start_date=datetime(2024, 1, 1), 
    schedule_interval="@daily",  # 设置为每日调度
    catchup=False,
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    params={"TRIGGERED_VIA_UI": True},# 触发 UI 弹出配置页面
) as dag:
    
    # 记录DAG实例化时的重要信息
    now = datetime.now()
    now_with_tz = now.replace(tzinfo=pytz.timezone('Asia/Shanghai'))
    default_exec_date = get_today_date()
    logger.info(f"【DAG初始化】当前时间: {now} / {now_with_tz}, 默认执行日期(用于初始化,非实际执行日期): {default_exec_date}")
    
    #############################################
    # 准备阶段: 检查并创建执行计划
    #############################################
    with TaskGroup("prepare_phase") as prepare_group:
        # 检查执行计划是否存在
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
        
        # 设置任务依赖
        check_plan >> create_plan
    
    #############################################
    # 执行阶段: 按依赖关系执行脚本
    #############################################
    with TaskGroup("execution_phase") as execution_group:
        try:
            # 获取当前DAG的执行日期
            exec_date = get_today_date()  # 使用当天日期作为默认值
            logger.info(f"当前DAG执行日期 ds={exec_date}，尝试从数据库获取执行计划")
            
            # 从数据库获取执行计划
            execution_plan = get_execution_plan_from_db(exec_date)
            
            # 检查是否成功获取到执行计划
            if execution_plan is None:
                error_msg = f"无法从数据库获取有效的执行计划，当前DAG exec_date={exec_date}"
                logger.error(error_msg)
                # 使用全局变量而不是异常来强制DAG失败
                raise ValueError(error_msg)
            
            # 提取信息
            exec_date = execution_plan.get("exec_date", exec_date)
            scripts = execution_plan.get("scripts", [])
            script_dependencies = execution_plan.get("script_dependencies", {})
            execution_order = execution_plan.get("execution_order", [])
            
            # 如果执行计划中没有execution_order或为空，使用NetworkX优化
            if not execution_order:
                logger.info("执行计划中没有execution_order，使用NetworkX进行优化")
                execution_order = optimize_script_execution_order(scripts, script_dependencies, nx.DiGraph())
            
            logger.info(f"执行计划: exec_date={exec_date}, scripts数量={len(scripts)}")
            
            # 如果执行计划为空（没有脚本），也应该失败
            if not scripts:
                error_msg = f"执行计划中没有任何脚本，当前DAG exec_date={exec_date}"
                logger.error(error_msg)
                raise ValueError(error_msg)
            
            # 1. 创建开始和结束任务
            start_execution = EmptyOperator(
                task_id="start_execution"
            )
            
            execution_completed = EmptyOperator(
                task_id="execution_completed",
                trigger_rule="none_failed_min_one_success"  # 只要有一个任务成功且没有失败的任务就标记为完成
            )
            
            # 创建脚本任务字典，用于管理任务依赖
            task_dict = {}
            
            # 2. 先创建所有脚本任务，不设置依赖关系
            for script in scripts:
                script_id = script['script_id']
                script_name = script.get("script_name")
                target_table = script.get("target_table")
                script_type = script.get("script_type", "python")
                update_mode = script.get("update_mode", "append")
                source_tables = script.get("source_tables", [])
                target_table_label = script.get("target_table_label", "")
                
                # 使用描述性的任务ID，包含脚本名称和目标表
                # 提取文件名
                if "/" in script_name:
                    script_file = script_name.split("/")[-1]  # 获取文件名部分
                else:
                    script_file = script_name
                
                # 确保任务ID不包含不允许的特殊字符
                safe_script_name = script_file.replace(" ", "_")
                safe_target_table = target_table.replace("-", "_").replace(" ", "_")
                
                # 按照指定格式创建任务ID
                task_id = f"{safe_script_name}-TO-{safe_target_table}"
                
                # 构建op_kwargs参数
                op_kwargs = {
                    "script_id": script_id,
                    "script_name": script_name,
                    "target_table": target_table,
                    "update_mode": update_mode,
                    "source_tables": source_tables,
                    "schedule_frequency": script.get("schedule_frequency", "daily"),
                    "target_table_label": target_table_label,
                    # logical_date会在任务执行时由Airflow自动添加
                }
                
                # 添加特殊参数（如果有）
                for key in ['target_type', 'storage_location']:
                    if key in script and script[key] is not None:
                        op_kwargs[key] = script[key]
                
                # 根据脚本类型和目标表标签选择执行函数
                if script_type.lower() == 'sql' and target_table_label == 'DataModel':
                    # 使用SQL脚本执行函数
                    logger.info(f"脚本 {script_id} 是SQL类型且目标表标签为DataModel，使用execute_sql函数执行")
                    python_callable = execute_sql
                elif script_type.lower() == 'python' and target_table_label == 'DataModel':
                    # 使用Python脚本执行函数
                    logger.info(f"脚本 {script_id} 是Python类型且目标表标签为DataModel，使用execute_python函数执行")
                    python_callable = execute_python
                elif script_type.lower() == 'python_script':
                    # 使用Python脚本文件执行函数
                    logger.info(f"脚本 {script_id} 是python_script类型，使用execute_python_script函数执行")
                    python_callable = execute_python_script
                else:
                    # 默认使用Python脚本文件执行函数
                    logger.warning(f"未识别的脚本类型 {script_type}，使用默认execute_python_script函数执行")
                    python_callable = execute_python_script
                    
                # 创建任务
                script_task = PythonOperator(
                    task_id=task_id,
                    python_callable=python_callable,
                    op_kwargs=op_kwargs,
                    retries=TASK_RETRY_CONFIG["retries"],
                    retry_delay=timedelta(minutes=TASK_RETRY_CONFIG["retry_delay_minutes"])
                )
                
                # 将任务添加到字典
                task_dict[script_id] = script_task
            
            # 3. 设置开始任务与所有无依赖的脚本任务的关系
            no_dep_scripts = []
            for script_id, dependencies in script_dependencies.items():
                if not dependencies:  # 如果没有依赖
                    if script_id in task_dict:
                        no_dep_scripts.append(script_id)
                        start_execution >> task_dict[script_id]
                        logger.info(f"设置无依赖脚本: start_execution >> {script_id}")
            
            # 4. 设置脚本间的依赖关系
            for script_id, dependencies in script_dependencies.items():
                for dep_id in dependencies:
                    if script_id in task_dict and dep_id in task_dict:
                        # 正确的依赖关系：依赖任务 >> 当前任务
                        task_dict[dep_id] >> task_dict[script_id]
                        logger.info(f"设置脚本依赖: {dep_id} >> {script_id}")
            
            # 5. 找出所有叶子节点（没有下游任务的节点）并连接到execution_completed
            # 首先，构建一个下游节点集合
            has_downstream = set()
            for script_id, dependencies in script_dependencies.items():
                for dep_id in dependencies:
                    has_downstream.add(dep_id)
            
            # 然后，找出没有下游节点的任务
            leaf_nodes = []
            for script_id in task_dict:
                if script_id not in has_downstream:
                    leaf_nodes.append(script_id)
                    task_dict[script_id] >> execution_completed
                    logger.info(f"将叶子节点连接到completion: {script_id} >> execution_completed")
            
            # 如果没有找到叶子节点，则将所有任务都连接到completion
            if not leaf_nodes:
                logger.warning("未找到叶子节点，将所有任务连接到completion")
                for script_id, task in task_dict.items():
                    task >> execution_completed
            
            # 设置TaskGroup与prepare_phase的依赖关系
            prepare_group >> start_execution
            
            logger.info(f"成功创建 {len(task_dict)} 个脚本执行任务")
            
        except Exception as e:
            logger.error(f"加载执行计划或创建任务时出错: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())

    # 添加触发finalize DAG的任务
    from airflow.operators.trigger_dagrun import TriggerDagRunOperator
    
    trigger_finalize_dag = TriggerDagRunOperator(
        task_id="trigger_finalize_dag",
        trigger_dag_id="dataops_productline_finalize_dag",
        conf={"execution_date": "{{ ds }}", "parent_execution_date": "{{ execution_date }}", "parent_run_id": "{{ run_id }}"},
        reset_dag_run=True,
        wait_for_completion=False,
        poke_interval=60,
    )
    
    # 设置依赖关系，确保执行阶段完成后触发finalize DAG
    execution_group >> trigger_finalize_dag

logger.info(f"DAG dataops_productline_execute_dag 定义完成")