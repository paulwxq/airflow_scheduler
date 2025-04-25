"""
统一数据产品线执行器 DAG

功能：
1. 面向脚本的作业编排，不再是面向表
2. 基于dataops_productline_prepare_dag生成的执行计划执行脚本
3. 支持对脚本执行顺序的优化
4. 提供详细的执行日志和错误处理
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
    

def get_cn_exec_date(logical_date):
    """
    获取逻辑执行日期
    
    参数:
        logical_date: 逻辑执行日期，UTC时间

    返回:
        logical_exec_date: 逻辑执行日期，北京时间
        local_logical_date: 北京时区的logical_date
    """
    # 获取逻辑执行日期
    local_logical_date = pendulum.instance(logical_date).in_timezone('Asia/Shanghai')
    exec_date = local_logical_date.strftime('%Y-%m-%d')
    return exec_date, local_logical_date


#############################################
# 脚本执行函数
#############################################

def execute_python_script(script_id, script_name, target_table, script_exec_mode, frequency, **kwargs):
    """
    执行Python脚本文件并返回执行结果
    
    参数:
        script_id: 脚本ID
        script_name: 脚本文件名（.py文件）
        target_table: 目标表名
        script_exec_mode: 执行模式
        frequency: 执行频率
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
    logger.info(f"script_exec_mode: {script_exec_mode}, 类型: {type(script_exec_mode)}")
    logger.info(f"frequency: {frequency}, 类型: {type(frequency)}")
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
                "execution_mode": script_exec_mode,
                "exec_date": exec_date,
                "frequency": frequency
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
def execute_sql(script_id, script_name, target_table, script_exec_mode, frequency, **kwargs):
    """
    执行SQL脚本并返回执行结果
    
    参数:
        script_id: 脚本ID
        script_name: 脚本名称(数据库中的名称)
        target_table: 目标表名
        script_exec_mode: 执行模式
        frequency: 执行频率
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
    logger.info(f"script_exec_mode: {script_exec_mode}, 类型: {type(script_exec_mode)}")
    logger.info(f"frequency: {frequency}, 类型: {type(frequency)}")
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
                "frequency": frequency,
                "target_table_label": kwargs.get('target_table_label', ''), # 传递目标表标签，用于ETL幂等性判断
                "execution_mode": script_exec_mode  # 传递执行模式参数
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
def execute_python(script_id, script_name, target_table, script_exec_mode, frequency, **kwargs):
    """
    执行Python脚本并返回执行结果
    
    参数:
        script_id: 脚本ID
        script_name: 脚本名称(数据库中的名称)
        target_table: 目标表名
        script_exec_mode: 执行模式
        frequency: 执行频率
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
    logger.info(f"script_exec_mode: {script_exec_mode}, 类型: {type(script_exec_mode)}")
    logger.info(f"frequency: {frequency}, 类型: {type(frequency)}")
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
                "frequency": frequency,
                "target_table_label": kwargs.get('target_table_label', ''), # 传递目标表标签
                "execution_mode": script_exec_mode  # 传递执行模式参数
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
    is_manual_trigger = dag_run.conf.get('MANUAL_TRIGGER', False) if dag_run.conf else False
    if is_manual_trigger:
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
        
    if not isinstance(execution_plan.get("script_dependencies", {}), dict):
        logger.error("执行计划的script_dependencies字段无效")
        return False
    
    # 检查是否有脚本数据
    scripts = execution_plan.get("scripts", [])
    
    if not scripts:
        logger.warning("执行计划不包含任何脚本")
        # 如果没有脚本，则阻止下游任务执行
        return False
    
    logger.info(f"执行计划验证成功: 包含 {len(scripts)} 个脚本")
    
    # 保存执行计划到XCom以便下游任务使用
    kwargs['ti'].xcom_push(key='execution_plan', value=execution_plan)
    
    return True

def optimize_execution_order(scripts, script_dependencies):
    """
    使用NetworkX优化脚本执行顺序
    
    参数:
        scripts (list): 脚本信息列表
        script_dependencies (dict): 脚本依赖关系字典
        
    返回:
        list: 优化后的脚本执行顺序（脚本ID列表）
    """
    logger.info("开始使用NetworkX优化脚本执行顺序")
    
    # 构建依赖图
    G = nx.DiGraph()
    
    # 添加所有脚本作为节点
    for script in scripts:
        script_id = script['script_id']
        G.add_node(script_id)
    
    # 添加依赖边
    for script_id, dependencies in script_dependencies.items():
        for dep_id in dependencies:
            # 添加从script_id到dep_id的边，表示script_id依赖于dep_id
            G.add_edge(script_id, dep_id)
            logger.debug(f"添加依赖边: {script_id} -> {dep_id}")
    
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
        
        logger.info(f"NetworkX优化后的脚本执行顺序: {execution_order}")
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
        
        # 检查是否是手动触发
        is_manual_trigger = dag_run.conf.get('MANUAL_TRIGGER', False) if dag_run.conf else False
        if is_manual_trigger:
            logger.info(f"【手动触发】当前DAG是手动触发的，使用传入的logical_date: {logical_date}")
        
        # 记录重要的时间参数
        logger.info(f"【时间参数】create_execution_plan: exec_date={exec_date}, logical_date={logical_date}")
        
        # 从XCom获取执行计划
        execution_plan = kwargs['ti'].xcom_pull(task_ids='check_execution_plan', key='execution_plan')
        
        # 如果找不到执行计划，则从数据库获取
        if not execution_plan:
            logger.info(f"未找到执行计划，从数据库获取。使用执行日期: {exec_date}")
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
            execution_order = optimize_execution_order(scripts, script_dependencies)
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
    params={
        'MANUAL_TRIGGER': False, 
    }
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
                execution_order = optimize_execution_order(scripts, script_dependencies)
            
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
                script_exec_mode = script.get("script_exec_mode", "append")
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
                    "script_exec_mode": script_exec_mode,
                    "source_tables": source_tables,
                    "frequency": script.get("frequency", "daily"),  # 显式添加frequency参数
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