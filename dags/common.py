# common.py
import psycopg2
from neo4j import GraphDatabase
import logging
import importlib.util
from pathlib import Path
import networkx as nx
import os
from datetime import datetime, timedelta
from config import PG_CONFIG, NEO4J_CONFIG, SCRIPTS_BASE_PATH
import functools
import time

# 创建统一的日志记录器
logger = logging.getLogger("airflow.task")

def get_pg_conn():
    """获取PostgreSQL连接"""
    return psycopg2.connect(**PG_CONFIG)

def get_neo4j_driver():
    """获取Neo4j连接驱动"""
    uri = NEO4J_CONFIG['uri']
    auth = (NEO4J_CONFIG['user'], NEO4J_CONFIG['password'])
    return GraphDatabase.driver(uri, auth=auth)

def update_task_start_time(exec_date, target_table, script_name, start_time):
    """更新任务开始时间"""
    logger.info(f"===== 更新任务开始时间 =====")
    logger.info(f"参数: exec_date={exec_date} ({type(exec_date).__name__}), target_table={target_table}, script_name={script_name}")
    
    conn = get_pg_conn()
    cursor = conn.cursor()
    try:
        # 首先检查记录是否存在
        cursor.execute("""
            SELECT COUNT(*) 
            FROM airflow_dag_schedule 
            WHERE exec_date = %s AND target_table = %s AND script_name = %s
        """, (exec_date, target_table, script_name))
        count = cursor.fetchone()[0]
        logger.info(f"查询到符合条件的记录数: {count}")
        
        if count == 0:
            logger.warning(f"未找到匹配的记录: exec_date={exec_date}, target_table={target_table}, script_name={script_name}")
            logger.info("尝试记录在airflow_dag_schedule表中找到的记录:")
            cursor.execute("""
                SELECT exec_date, target_table, script_name
                FROM airflow_dag_schedule
                LIMIT 5
            """)
            sample_records = cursor.fetchall()
            for record in sample_records:
                logger.info(f"样本记录: exec_date={record[0]} ({type(record[0]).__name__}), target_table={record[1]}, script_name={record[2]}")
        
        # 执行更新
        sql = """
            UPDATE airflow_dag_schedule 
            SET exec_start_time = %s
            WHERE exec_date = %s AND target_table = %s AND script_name = %s
        """
        logger.info(f"执行SQL: {sql}")
        logger.info(f"参数: start_time={start_time}, exec_date={exec_date}, target_table={target_table}, script_name={script_name}")
        
        cursor.execute(sql, (start_time, exec_date, target_table, script_name))
        affected_rows = cursor.rowcount
        logger.info(f"更新影响的行数: {affected_rows}")
        
        conn.commit()
        logger.info("事务已提交")
    except Exception as e:
        logger.error(f"更新任务开始时间失败: {str(e)}")
        import traceback
        logger.error(f"错误堆栈: {traceback.format_exc()}")
        conn.rollback()
        logger.info("事务已回滚")
        raise
    finally:
        cursor.close()
        conn.close()
        logger.info("数据库连接已关闭")
        logger.info("===== 更新任务开始时间完成 =====")

def update_task_completion(exec_date, target_table, script_name, success, end_time, duration):
    """更新任务完成信息"""
    logger.info(f"===== 更新任务完成信息 =====")
    logger.info(f"参数: exec_date={exec_date} ({type(exec_date).__name__}), target_table={target_table}, script_name={script_name}")
    logger.info(f"参数: success={success} ({type(success).__name__}), end_time={end_time}, duration={duration}")
    
    conn = get_pg_conn()
    cursor = conn.cursor()
    try:
        # 首先检查记录是否存在
        cursor.execute("""
            SELECT COUNT(*) 
            FROM airflow_dag_schedule 
            WHERE exec_date = %s AND target_table = %s AND script_name = %s
        """, (exec_date, target_table, script_name))
        count = cursor.fetchone()[0]
        logger.info(f"查询到符合条件的记录数: {count}")
        
        if count == 0:
            logger.warning(f"未找到匹配的记录: exec_date={exec_date}, target_table={target_table}, script_name={script_name}")
            # 查询表中前几条记录作为参考
            cursor.execute("""
                SELECT exec_date, target_table, script_name
                FROM airflow_dag_schedule
                LIMIT 5
            """)
            sample_records = cursor.fetchall()
            logger.info("airflow_dag_schedule表中的样本记录:")
            for record in sample_records:
                logger.info(f"样本记录: exec_date={record[0]} ({type(record[0]).__name__}), target_table={record[1]}, script_name={record[2]}")
        
        # 确保success是布尔类型
        if not isinstance(success, bool):
            original_success = success
            success = bool(success)
            logger.warning(f"success参数不是布尔类型，原始值: {original_success}，转换为: {success}")
        
        # 执行更新
        sql = """
            UPDATE airflow_dag_schedule 
            SET exec_result = %s, exec_end_time = %s, exec_duration = %s
            WHERE exec_date = %s AND target_table = %s AND script_name = %s
        """
        logger.info(f"执行SQL: {sql}")
        logger.info(f"参数: success={success}, end_time={end_time}, duration={duration}, exec_date={exec_date}, target_table={target_table}, script_name={script_name}")
        
        cursor.execute(sql, (success, end_time, duration, exec_date, target_table, script_name))
        affected_rows = cursor.rowcount
        logger.info(f"更新影响的行数: {affected_rows}")
        
        if affected_rows == 0:
            logger.warning("更新操作没有影响任何行，可能是因为条件不匹配")
            # 尝试用不同格式的exec_date查询
            if isinstance(exec_date, str):
                try:
                    # 尝试解析日期字符串
                    from datetime import datetime
                    parsed_date = datetime.strptime(exec_date, "%Y-%m-%d").date()
                    logger.info(f"尝试使用解析后的日期格式: {parsed_date}")
                    
                    cursor.execute("""
                        SELECT COUNT(*) 
                        FROM airflow_dag_schedule 
                        WHERE exec_date = %s AND target_table = %s AND script_name = %s
                    """, (parsed_date, target_table, script_name))
                    parsed_count = cursor.fetchone()[0]
                    logger.info(f"使用解析日期后查询到的记录数: {parsed_count}")
                    
                    if parsed_count > 0:
                        # 尝试用解析的日期更新
                        cursor.execute("""
                            UPDATE airflow_dag_schedule 
                            SET exec_result = %s, exec_end_time = %s, exec_duration = %s
                            WHERE exec_date = %s AND target_table = %s AND script_name = %s
                        """, (success, end_time, duration, parsed_date, target_table, script_name))
                        new_affected_rows = cursor.rowcount
                        logger.info(f"使用解析日期后更新影响的行数: {new_affected_rows}")
                except Exception as parse_e:
                    logger.error(f"尝试解析日期格式时出错: {str(parse_e)}")
        
        conn.commit()
        logger.info("事务已提交")
    except Exception as e:
        logger.error(f"更新任务完成信息失败: {str(e)}")
        import traceback
        logger.error(f"错误堆栈: {traceback.format_exc()}")
        conn.rollback()
        logger.info("事务已回滚")
        raise
    finally:
        cursor.close()
        conn.close()
        logger.info("数据库连接已关闭")
        logger.info("===== 更新任务完成信息完成 =====")

def execute_with_monitoring(target_table, script_name, script_exec_mode, exec_date, **kwargs):
    """执行脚本并监控执行情况"""

    # 添加详细日志
    logger.info(f"===== 开始监控执行 =====")
    logger.info(f"target_table: {target_table}, 类型: {type(target_table)}")
    logger.info(f"script_name: {script_name}, 类型: {type(script_name)}")
    logger.info(f"script_exec_mode: {script_exec_mode}, 类型: {type(script_exec_mode)}")
    logger.info(f"exec_date: {exec_date}, 类型: {type(exec_date)}")

    # 检查script_name是否为空
    if not script_name:
        logger.error(f"表 {target_table} 的script_name为空，无法执行")
        # 记录执行失败
        now = datetime.now()
        update_task_completion(exec_date, target_table, script_name or "", False, now, 0)
        return False
    # 记录执行开始时间
    start_time = datetime.now()
    
    # 尝试更新开始时间并记录结果
    try:
        update_task_start_time(exec_date, target_table, script_name, start_time)
        logger.info(f"成功更新任务开始时间: {start_time}")
    except Exception as e:
        logger.error(f"更新任务开始时间失败: {str(e)}")
    
    try:
        # 执行实际脚本
        logger.info(f"开始执行脚本: {script_name}")
        result = execute_script(script_name, target_table, script_exec_mode)
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
        
        # 尝试更新完成状态并记录结果
        try:
            logger.info(f"尝试更新完成状态: result={result}, end_time={end_time}, duration={duration}")
            update_task_completion(exec_date, target_table, script_name, result, end_time, duration)
            logger.info(f"成功更新任务完成状态，结果: {result}")
        except Exception as e:
            logger.error(f"更新任务完成状态失败: {str(e)}")
        
        logger.info(f"===== 监控执行完成 =====")
        return result
    except Exception as e:
        # 处理异常
        logger.error(f"执行任务出错: {str(e)}")
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        # 尝试更新失败状态并记录结果
        try:
            logger.info(f"尝试更新失败状态: end_time={end_time}, duration={duration}")
            update_task_completion(exec_date, target_table, script_name, False, end_time, duration)
            logger.info(f"成功更新任务失败状态")
        except Exception as update_e:
            logger.error(f"更新任务失败状态失败: {str(update_e)}")
        
        logger.info(f"===== 监控执行异常结束 =====")
        raise e

def ensure_boolean_result(func):
    """装饰器：确保函数返回布尔值"""
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            result = func(*args, **kwargs)
            logger.debug(f"脚本原始返回值: {result} (类型: {type(result).__name__})")
            
            # 处理None值
            if result is None:
                logger.warning(f"脚本函数 {func.__name__} 返回了None，默认设置为False")
                return False
                
            # 处理非布尔值
            if not isinstance(result, bool):
                try:
                    # 尝试转换为布尔值
                    bool_result = bool(result)
                    logger.warning(f"脚本函数 {func.__name__} 返回非布尔值 {result}，已转换为布尔值 {bool_result}")
                    return bool_result
                except Exception as e:
                    logger.error(f"无法将脚本返回值 {result} 转换为布尔值: {str(e)}")
                    return False
            
            return result
        except Exception as e:
            logger.error(f"脚本函数 {func.__name__} 执行出错: {str(e)}")
            return False
    return wrapper

def execute_script(script_path=None, script_name=None, script_exec_mode=None, table_name=None, execution_mode=None, args=None):
    """
    执行指定的脚本，并返回执行结果
    支持两种调用方式:
    1. execute_script(script_path, script_name, script_exec_mode, args={})
    2. execute_script(script_name, table_name, execution_mode)
    """
    # 确定调用方式并统一参数
    if script_path and script_name and script_exec_mode is not None:
        # 第一种调用方式
        if args is None:
            args = {}
    elif script_name and table_name and execution_mode is not None:
        # 第二种调用方式
        script_path = os.path.join(SCRIPTS_BASE_PATH, f"{script_name}.py")
        script_exec_mode = execution_mode
        args = {"table_name": table_name}
    else:
        logger.error("参数不正确，无法执行脚本")
        return False

    try:
        # 确保脚本路径存在
        if not os.path.exists(script_path):
            logger.error(f"脚本路径 {script_path} 不存在")
            return False

        # 加载脚本模块
        spec = importlib.util.spec_from_file_location("script_module", script_path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        
        # 检查并记录所有可用的函数
        module_functions = [f for f in dir(module) if callable(getattr(module, f)) and not f.startswith('_')]
        logger.debug(f"模块 {script_name} 中的可用函数: {module_functions}")

        # 获取脚本的运行函数
        if not hasattr(module, "run"):
            logger.error(f"脚本 {script_name} 没有run函数")
            return False

        # 装饰run函数，确保返回布尔值
        original_run = module.run
        module.run = ensure_boolean_result(original_run)
        
        logger.info(f"开始执行脚本 {script_name}，执行模式: {script_exec_mode}, 参数: {args}")
        start_time = time.time()
        
        # 执行脚本
        if table_name is not None:
            # 第二种调用方式的参数格式
            exec_result = module.run(table_name=table_name, execution_mode=script_exec_mode)
        else:
            # 第一种调用方式的参数格式
            exec_result = module.run(script_exec_mode, args)
        
        end_time = time.time()
        duration = end_time - start_time
        
        logger.info(f"脚本 {script_name} 执行完成，结果: {exec_result}, 耗时: {duration:.2f}秒")
        return exec_result
    except Exception as e:
        logger.error(f"执行脚本 {script_name} 时出错: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return False

def generate_optimized_execution_order(table_names, dependency_dict):
    """
    生成优化的执行顺序，处理循环依赖
    
    参数:
        table_names: 表名列表
        dependency_dict: 依赖关系字典 {表名: [依赖表1, 依赖表2, ...]}
    
    返回:
        list: 优化后的执行顺序列表
    """
    # 创建有向图
    G = nx.DiGraph()
    
    # 添加所有节点
    for table_name in table_names:
        G.add_node(table_name)
    
    # 添加依赖边
    for target, sources in dependency_dict.items():
        for source in sources:
            if source in table_names:  # 确保只考虑目标表集合中的表
                # 从依赖指向目标，表示依赖需要先执行
                G.add_edge(source, target)
    
    # 检测循环依赖
    cycles = list(nx.simple_cycles(G))
    if cycles:
        logger.warning(f"检测到循环依赖，将尝试打破循环: {cycles}")
        # 打破循环依赖（简单策略：移除每个循环中的一条边）
        for cycle in cycles:
            # 移除循环中的最后一条边
            G.remove_edge(cycle[-1], cycle[0])
            logger.info(f"打破循环依赖: 移除 {cycle[-1]} -> {cycle[0]} 的依赖")
    
    # 生成拓扑排序
    try:
        execution_order = list(nx.topological_sort(G))
        return execution_order
    except Exception as e:
        logger.error(f"生成执行顺序失败: {str(e)}")
        # 返回原始列表作为备选
        return table_names

def get_datamodel_dependency_from_neo4j(table_names):
    """
    从Neo4j获取DataModel表间的依赖关系
    
    参数:
        table_names: 表名列表
    
    返回:
        dict: 依赖关系字典 {目标表: [依赖表1, 依赖表2, ...]}
    """
    logger.info(f"开始获取 {len(table_names)} 个表的依赖关系")
    
    # 创建Neo4j连接
    driver = get_neo4j_driver()
    dependency_dict = {name: [] for name in table_names}
    
    try:
        with driver.session() as session:
            # 使用一次性查询获取所有表之间的依赖关系
            query = """
                MATCH (source:DataModel)-[:DERIVED_FROM]->(target:DataModel)
                WHERE source.en_name IN $table_names AND target.en_name IN $table_names
                RETURN source.en_name AS source, target.en_name AS target
            """
            result = session.run(query, table_names=table_names)
            
            # 处理结果
            for record in result:
                source = record.get("source")
                target = record.get("target")
                
                if source and target:
                    # 目标依赖于源
                    if source in dependency_dict:
                        dependency_dict[source].append(target)
                        logger.debug(f"依赖关系: {source} 依赖于 {target}")
    except Exception as e:
        logger.error(f"从Neo4j获取依赖关系时出错: {str(e)}")
    finally:
        driver.close()
    
    # 记录依赖关系
    for table, deps in dependency_dict.items():
        if deps:
            logger.info(f"表 {table} 依赖于: {deps}")
        else:
            logger.info(f"表 {table} 没有依赖")
    
    return dependency_dict

def get_today_date():
    """获取今天的日期，返回YYYY-MM-DD格式字符串"""
    return datetime.now().strftime("%Y-%m-%d")