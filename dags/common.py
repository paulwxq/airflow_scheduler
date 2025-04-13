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
    conn = get_pg_conn()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            UPDATE airflow_dag_schedule 
            SET exec_start_time = %s
            WHERE exec_date = %s AND target_table = %s AND script_name = %s
        """, (start_time, exec_date, target_table, script_name))
        conn.commit()
    except Exception as e:
        logger.error(f"更新任务开始时间失败: {str(e)}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

def update_task_completion(exec_date, target_table, script_name, success, end_time, duration):
    """更新任务完成信息"""
    conn = get_pg_conn()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            UPDATE airflow_dag_schedule 
            SET exec_result = %s, exec_end_time = %s, exec_duration = %s
            WHERE exec_date = %s AND target_table = %s AND script_name = %s
        """, (success, end_time, duration, exec_date, target_table, script_name))
        conn.commit()
    except Exception as e:
        logger.error(f"更新任务完成信息失败: {str(e)}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

def execute_with_monitoring(target_table, script_name, script_exec_mode, exec_date, **kwargs):
    """执行脚本并监控执行情况"""

    # 检查script_name是否为空
    if not script_name:
        logger.error(f"表 {target_table} 的script_name为空，无法执行")
        # 记录执行失败
        now = datetime.now()
        update_task_completion(exec_date, target_table, script_name or "", False, now, 0)
        return False
    # 记录执行开始时间
    start_time = datetime.now()
    update_task_start_time(exec_date, target_table, script_name, start_time)
    
    try:
        # 执行实际脚本
        success = execute_script(script_name, target_table, script_exec_mode)
        
        # 记录结束时间和结果
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        update_task_completion(exec_date, target_table, script_name, success, end_time, duration)
        
        return success
    except Exception as e:
        # 处理异常
        logger.error(f"执行任务出错: {str(e)}")
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        update_task_completion(exec_date, target_table, script_name, False, end_time, duration)
        raise e

def execute_script(script_name, table_name, execution_mode):
    """执行脚本并返回结果"""
    if not script_name:
        logger.error("未提供脚本名称，无法执行")
        return False
    
    try:
        # 检查脚本路径
        script_path = Path(SCRIPTS_BASE_PATH) / script_name
        logger.info(f"准备执行脚本，完整路径: {script_path}")
        
        # 检查脚本路径是否存在
        if not os.path.exists(script_path):
            logger.error(f"脚本文件不存在: {script_path}")
            logger.error(f"请确认脚本文件已部署到正确路径: {SCRIPTS_BASE_PATH}")
            
            # 尝试列出脚本目录中的文件
            try:
                script_dir = Path(SCRIPTS_BASE_PATH)
                if os.path.exists(script_dir):
                    files = os.listdir(script_dir)
                    logger.info(f"可用脚本文件: {files}")
                else:
                    logger.error(f"脚本目录不存在: {script_dir}")
            except Exception as le:
                logger.error(f"尝试列出脚本目录内容时出错: {str(le)}")
                
            return False
            
        logger.info(f"脚本文件存在，开始导入: {script_path}")
        
        # 动态导入模块
        try:
            spec = importlib.util.spec_from_file_location("dynamic_module", script_path)
            if spec is None:
                logger.error(f"无法加载脚本规范: {script_path}")
                return False
                
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            logger.info(f"成功导入脚本模块: {script_name}")
        except ImportError as ie:
            logger.error(f"导入脚本时出错: {str(ie)}")
            import traceback
            logger.error(traceback.format_exc())
            return False
        except SyntaxError as se:
            logger.error(f"脚本语法错误: {str(se)}")
            logger.error(f"错误位置: {se.filename}, 行 {se.lineno}, 列 {se.offset}")
            return False
        
        # 验证run函数存在
        if not hasattr(module, "run"):
            available_funcs = [func for func in dir(module) if callable(getattr(module, func)) and not func.startswith("_")]
            logger.error(f"脚本 {script_name} 未定义标准入口函数 run()，无法执行")
            logger.error(f"可用函数: {available_funcs}")
            return False
        
        # 执行run函数
        logger.info(f"执行脚本 {script_name} 的run函数，参数: table_name={table_name}, execution_mode={execution_mode}")
        result = module.run(table_name=table_name, execution_mode=execution_mode)
        logger.info(f"脚本 {script_name} 执行结果: {result}")
        return result
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