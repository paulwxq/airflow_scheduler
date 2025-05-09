# dataops_productline_prepare_dag.py
from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import logging
import networkx as nx
import json
import os
import re
import glob
from pathlib import Path
import hashlib
import pendulum
from utils import (
    get_pg_conn, 
    get_neo4j_driver,
    get_cn_exec_date
)
from config import PG_CONFIG, NEO4J_CONFIG, DATAOPS_DAGS_PATH

# 创建日志记录器
logger = logging.getLogger(__name__)

def get_enabled_tables():
    """获取所有启用调度的表"""
    try:
        # 使用Neo4j查询所有schedule_status为true的关系
        driver = get_neo4j_driver()
        
        with driver.session() as session:
            # 查询DataModel表中有schedule_status为true的关系
            query_datamodel = """
                MATCH (target:DataModel)-[rel:DERIVED_FROM]->()
                WHERE rel.schedule_status = true
                RETURN target.en_name AS table_name
            """
            
            # 查询DataResource表中有schedule_status为true的关系
            query_dataresource = """
                MATCH (target:DataResource)-[rel:ORIGINATES_FROM]->()
                WHERE rel.schedule_status = true
                RETURN target.en_name AS table_name
            """
            
            # 查询structure类型的DataResource表中有schedule_status为true的节点
            query_structure = """
                MATCH (target:DataResource)
                WHERE target.type = 'structure' AND target.schedule_status = true
                RETURN target.en_name AS table_name
            """
            
            try:
                # 获取结果
                result_datamodel = session.run(query_datamodel)
                result_dataresource = session.run(query_dataresource)
                result_structure = session.run(query_structure)
                
                # 合并结果
                tables = []
                for result in [result_datamodel, result_dataresource, result_structure]:
                    for record in result:
                        table_name = record.get("table_name")
                        if table_name and table_name not in tables:
                            tables.append(table_name)
                
                logger.info(f"从Neo4j找到 {len(tables)} 个启用的表: {tables}")
                return tables
                
            except Exception as e:
                logger.error(f"Neo4j查询启用的表失败: {str(e)}")
                raise Exception(f"Neo4j查询启用的表失败: {str(e)}")
    except Exception as e:
        logger.error(f"连接Neo4j数据库失败: {str(e)}")
        raise Exception(f"无法连接Neo4j数据库: {str(e)}")
    

def check_table_directly_subscribed(table_name):
    """检查表是否在节点关系中有schedule_status为True的脚本，若有则直接调度"""
    try:
        driver = get_neo4j_driver()
    except Exception as e:
        logger.error(f"连接Neo4j数据库失败: {str(e)}")
        raise Exception(f"无法连接Neo4j数据库: {str(e)}")
    
    try:
        with driver.session() as session:
            # 查询是否有直接调度的脚本
            query_datamodel = """
                MATCH (target:DataModel {en_name: $table_name})-[rel:DERIVED_FROM]->(source)
                WHERE rel.schedule_status = true
                RETURN count(rel) > 0 AS directly_subscribed
            """
            
            query_dataresource = """
                MATCH (target:DataResource {en_name: $table_name})-[rel:ORIGINATES_FROM]->(source)
                WHERE rel.schedule_status = true
                RETURN count(rel) > 0 AS directly_subscribed
            """
            
            # 获取类型
            labels_query = """
                MATCH (n {en_name: $table_name})
                RETURN labels(n) AS labels
            """
            
            result = session.run(labels_query, table_name=table_name)
            record = result.single()
            
            if not record:
                logger.warning(f"在Neo4j中未找到表 {table_name} 的标签信息")
                return False
                
            labels = record.get("labels", [])
            
            # 根据不同标签类型执行不同查询
            if "DataModel" in labels:
                result = session.run(query_datamodel, table_name=table_name)
            elif "DataResource" in labels:
                # 检查是否是structure类型
                structure_query = """
                    MATCH (n:DataResource {en_name: $table_name})
                    RETURN n.type AS type, n.schedule_status AS schedule_status
                """
                result = session.run(structure_query, table_name=table_name)
                record = result.single()
                
                if record and record.get("type") == "structure":
                    # structure类型，从节点获取schedule_status
                    return record.get("schedule_status", False)
                
                # 非structure类型，继续查询关系
                result = session.run(query_dataresource, table_name=table_name)
            else:
                logger.warning(f"表 {table_name} 不是DataModel或DataResource类型")
                return False
            
            record = result.single()
            return record and record.get("directly_subscribed", False)
            
    except Exception as e:
        logger.error(f"检查表订阅状态失败: {str(e)}")
        raise Exception(f"Neo4j查询表订阅状态失败: {str(e)}")
    finally:
        driver.close()


def should_execute_today(table_name, schedule_frequency, exec_date):
    """
    判断指定频率的表在给定执行日期是否应该执行
    
    参数:
        table_name (str): 表名，用于日志记录
        schedule_frequency (str): 调度频率，如'daily'、'weekly'、'monthly'，为None时默认为'daily'
        exec_date (str): 执行日期，格式为'YYYY-MM-DD'
    
    返回:
        bool: 如果该表应该在执行日期执行，则返回True，否则返回False
    """
    # 将执行日期字符串转换为pendulum日期对象
    try:
        exec_date_obj = pendulum.parse(exec_date)
    except Exception as e:
        logger.error(f"解析执行日期 {exec_date} 出错: {str(e)}，使用当前日期")
        exec_date_obj = pendulum.today()
    
    # 计算下一个日期，用于判断是否是月初、周初等
    next_date = exec_date_obj.add(days=1)
    
    # 如果频率为None或空字符串，默认为daily
    if not schedule_frequency:
        logger.info(f"表 {table_name} 未指定调度频率，默认为daily")
        return True
    
    schedule_frequency = schedule_frequency.lower() if isinstance(schedule_frequency, str) else 'daily'
    
    if schedule_frequency == 'daily':
        # 日任务每天都执行
        return True
    elif schedule_frequency == 'weekly':
        # 周任务只在周日执行（因为exec_date+1是周一时才执行）
        is_sunday = next_date.day_of_week == 1  # 1表示周一
        logger.info(f"表 {table_name} 是weekly任务，exec_date={exec_date}，next_date={next_date.to_date_string()}，是否周日: {is_sunday}")
        return is_sunday
    elif schedule_frequency == 'monthly':
        # 月任务只在每月最后一天执行（因为exec_date+1是月初时才执行）
        is_month_end = next_date.day == 1
        logger.info(f"表 {table_name} 是monthly任务，exec_date={exec_date}，next_date={next_date.to_date_string()}，是否月末: {is_month_end}")
        return is_month_end
    elif schedule_frequency == 'quarterly':
        # 季度任务只在每季度最后一天执行（因为exec_date+1是季度初时才执行）
        is_quarter_end = next_date.day == 1 and next_date.month in [1, 4, 7, 10]
        logger.info(f"表 {table_name} 是quarterly任务，exec_date={exec_date}，next_date={next_date.to_date_string()}，是否季末: {is_quarter_end}")
        return is_quarter_end
    elif schedule_frequency == 'yearly':
        # 年任务只在每年最后一天执行（因为exec_date+1是年初时才执行）
        is_year_end = next_date.day == 1 and next_date.month == 1
        logger.info(f"表 {table_name} 是yearly任务，exec_date={exec_date}，next_date={next_date.to_date_string()}，是否年末: {is_year_end}")
        return is_year_end
    else:
        # 未知频率，默认执行
        logger.warning(f"表 {table_name} 使用未知的调度频率: {schedule_frequency}，默认执行")
        return True


def get_table_info_from_neo4j(table_name):
    """从Neo4j获取表的详细信息，保留完整的scripts_info并确保正确获取源表依赖"""
    try:
        driver = get_neo4j_driver()
    except Exception as e:
        logger.error(f"连接Neo4j数据库失败: {str(e)}")
        raise Exception(f"无法连接Neo4j数据库: {str(e)}")
        
    # 检查表是否直接订阅
    is_directly_schedule = check_table_directly_subscribed(table_name)

    table_info = {
        'target_table': table_name,
        'is_directly_schedule': is_directly_schedule,  # 初始值设为True，从schedule_status表获取
    }
    
    try:
        with driver.session() as session:
            # 尝试执行一个简单查询来验证连接
            try:
                test_result = session.run("RETURN 1 as test")
                test_record = test_result.single()
                if not test_record or test_record.get("test") != 1:
                    logger.error("Neo4j连接测试失败")
                    raise Exception("Neo4j连接测试失败")
            except Exception as e:
                logger.error(f"Neo4j连接测试失败: {str(e)}")
                raise Exception(f"Neo4j连接测试失败: {str(e)}")
            
            # 查询表标签和状态
            query_table = """
                MATCH (t {en_name: $table_name})
                RETURN labels(t) AS labels, t.status AS status,
                       t.type AS type, t.storage_location AS storage_location,
                       t.update_mode as update_mode
            """
            try:
                result = session.run(query_table, table_name=table_name)
                record = result.single()
            except Exception as e:
                logger.error(f"Neo4j查询表信息失败: {str(e)}")
                raise Exception(f"Neo4j查询表信息失败: {str(e)}")
            
            if record:
                labels = record.get("labels", [])
                table_info['target_table_label'] = [label for label in labels if label in ["DataResource", "DataModel", "DataSource"]][0] if labels else None
                table_info['target_table_status'] = record.get("status", True)  # 默认为True
                table_info['target_type'] = record.get("type")  # 获取type属性
                table_info['storage_location'] = record.get("storage_location")  # 获取storage_location属性
                
                # 根据标签类型查询关系和脚本信息
                if "DataResource" in labels:
                    # 检查是否为structure类型
                    if table_info.get('target_type') == "structure":
                        # 对于structure类型，设置默认值，不查询关系
                        table_info['source_tables'] = []  # 使用空数组表示无源表
                        table_info['script_name'] = "load_file.py"
                        table_info['script_type'] = "python_script"
                        
                        # 从节点属性中获取update_mode，如果不存在则使用默认值
                        table_info['script_update_mode'] = record.get("update_mode", "append")
                        table_info['schedule_frequency'] = record.get("schedule_frequency", "daily")
                        table_info['schedule_status'] = record.get("schedule_status", True)
                        
                        logger.info(f"表 {table_name} 为structure类型，使用执行模式: {table_info['script_update_mode']}")

                        # 添加脚本信息
                        table_info['scripts_info'] = {
                            "load_file.py": {
                                "sources": [],
                                "script_type": "python_script",
                                "script_update_mode": table_info['script_update_mode'],
                                "schedule_frequency": table_info['schedule_frequency'],
                                "schedule_status": table_info['schedule_status']
                            }
                        }

                        return table_info
                    else:
                        # 查询源表关系和脚本信息
                        query_rel = """
                            MATCH (target {en_name: $table_name})-[rel:ORIGINATES_FROM]->(source)
                            RETURN source.en_name AS source_table, rel.script_name AS script_name,
                                  rel.script_type AS script_type, rel.update_mode AS script_update_mode,
                                  rel.schedule_frequency AS schedule_frequency, 
                                  rel.schedule_status AS schedule_status
                        """
                elif "DataModel" in labels:
                    # 查询源表关系和脚本信息
                    query_rel = """
                        MATCH (target {en_name: $table_name})-[rel:DERIVED_FROM]->(source)
                        RETURN source.en_name AS source_table, rel.script_name AS script_name,
                              rel.script_type AS script_type, rel.update_mode AS script_update_mode,
                              rel.schedule_frequency AS schedule_frequency, 
                              rel.schedule_status AS schedule_status
                    """
                else:
                    logger.warning(f"表 {table_name} 不是DataResource或DataModel类型")
                    # 即使不是这两种类型，也尝试查询其源表依赖关系
                    query_rel = """
                        MATCH (target {en_name: $table_name})-[rel]->(source)
                        RETURN source.en_name AS source_table, rel.script_name AS script_name,
                              rel.script_type AS script_type, rel.update_mode AS script_update_mode,
                              rel.schedule_frequency AS schedule_frequency, 
                              rel.schedule_status AS schedule_status
                    """
                
                # 收集所有关系记录
                result = session.run(query_rel, table_name=table_name)
                # 检查result对象是否有collect方法，否则使用data方法或list直接转换
                try:
                    if hasattr(result, 'collect'):
                        records = result.collect()  # 使用collect()获取所有记录
                    else:
                        # 尝试使用其他方法获取记录
                        logger.info(f"表 {table_name} 的查询结果不支持collect方法，尝试使用其他方法")
                        try:
                            records = list(result)  # 直接转换为列表
                        except Exception as e1:
                            logger.warning(f"尝试列表转换失败: {str(e1)}，尝试使用data方法")
                            try:
                                records = result.data()  # 使用data()方法
                            except Exception as e2:
                                logger.warning(f"所有方法都失败，使用空列表: {str(e2)}")
                                records = []
                except Exception as e:
                    logger.warning(f"获取查询结果时出错: {str(e)}，使用空列表")
                    records = []
                
                # 记录查询到的原始记录
                logger.info(f"表 {table_name} 查询到 {len(records)} 条关系记录")
                for idx, rec in enumerate(records):
                    logger.info(f"关系记录[{idx}]: source_table={rec.get('source_table')}, script_name={rec.get('script_name')}, " 
                                f"script_type={rec.get('script_type')}, script_update_mode={rec.get('script_update_mode')}, "
                                f"schedule_frequency={rec.get('schedule_frequency')}, schedule_status={rec.get('schedule_status')}")
                
                if records:
                    # 按脚本名称分组源表
                    scripts_info = {}
                    for record in records:
                        script_name = record.get("script_name")
                        source_table = record.get("source_table")
                        script_type = record.get("script_type")
                        script_update_mode = record.get("script_update_mode")
                        schedule_frequency = record.get("schedule_frequency")
                        schedule_status = record.get("schedule_status")
                        
                        logger.info(f"处理记录: source_table={source_table}, script_name={script_name}")
                        
                        # 如果script_name为空，生成默认的脚本名
                        if not script_name:
                            script_name = f"{table_name}_script.py"
                            logger.warning(f"表 {table_name} 的关系中没有script_name属性，使用默认值: {script_name}")
                            
                        if script_name not in scripts_info:
                            scripts_info[script_name] = {
                                "sources": [],
                                "script_type": script_type,
                                "script_update_mode": script_update_mode,
                                "schedule_frequency": schedule_frequency,
                                "schedule_status": schedule_status
                            }
                        
                        # 确保source_table有值且不为None才添加到sources列表中
                        if source_table and source_table not in scripts_info[script_name]["sources"]:
                            scripts_info[script_name]["sources"].append(source_table)
                            logger.debug(f"为表 {table_name} 的脚本 {script_name} 添加源表: {source_table}")
                    
                    # 处理分组信息
                    if scripts_info:
                        # 存储完整的脚本信息
                        table_info['scripts_info'] = scripts_info
                        
                        # 如果只有一个脚本，直接使用它
                        if len(scripts_info) == 1:
                            script_name = list(scripts_info.keys())[0]
                            script_info = scripts_info[script_name]
                            
                            table_info['source_tables'] = script_info["sources"]  # 使用数组
                            table_info['script_name'] = script_name
                            table_info['script_type'] = script_info["script_type"]
                            table_info['script_update_mode'] = script_info["script_update_mode"]
                            table_info['schedule_frequency'] = script_info["schedule_frequency"]
                            table_info['schedule_status'] = script_info["schedule_status"]
                            logger.info(f"表 {table_name} 有单个脚本 {script_name}，源表: {script_info['sources']}")
                        else:
                            # 如果有多个不同脚本，记录多脚本信息
                            logger.info(f"表 {table_name} 有多个不同脚本: {list(scripts_info.keys())}")
                            # 暂时使用第一个脚本的信息作为默认值
                            first_script = list(scripts_info.keys())[0]
                            table_info['source_tables'] = scripts_info[first_script]["sources"]
                            table_info['script_name'] = first_script
                            table_info['script_type'] = scripts_info[first_script]["script_type"]
                            table_info['script_update_mode'] = scripts_info[first_script]["script_update_mode"]
                            table_info['schedule_frequency'] = scripts_info[first_script]["schedule_frequency"]
                            table_info['schedule_status'] = scripts_info[first_script]["schedule_status"]
                    else:
                        logger.warning(f"表 {table_name} 未找到有效的脚本信息")
                        table_info['source_tables'] = []  # 使用空数组
                else:
                    # 如果没有找到关系记录，则尝试直接查询表的上游依赖
                    logger.info(f"表 {table_name} 未通过关系查询找到源表，尝试直接查询依赖...")
                    
                    # 查询任何类型的上游依赖关系
                    query_deps = """
                        MATCH (target {en_name: $table_name})-[rel]->(source)
                        RETURN source.en_name AS source_table
                    """
                    
                    try:
                        deps_result = session.run(query_deps, table_name=table_name)
                        deps_records = list(deps_result)
                        
                        source_tables = []
                        for rec in deps_records:
                            src_table = rec.get("source_table")
                            if src_table and src_table not in source_tables:
                                source_tables.append(src_table)
                                logger.info(f"直接依赖查询: 表 {table_name} 依赖于 {src_table}")
                        
                        # 设置默认的脚本名和源表
                        script_name = f"{table_name}_script.py"
                        table_info['source_tables'] = source_tables
                        table_info['script_name'] = script_name
                        table_info['script_type'] = "python_script"
                        table_info['script_update_mode'] = "append"
                        table_info['schedule_frequency'] = "daily"
                        table_info['schedule_status'] = True
                        
                        # 创建scripts_info
                        table_info['scripts_info'] = {
                            script_name: {
                                "sources": source_tables,
                                "script_type": "python_script",
                                "script_update_mode": "append",
                                "schedule_frequency": "daily",
                                "schedule_status": True
                            }
                        }
                        
                        logger.info(f"为表 {table_name} 设置默认脚本 {script_name}，源表: {source_tables}")
                    except Exception as e:
                        logger.warning(f"直接查询表 {table_name} 的依赖关系时出错: {str(e)}")
                        table_info['source_tables'] = []  # 使用空数组
                        table_info['script_name'] = f"{table_name}_script.py"
                        table_info['script_type'] = "python_script"
                        table_info['script_update_mode'] = "append"
                        table_info['schedule_frequency'] = "daily"
                        table_info['schedule_status'] = True
                        
                        # 创建空的scripts_info
                        table_info['scripts_info'] = {
                            table_info['script_name']: {
                                "sources": [],
                                "script_type": "python_script",
                                "script_update_mode": "append",
                                "schedule_frequency": "daily",
                                "schedule_status": True
                            }
                        }
            else:
                logger.warning(f"在Neo4j中找不到表 {table_name} 的信息，设置默认值")
                table_info['source_tables'] = []
                table_info['script_name'] = f"{table_name}_script.py"
                table_info['script_type'] = "python_script"
                table_info['script_update_mode'] = "append"
                table_info['schedule_frequency'] = "daily"
                table_info['schedule_status'] = True
                
                # 创建空的scripts_info
                table_info['scripts_info'] = {
                    table_info['script_name']: {
                        "sources": [],
                        "script_type": "python_script",
                        "script_update_mode": "append",
                        "schedule_frequency": "daily",
                        "schedule_status": True
                    }
                }
    except Exception as e:
        if "Neo4j连接" in str(e) or "Neo4j查询" in str(e):
            # 这是我们已经处理过的错误，直接抛出
            raise
        else:
            logger.error(f"处理表 {table_name} 的信息时出错: {str(e)}")
            raise Exception(f"Neo4j数据处理失败: {str(e)}")
    finally:
        driver.close()
    
    return table_info


def process_dependencies(tables_info):
    """处理表间依赖关系，添加被动调度的表"""
    # 存储所有表信息的字典
    all_tables = {t['target_table']: t for t in tables_info}
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
                
            for table_name, table_info in list(all_tables.items()):
                if table_info.get('target_table_label') == 'DataModel':
                    # 查询其依赖表
                    query = """
                        MATCH (dm {en_name: $table_name})-[rel:DERIVED_FROM]->(dep)
                        RETURN dep.en_name AS dep_name, labels(dep) AS dep_labels, 
                               dep.status AS dep_status, rel.schedule_frequency AS schedule_frequency,
                               rel.update_mode AS update_mode, rel.schedule_status AS schedule_status,
                               rel.script_name AS script_name, rel.script_type AS script_type
                    """
                    try:
                        result = session.run(query, table_name=table_name)
                    except Exception as e:
                        logger.error(f"Neo4j查询依赖关系失败: {str(e)}")
                        raise Exception(f"Neo4j查询依赖关系失败: {str(e)}")
                    
                    for record in result:
                        dep_name = record.get("dep_name")
                        dep_labels = record.get("dep_labels", [])
                        dep_status = record.get("dep_status", True)
                        schedule_frequency = record.get("schedule_frequency")
                        update_mode = record.get("update_mode")
                        schedule_status = record.get("schedule_status", False)
                        script_name = record.get("script_name")
                        script_type = record.get("script_type")
                        
                        # 处理未被直接调度的依赖表
                        if dep_name and dep_name not in all_tables:
                            logger.info(f"发现被动依赖表: {dep_name}, 标签: {dep_labels}")
                            
                            # 获取依赖表详细信息
                            dep_info = get_table_info_from_neo4j(dep_name)
                            dep_info['is_directly_schedule'] = False
                            
                            # 手动更新一些可能从关系中获取到的属性
                            if schedule_frequency:
                                dep_info['schedule_frequency'] = schedule_frequency
                            if update_mode:
                                dep_info['script_update_mode'] = update_mode
                            if schedule_status is not None:
                                dep_info['schedule_status'] = schedule_status
                            if script_name:
                                dep_info['script_name'] = script_name
                            if script_type:
                                dep_info['script_type'] = script_type
                            
                            all_tables[dep_name] = dep_info
    except Exception as e:
        if "Neo4j" in str(e):
            # 已经处理过的错误，直接抛出
            raise
        else:
            logger.error(f"处理依赖关系时出错: {str(e)}")
            raise Exception(f"处理依赖关系时出错: {str(e)}")
    finally:
        driver.close()
    
    return list(all_tables.values())


def filter_invalid_tables(tables_info):
    """过滤无效表及其依赖，使用NetworkX构建依赖图"""
    # 构建表名到索引的映射
    table_dict = {t['target_table']: i for i, t in enumerate(tables_info)}
    
    # 找出无效表
    invalid_tables = set()
    for table in tables_info:
        if table.get('target_table_status') is False:
            invalid_tables.add(table['target_table'])
            logger.info(f"表 {table['target_table']} 的状态为无效")
    
    # 构建依赖图
    G = nx.DiGraph()
    
    # 添加所有节点
    for table in tables_info:
        G.add_node(table['target_table'])
    
    # 查询并添加依赖边
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
                
            for table in tables_info:
                if table.get('target_table_label') == 'DataModel':
                    query = """
                        MATCH (source {en_name: $table_name})-[:DERIVED_FROM]->(target)
                        RETURN target.en_name AS target_name
                    """
                    try:
                        result = session.run(query, table_name=table['target_table'])
                    except Exception as e:
                        logger.error(f"Neo4j查询表依赖关系失败: {str(e)}")
                        raise Exception(f"Neo4j查询表依赖关系失败: {str(e)}")
                    
                    for record in result:
                        target_name = record.get("target_name")
                        if target_name and target_name in table_dict:
                            # 添加从目标到源的边，表示目标依赖于源
                            G.add_edge(table['target_table'], target_name)
                            logger.debug(f"添加依赖边: {table['target_table']} -> {target_name}")
    except Exception as e:
        if "Neo4j" in str(e):
            # 已经处理过的错误，直接抛出
            raise
        else:
            logger.error(f"构建依赖图时出错: {str(e)}")
            raise Exception(f"构建依赖图时出错: {str(e)}")
    finally:
        driver.close()
    
    # 找出依赖于无效表的所有表
    downstream_invalid = set()
    for invalid_table in invalid_tables:
        # 获取可从无效表到达的所有节点
        try:
            descendants = nx.descendants(G, invalid_table)
            downstream_invalid.update(descendants)
            logger.info(f"表 {invalid_table} 的下游无效表: {descendants}")
        except Exception as e:
            logger.error(f"处理表 {invalid_table} 的下游依赖时出错: {str(e)}")
            raise Exception(f"处理下游依赖失败: {str(e)}")
    
    # 合并所有无效表
    all_invalid = invalid_tables.union(downstream_invalid)
    logger.info(f"总共 {len(all_invalid)} 个表被标记为无效: {all_invalid}")
    
    # 过滤出有效表
    valid_tables = [t for t in tables_info if t['target_table'] not in all_invalid]
    logger.info(f"过滤后保留 {len(valid_tables)} 个有效表")
    
    return valid_tables


def touch_product_scheduler_file():
    """
    更新产品线调度器DAG文件的修改时间，触发重新解析
    
    返回:
        bool: 是否成功更新
    """
    data_scheduler_path = os.path.join(os.path.dirname(__file__), 'dataops_productline_execute_dag.py')
    
    success = False
    try:
        if os.path.exists(data_scheduler_path):
            # 更新文件修改时间，触发Airflow重新解析
            os.utime(data_scheduler_path, None)
            logger.info(f"已触发产品线执行DAG重新解析: {data_scheduler_path}")
            success = True
        else:
            logger.warning(f"产品线执行DAG文件不存在: {data_scheduler_path}")
                
        return success
    except Exception as e:
        logger.error(f"触发DAG重新解析时出错: {str(e)}")
        return False

# def get_subscription_state_hash():
#     """获取订阅表状态的哈希值"""
#     try:
#         conn = get_pg_conn()
#         cursor = conn.cursor()
#         try:
#             cursor.execute("""
#                 SELECT table_name, schedule_is_enabled
#                 FROM schedule_status
#                 ORDER BY table_name
#             """)
#             rows = cursor.fetchall()
#             # 将所有行拼接成一个字符串，然后计算哈希值
#             data_str = '|'.join(f"{row[0]}:{row[1]}" for row in rows)
#             return hashlib.md5(data_str.encode()).hexdigest()
#         except Exception as e:
#             logger.error(f"计算订阅表状态哈希值时出错: {str(e)}")
#             raise Exception(f"PostgreSQL查询订阅表状态失败: {str(e)}")
#         finally:
#             cursor.close()
#             conn.close()
#     except Exception as e:
#         logger.error(f"连接PostgreSQL数据库失败: {str(e)}")
#         raise Exception(f"无法连接PostgreSQL数据库: {str(e)}")

def check_execution_plan_in_db(**kwargs):
    """
    检查当天的执行计划是否存在于数据库中
    返回False将阻止所有下游任务执行
    """
    # 获取执行日期
    dag_run = kwargs.get('dag_run')
    logger.info(f"This DAG run was triggered via: {dag_run.run_type}")
    logical_date = dag_run.logical_date    
    exec_date, local_logical_date = get_cn_exec_date(logical_date)
    logger.info(f"logical_date： {logical_date} ")
    logger.info(f"local_logical_date： {local_logical_date} ")
    logger.info(f"检查执行日期 exec_date： {exec_date} 的执行计划是否存在于数据库中")
   
    # 检查数据库中是否存在执行计划
    try:
        conn = get_pg_conn()
        cursor = conn.cursor()
        try:
            cursor.execute("""
                SELECT plan
                FROM airflow_exec_plans
                WHERE exec_date = %s AND dag_id = 'dataops_productline_prepare_dag'
                ORDER BY logical_date DESC
                LIMIT 1
            """, (exec_date,))
            
            result = cursor.fetchone()
            if not result:
                logger.error(f"数据库中不存在执行日期 {exec_date} 的执行计划")
                return False
            
            # 检查执行计划内容是否有效
            try:
                # PostgreSQL的jsonb类型会被psycopg2自动转换为Python字典，无需再使用json.loads
                plan_data = result[0]            
                # 检查必要字段
                if "exec_date" not in plan_data:
                    logger.error("执行计划缺少exec_date字段")
                    return False
                    
                if not isinstance(plan_data.get("scripts", []), list):
                    logger.error("执行计划的scripts字段无效")
                    return False
                    
                if not isinstance(plan_data.get("resource_scripts", []), list):
                    logger.error("执行计划的resource_scripts字段无效")
                    return False

                if not isinstance(plan_data.get("model_scripts", []), list):
                    logger.error("执行计划的model_scripts字段无效")
                    return False
                
                # 检查是否有脚本数据
                scripts = plan_data.get("scripts", [])
                resource_scripts = plan_data.get("resource_scripts", [])
                model_scripts = plan_data.get("model_scripts", [])
                
                logger.info(f"执行计划验证成功: 包含 {len(scripts)} 个脚本，{len(resource_scripts)} 个资源脚本和 {len(model_scripts)} 个模型脚本")
                return True
                
            except Exception as je:
                logger.error(f"处理执行计划数据时出错: {str(je)}")
                return False
            
        except Exception as e:
            logger.error(f"检查数据库中执行计划时出错: {str(e)}")
            raise Exception(f"PostgreSQL查询执行计划失败: {str(e)}")
        finally:
            cursor.close()
            conn.close()
    except Exception as e:
        logger.error(f"连接PostgreSQL数据库失败: {str(e)}")
        raise Exception(f"无法连接PostgreSQL数据库: {str(e)}")

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
                script_type = script_info.get('script_type', 'python_script')
                script_update_mode = script_info.get('script_update_mode', 'append')
                script_schedule_frequency = script_info.get('schedule_frequency', schedule_frequency)
                script_schedule_status = script_info.get('schedule_status', True)
                
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
                    "update_mode": script_update_mode,  # 使用update_mode代替script_update_mode
                    "schedule_frequency": script_schedule_frequency,
                    "schedule_status": script_schedule_status,
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
            script_type = table.get('script_type', 'python_script')
            script_update_mode = table.get('script_update_mode', 'append')
            table_schedule_frequency = table.get('schedule_frequency', 'daily')
            table_schedule_status = table.get('schedule_status', True)
            
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
                "update_mode": script_update_mode,  # 使用update_mode代替script_update_mode
                "schedule_frequency": table_schedule_frequency,
                "schedule_status": table_schedule_status,
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

def set_dataops_dags_path_variable():
    """
    将DATAOPS_DAGS_PATH设置为Airflow变量
    """
    try:
        # 从config中获取DATAOPS_DAGS_PATH值
        Variable.set("DATAOPS_DAGS_PATH", DATAOPS_DAGS_PATH)
        logger.info(f"已成功设置Airflow变量DATAOPS_DAGS_PATH为: {DATAOPS_DAGS_PATH}")
        return True
    except Exception as e:
        logger.error(f"设置Airflow变量DATAOPS_DAGS_PATH失败: {str(e)}")
        return False


def prepare_productline_dag_schedule(**kwargs):
    """准备产品线DAG调度任务的主函数"""
    # 添加更严格的异常处理
    try:
        # 检查是否是手动触发模式
        is_manual_trigger = False
        dag_run = kwargs['dag_run']
        logger.info(f"This DAG run was triggered via: {dag_run.run_type}")
        
        if dag_run.external_trigger:
            is_manual_trigger = True
            logger.info("This DAG run was manually triggered.")
        
        # 获取执行日期
        dag_run = kwargs.get('dag_run')
        logical_date = dag_run.logical_date
        exec_date, local_logical_date = get_cn_exec_date(logical_date)
        logger.info(f"开始准备执行日期 {exec_date} 的创建执行计划的调度任务")
        
        # 检查是否需要创建新的执行计划
        # 出于测试目的，直接设置为True
        need_create_plan = True
        
        # 条件1: 数据库中不存在当天的执行计划
        try:
            has_plan_in_db = check_execution_plan_in_db(**kwargs)
            if not has_plan_in_db:
                logger.info(f"数据库中不存在执行日期exec_date {exec_date} 的执行计划，需要创建新的执行计划")
                need_create_plan = True
        except Exception as e:
            # 如果执行计划查询失败，直接报错
            logger.error(f"检查执行计划失败: {str(e)}")
            raise Exception(f"检查执行计划失败，可能是数据库连接问题: {str(e)}")
        
        # 条件2: schedule_status表中的数据发生了变更
        # if not need_create_plan:
        #     # 计算当前哈希值
        #     current_hash = get_subscription_state_hash()
        #     # 读取上次记录的哈希值
        #     hash_file = os.path.join(os.path.dirname(__file__), '.subscription_state')
        #     last_hash = None
        #     if os.path.exists(hash_file):
        #         try:
        #             with open(hash_file, 'r') as f:
        #                 last_hash = f.read().strip()
        #         except Exception as e:
        #             logger.warning(f"读取上次订阅状态哈希值失败: {str(e)}")
            
        #     # 如果哈希值不同，表示数据发生了变更
        #     if current_hash != last_hash:
        #         logger.info(f"检测到schedule_status表数据变更。旧哈希值: {last_hash}, 新哈希值: {current_hash}")
        #         need_create_plan = True
        
        # 手动触发模式覆盖以上判断
        if is_manual_trigger:
            logger.info("手动触发模式，将创建新的执行计划")
            need_create_plan = True
        
        # 如果不需要创建新的执行计划，直接返回
        if not need_create_plan:
            logger.info("无需创建新的执行计划")
            return 0
        
        # 继续处理，创建新的执行计划
        # 1. 获取启用的表
        enabled_tables = get_enabled_tables()
        logger.info(f"获取到 {len(enabled_tables)} 个启用的表")
        
        if not enabled_tables:
            logger.warning("没有找到启用的表，准备工作结束")
            return 0
        
        # 2. 获取表的详细信息
        tables_info = []
        for table_name in enabled_tables:
            table_info = get_table_info_from_neo4j(table_name)
            if table_info:
                tables_info.append(table_info)
        
        logger.info(f"成功获取 {len(tables_info)} 个表的详细信息")
        
        # 2.1 根据调度频率过滤表
        filtered_tables_info = []
        for table_info in tables_info:
            table_name = table_info['target_table']
            schedule_frequency = table_info.get('schedule_frequency')
            
            if should_execute_today(table_name, schedule_frequency, exec_date):
                filtered_tables_info.append(table_info)
                logger.info(f"表 {table_name} (频率: {schedule_frequency}) 将在今天{exec_date}执行")
            else:
                logger.info(f"表 {table_name} (频率: {schedule_frequency}) 今天{exec_date}不执行，已过滤")
        
        logger.info(f"按调度频率过滤后，今天{exec_date}需要执行的表有 {len(filtered_tables_info)} 个")

        # 3. 处理依赖关系，添加被动调度的表
        enriched_tables = process_dependencies(filtered_tables_info)
        logger.info(f"处理依赖后，总共有 {len(enriched_tables)} 个表")
        
        # 4. 过滤无效表及其依赖
        valid_tables = filter_invalid_tables(enriched_tables)
        logger.info(f"过滤无效表后，最终有 {len(valid_tables)} 个有效表")
        
        # 5. 将表信息转换为脚本信息
        scripts = prepare_scripts_from_tables(valid_tables)
        logger.info(f"生成了 {len(scripts)} 个脚本信息")
        
        # 检查所有脚本的source_tables字段
        scripts_without_sources = [s['script_id'] for s in scripts if not s.get('source_tables')]
        if scripts_without_sources:
            logger.warning(f"有 {len(scripts_without_sources)} 个脚本没有源表信息: {scripts_without_sources}")
        
        # 6. 处理脚本依赖关系
        script_dependencies, dependency_graph = build_script_dependency_graph(scripts)
        logger.info(f"构建了脚本依赖关系图，包含 {len(script_dependencies)} 个节点")
        
        # 检查依赖关系是否为空
        empty_deps = {k: v for k, v in script_dependencies.items() if not v}
        if len(empty_deps) == len(script_dependencies):
            logger.warning(f"所有脚本的依赖关系为空，这可能表示Neo4j查询未能正确获取表之间的关系")
            
            # 尝试从Neo4j额外构建顶层表间依赖关系
            logger.info("尝试通过Neo4j直接查询表间依赖关系...")
            driver = get_neo4j_driver()
            try:
                with driver.session() as session:
                    # 查询所有表之间的依赖关系
                    query = """
                        MATCH (a)-[r]->(b)
                        WHERE a.en_name IS NOT NULL AND b.en_name IS NOT NULL
                        RETURN a.en_name AS source, b.en_name AS target
                    """
                    
                    try:
                        result = session.run(query)
                        records = list(result)
                        
                        # 创建表之间的依赖映射
                        table_dependencies = {}
                        for record in records:
                            source = record.get("source")
                            target = record.get("target")
                            
                            if source and target:
                                if source not in table_dependencies:
                                    table_dependencies[source] = []
                                
                                if target not in table_dependencies[source]:
                                    table_dependencies[source].append(target)
                                    logger.info(f"从Neo4j发现表依赖: {source} 依赖于 {target}")
                        
                        # 创建表到脚本的映射
                        table_to_script_map = {}
                        for script in scripts:
                            target_table = script['target_table']
                            script_id = script['script_id']
                            
                            if target_table not in table_to_script_map:
                                table_to_script_map[target_table] = []
                            
                            table_to_script_map[target_table].append(script_id)
                        
                        # 基于表依赖关系构建脚本依赖关系
                        updated_dependencies = False
                        for table, deps in table_dependencies.items():
                            if table in table_to_script_map:
                                for table_script_id in table_to_script_map[table]:
                                    for dep_table in deps:
                                        if dep_table in table_to_script_map:
                                            for dep_script_id in table_to_script_map[dep_table]:
                                                if table_script_id != dep_script_id and dep_script_id not in script_dependencies[table_script_id]:
                                                    script_dependencies[table_script_id].append(dep_script_id)
                                                    logger.info(f"添加额外脚本依赖: {table_script_id} 依赖于 {dep_script_id}")
                                                    updated_dependencies = True
                        
                        if updated_dependencies:
                            # 如果依赖关系有更新，重新构建依赖图并优化执行顺序
                            G = nx.DiGraph()
                            
                            # 添加所有脚本作为节点
                            for script in scripts:
                                G.add_node(script['script_id'])
                            
                            # 添加依赖边
                            for script_id, deps in script_dependencies.items():
                                for dep_id in deps:
                                    G.add_edge(script_id, dep_id)
                            
                            dependency_graph = G
                            logger.info("依赖图已基于表依赖关系重新构建")
                    except Exception as e:
                        logger.warning(f"查询Neo4j获取表级依赖时出错: {str(e)}")
            except Exception as e:
                logger.error(f"尝试直接查询表依赖关系时出错: {str(e)}")
            finally:
                driver.close()
        
        # 7. 优化脚本执行顺序
        execution_order = optimize_script_execution_order(scripts, script_dependencies, dependency_graph)
        logger.info(f"生成优化的执行顺序，包含 {len(execution_order)} 个脚本")
        
        # 8. 分类脚本
        resource_scripts = []
        model_scripts = []
        
        for script in scripts:
            script_id = script['script_id']
            if script['target_table_label'] == 'DataResource':
                resource_scripts.append(script_id)
            elif script['target_table_label'] == 'DataModel':
                model_scripts.append(script_id)
        
        logger.info(f"分类完成: {len(resource_scripts)} 个资源脚本, {len(model_scripts)} 个模型脚本")
        
        # 构建执行计划并保存到数据库
        try:
            # 9. 创建最终执行计划
            execution_plan = {
                "exec_date": exec_date,
                "version": "2.0",
                "scripts": scripts,
                "script_dependencies": script_dependencies,
                "resource_scripts": resource_scripts,
                "model_scripts": model_scripts,
                "execution_order": execution_order
            }
            
            # 10. 更新订阅表状态哈希值
            # current_hash = get_subscription_state_hash()
            # hash_file = os.path.join(os.path.dirname(__file__), '.subscription_state')
            # with open(hash_file, 'w') as f:
            #     f.write(current_hash)
            # logger.info(f"已更新订阅表状态哈希值: {current_hash}")
            
            # 11. 触发产品线执行DAG重新解析
            touch_product_scheduler_file()
            
            # 12. 保存执行计划到数据库表
            try:
                # 获取DAG运行信息
                dag_run = kwargs.get('dag_run')
                if dag_run:
                    dag_id = dag_run.dag_id
                    run_id = dag_run.run_id
                    logical_date = dag_run.logical_date
                    local_logical_date = pendulum.instance(logical_date).in_timezone('Asia/Shanghai')
                else:
                    # 如果无法获取dag_run，使用默认值
                    dag_id = kwargs.get('dag').dag_id if 'dag' in kwargs else "dataops_productline_prepare_dag"
                    run_id = f"manual_{datetime.now().strftime('%Y%m%d%H%M%S')}"
                    logical_date = datetime.now()
                
                # 保存到数据库
                save_result = save_execution_plan_to_db(
                    execution_plan=execution_plan,
                    dag_id=dag_id,
                    run_id=run_id,
                    logical_date=local_logical_date,
                    ds=exec_date
                )
                
                if save_result:
                    logger.info("执行计划已成功保存到数据库")
                else:
                    raise Exception("执行计划保存到数据库失败")
                
                # 13. 设置Airflow变量DATAOPS_DAGS_PATH
                set_dataops_dags_path_variable()
                
            except Exception as db_e:
                # 捕获数据库保存错误
                error_msg = f"保存执行计划到数据库时出错: {str(db_e)}"
                logger.error(error_msg)
                raise Exception(error_msg)
            
        except Exception as e:
            error_msg = f"创建或保存执行计划时出错: {str(e)}"
            logger.error(error_msg)
            # 强制抛出异常，确保任务失败，阻止下游DAG执行
            raise Exception(error_msg)
        
        return len(scripts)  # 返回脚本数量
    except Exception as e:
        error_msg = f"产品线DAG调度任务准备失败: {str(e)}"
        logger.error(error_msg)
        # 强制抛出异常，确保任务失败
        raise Exception(error_msg)

# 创建DAG
with DAG(
    "dataops_productline_prepare_dag",
    start_date=datetime(2025, 1, 1),
    # 每小时执行一次
    schedule_interval="0 * * * *",
    catchup=False,
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    params={"TRIGGERED_VIA_UI": True},  # 触发 UI 弹出配置页面
) as dag:
    
    # 任务开始标记
    start_preparation = EmptyOperator(
        task_id="start_preparation",
        dag=dag
    )
    
    # 准备调度任务
    prepare_task = PythonOperator(
        task_id="prepare_productline_dag_schedule",
        python_callable=prepare_productline_dag_schedule,
        provide_context=True,
        dag=dag
    )
    
    # 检查执行计划是否存在于数据库中
    check_plan_in_db = ShortCircuitOperator(
        task_id="check_execution_plan_in_db",
        python_callable=check_execution_plan_in_db,
        provide_context=True,
        dag=dag
    )
    
    # 准备完成标记
    preparation_completed = EmptyOperator(
        task_id="preparation_completed",
        dag=dag
    )
    
    # 设置任务依赖
    start_preparation >> prepare_task >> check_plan_in_db >> preparation_completed 