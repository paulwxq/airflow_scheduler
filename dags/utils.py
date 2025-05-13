# utils.py
import psycopg2
import os
import sys
from config import PG_CONFIG, NEO4J_CONFIG, SCRIPTS_BASE_PATH
from neo4j import GraphDatabase
import logging
import importlib.util
from pathlib import Path
import networkx as nx
from airflow.exceptions import AirflowFailException
from datetime import datetime, timedelta, date
import functools
import time
import pendulum

# 创建统一的日志记录器
logger = logging.getLogger("airflow.task")

def get_pg_conn():
    return psycopg2.connect(**PG_CONFIG)



def execute_script(script_name=None, table_name=None, update_mode=None, script_path=None, script_exec_mode=None, args=None):
    """
    根据脚本名称动态导入并执行对应的脚本
    支持两种调用方式:
    1. execute_script(script_name, table_name, update_mode) - 原始实现
    2. execute_script(script_path, script_name, script_exec_mode, args={}) - 来自common.py的实现
        
    返回:
        bool: 执行成功返回True，否则返回False
    """
    # 第一种调用方式 - 原始函数实现
    if script_name and table_name and update_mode is not None and script_path is None and script_exec_mode is None:
        if not script_name:
            logger.error("未提供脚本名称，无法执行")
            return False
        
        try:
            # 直接使用配置的部署路径，不考虑本地开发路径
            script_path = Path(SCRIPTS_BASE_PATH) / script_name
            logger.info(f"使用配置的Airflow部署路径: {script_path}")
            
            # 动态导入模块
            spec = importlib.util.spec_from_file_location("dynamic_module", script_path)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            
            # 使用标准入口函数run
            if hasattr(module, "run"):
                logger.info(f"执行脚本 {script_name} 的标准入口函数 run()")
                module.run(table_name=table_name, update_mode=update_mode)
                return True
            else:
                logger.warning(f"脚本 {script_name} 未定义标准入口函数 run()，无法执行")
                return False
        except Exception as e:
            logger.error(f"执行脚本 {script_name} 时出错: {str(e)}")
            return False
    
    # 第二种调用方式 - 从common.py迁移的实现
    else:
        # 确定调用方式并统一参数
        if script_path and script_name and script_exec_mode is not None:
            # 第二种调用方式 - 显式提供所有参数
            if args is None:
                args = {}
        elif script_name and table_name and update_mode is not None:
            # 第二种调用方式 - 但使用第一种调用方式的参数名
            script_path = os.path.join(SCRIPTS_BASE_PATH, f"{script_name}.py")
            script_exec_mode = update_mode
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
                # 使用table_name参数调用
                exec_result = module.run(table_name=table_name, update_mode=script_exec_mode)
            else:
                # 使用script_exec_mode和args调用
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



def get_resource_subscribed_tables(enabled_tables: list) -> list:
    result = []
    for t in enabled_tables:
        if is_data_resource_table(t['table_name']):
            result.append(t)
    return result


# 根据目标表，递归查找其所有上游依赖的 DataResource 表（不限层级）
def get_dependency_resource_tables(enabled_tables: list) -> list:
    uri = NEO4J_CONFIG['uri']
    auth = (NEO4J_CONFIG['user'], NEO4J_CONFIG['password'])
    driver = GraphDatabase.driver(uri, auth=auth)
    resource_set = set()
    try:
        with driver.session() as session:
            for t in enabled_tables:
                query = """
                    MATCH (target:Table {name: $table_name})
                    MATCH (res:DataResource)-[:ORIGINATES_FROM]->(:DataSource)
                    WHERE (target)-[:DERIVED_FROM*1..]->(res)
                    RETURN DISTINCT res.en_name AS name
                """
                result = session.run(query, table_name=t['table_name'])
                for record in result:
                    resource_set.add(record['name'])
    finally:
        driver.close()

    output = []
    for name in resource_set:
        output.append({"table_name": name, "execution_mode": "append"})
    return output


# 从 PostgreSQL 获取启用的表，按调度频率 daily/weekly/monthly 过滤
def get_enabled_tables(frequency: str) -> list:
    """
    从PostgreSQL获取启用的表，按调度频率daily/weekly/monthly过滤
    
    参数:
        frequency (str): 调度频率，如daily, weekly, monthly
        
    返回:
        list: 包含表名和执行模式的列表
    """
    conn = get_pg_conn()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT table_name, update_mode
        FROM table_schedule
        WHERE is_enabled = TRUE AND schedule_frequency = %s
    """, (frequency,))
    result = cursor.fetchall()
    cursor.close()
    conn.close()

    output = []
    for r in result:
        output.append({"table_name": r[0], "update_mode": r[1]})
    return output

# 判断给定表名是否是 Neo4j 中的 DataResource 类型
def is_data_resource_table(table_name: str) -> bool:
    uri = NEO4J_CONFIG['uri']
    auth = (NEO4J_CONFIG['user'], NEO4J_CONFIG['password'])
    driver = GraphDatabase.driver(uri, auth=auth)
    query = """
        MATCH (n:DataResource {en_name: $table_name}) RETURN count(n) > 0 AS exists
    """
    try:
        with driver.session() as session:
            result = session.run(query, table_name=table_name)
            record = result.single()
            return record and record["exists"]
    finally:
        driver.close()

# 从 Neo4j 查询 DataModel 表的 DERIVED_FROM 关系上的 script_name 属性
def get_script_name_from_neo4j(table_name):
    uri = NEO4J_CONFIG['uri']
    auth = (NEO4J_CONFIG['user'], NEO4J_CONFIG['password'])
    driver = GraphDatabase.driver(uri, auth=auth)
    logger.info(f"从Neo4j查询表 {table_name} 的脚本名称")
    
    # 检查查询的是 DERIVED_FROM 关系的方向
    check_query = """
        MATCH (a:DataModel {en_name: $table_name})-[r:DERIVED_FROM]->(b)
        RETURN b.en_name AS upstream_name LIMIT 5
    """
    
    try:
        with driver.session() as session:
            # 先检查依赖关系
            logger.info(f"检查表 {table_name} 的上游依赖方向")
            check_result = session.run(check_query, table_name=table_name)
            upstreams = [record['upstream_name'] for record in check_result if 'upstream_name' in record]
            logger.info(f"表 {table_name} 的上游依赖: {upstreams}")
            
            # 查询脚本名称
            query = """
                MATCH (target:DataModel {en_name: $table_name})-[r:DERIVED_FROM]->(n)
                WHERE n:DataModel OR n:DataResource
                RETURN r.script_name AS script_name
            """
            result = session.run(query, table_name=table_name)
            record = result.single()
            if record:
                try:
                    script_name = record['script_name']
                    logger.info(f"找到表 {table_name} 的脚本名称: {script_name}")
                    return script_name
                except (KeyError, TypeError) as e:
                    logger.warning(f"记录中不包含script_name字段: {e}")
                    return None
            else:
                logger.warning(f"没有找到表 {table_name} 的脚本名称")
                return None
    except Exception as e:
        logger.error(f"查询表 {table_name} 的脚本名称时出错: {str(e)}")
        return None
    finally:
        driver.close()

# 判断给定表名是否是 Neo4j 中的 DataModel 类型
def is_data_model_table(table_name):
    uri = NEO4J_CONFIG['uri']
    auth = (NEO4J_CONFIG['user'], NEO4J_CONFIG['password'])
    driver = GraphDatabase.driver(uri, auth=auth)
    query = """
        MATCH (n:DataModel {en_name: $table_name}) RETURN count(n) > 0 AS exists
    """
    try:
        with driver.session() as session:
            result = session.run(query, table_name=table_name)
            record = result.single()
            return record and record['exists']
    finally:
        driver.close()

def check_script_exists(script_name):
    """
    检查脚本文件是否存在于配置的脚本目录中
    
    参数:
        script_name (str): 脚本文件名
        
    返回:
        bool: 如果脚本存在返回True，否则返回False
        str: 完整的脚本路径
    """
    if not script_name:
        logger.error("脚本名称为空，无法检查")
        return False, None
    
    script_path = Path(SCRIPTS_BASE_PATH) / script_name
    script_path_str = str(script_path)
    
    logger.info(f"检查脚本路径: {script_path_str}")
    
    if os.path.exists(script_path_str):
        logger.info(f"脚本文件已找到: {script_path_str}")
        return True, script_path_str
    else:
        logger.error(f"脚本文件不存在: {script_path_str}")
        
        # 尝试列出目录中的文件
        try:
            base_dir = Path(SCRIPTS_BASE_PATH)
            if base_dir.exists():
                files = list(base_dir.glob("*.py"))
                logger.info(f"目录 {SCRIPTS_BASE_PATH} 中的Python文件: {[f.name for f in files]}")
            else:
                logger.error(f"基础目录不存在: {SCRIPTS_BASE_PATH}")
        except Exception as e:
            logger.error(f"列出目录内容时出错: {str(e)}")
            
        return False, script_path_str

def run_model_script(table_name, update_mode):
    """
    执行与表关联的脚本
    
    参数:
        table_name (str): 表名
        update_mode (str): 更新模式，如append, full_refresh等
        
    返回:
        bool: 执行成功返回True，否则返回False
    """
    logger.info(f"执行表 {table_name} 关联的脚本")
    
    # 检查表类型
    is_model = is_data_model_table(table_name)
    
    if is_model:
        # 从Neo4j获取脚本名称
        script_name = get_script_name_from_neo4j(table_name)
        if not script_name:
            logger.error(f"未找到表 {table_name} 关联的脚本")
            return False
            
        logger.info(f"查询到表 {table_name} 关联的脚本: {script_name}")
        
        # 检查脚本文件是否存在
        script_exists, script_path = check_script_exists(script_name)
        
        if not script_exists:
            logger.error(f"脚本文件 {script_name} 不存在")
            return False
        
        logger.info(f"脚本文件路径: {script_path}")
        
        # 执行脚本
        try:
            # 包含PY扩展名时，确保使用完整文件名
            if not script_name.endswith('.py'):
                script_name = f"{script_name}.py"
                
            return execute_script(script_name=script_name, table_name=table_name, update_mode=update_mode)
        except Exception as e:
            logger.error(f"执行脚本 {script_name} 时发生错误: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return False
    else:
        logger.warning(f"表 {table_name} 不是DataModel类型，跳过脚本执行")
        return True


def get_model_dependency_graph(table_names: list) -> dict:
    """
    使用networkx从Neo4j获取指定DataModel表之间的依赖关系图    
    参数:
        table_names: 表名列表    
    返回:
        dict: 依赖关系字典 {目标表: [上游依赖表1, 上游依赖表2, ...]}
    """
    logger.info(f"开始构建依赖关系图，表列表: {table_names}")
    # 创建有向图
    G = nx.DiGraph()
    
    # 添加所有节点
    for table_name in table_names:
        G.add_node(table_name)
    
    # 从Neo4j获取依赖关系并添加边
    uri = NEO4J_CONFIG['uri']
    auth = (NEO4J_CONFIG['user'], NEO4J_CONFIG['password'])
    driver = GraphDatabase.driver(uri, auth=auth)
    try:
        with driver.session() as session:
            # 使用一次性查询获取所有表之间的依赖关系
            # 注意：这里查询的是 A-[:DERIVED_FROM]->B 关系，表示A依赖B
            
            # 记录原始查询参数用于调试
            logger.info(f"查询参数 table_names: {table_names}, 类型: {type(table_names)}")
            
            # 第一层查询 - 更明确的查询形式
            query = """
                MATCH (source)-[r:DERIVED_FROM]->(target)
                WHERE source.en_name IN $table_names AND target.en_name IN $table_names
                RETURN source.en_name AS source, target.en_name AS target, r.script_name AS script_name
            """
            logger.info(f"执行Neo4j查询: 查找所有表之间的依赖关系")
            result = session.run(query, table_names=table_names)
            
            # 转换结果为列表，确保结果被消费
            result_records = list(result)
            logger.info(f"第一层查询返回记录数: {len(result_records)}")
            
            # 处理依赖关系
            found_deps = 0
            # 初始化依赖字典
            dependency_dict = {name: [] for name in table_names}
            
            # 这里是问题所在 - 需要正确处理记录
            for record in result_records:
                # 直接将记录转换为字典，避免访问问题
                record_dict = dict(record)
                
                # 从字典中获取值
                source = record_dict.get('source')
                target = record_dict.get('target') 
                script_name = record_dict.get('script_name', 'unknown_script')
                
                # 确保字段存在且有值
                if source and target:
                    logger.info(f"发现依赖关系: {source} -[:DERIVED_FROM]-> {target}, 脚本: {script_name}")
                    
                    # 添加依赖关系到字典
                    if source in dependency_dict:
                        dependency_dict[source].append(target)
                        found_deps += 1
                        
                        # 添加边到图 - 把被依赖方指向依赖方，表示执行顺序（被依赖方先执行）
                        G.add_edge(target, source)
                        logger.info(f"添加执行顺序边: {target} -> {source} (因为{source}依赖{target})")
            
            logger.info(f"总共发现 {found_deps} 个依赖关系")
            
            # 如果没有找到依赖关系，尝试检查所有可能的表对关系
            if found_deps == 0:
                logger.warning("仍未找到依赖关系，尝试检查所有表对之间的关系")
                logger.info("第三层查询: 开始表对之间的循环检查")
                logger.info(f"要检查的表对数量: {len(table_names) * (len(table_names) - 1)}")
                
                pair_count = 0
                for source_table in table_names:
                    for target_table in table_names:
                        if source_table != target_table:
                            pair_count += 1
                            logger.info(f"检查表对[{pair_count}]: {source_table} -> {target_table}")
                            
                            check_result = check_table_relationship(source_table, target_table)
                            
                            # 检查forward方向的关系
                            if 'forward' in check_result and check_result['forward']['exists']:
                                script_name = check_result['forward'].get('script_name', 'unknown_script')
                                logger.info(f"表对检查发现关系: {source_table} -[:DERIVED_FROM]-> {target_table}, 脚本: {script_name}")
                                
                                dependency_dict[source_table].append(target_table)
                                G.add_edge(target_table, source_table)
                                found_deps += 1
                
                logger.info(f"表对检查后找到 {found_deps} 个依赖关系")
    finally:
        driver.close()
    
    # 检测循环依赖
    try:
        cycles = list(nx.simple_cycles(G))
        if cycles:
            logger.warning(f"检测到表间循环依赖: {cycles}")
    except Exception as e:
        logger.error(f"检查循环依赖失败: {str(e)}")
    
    # 将图转换为字典格式
    final_dependency_dict = {}
    for table_name in table_names:
        final_dependency_dict[table_name] = dependency_dict.get(table_name, [])
        logger.info(f"最终依赖关系 - 表 {table_name} 依赖于: {final_dependency_dict[table_name]}")
    
    logger.info(f"完整依赖图: {final_dependency_dict}")
    return final_dependency_dict


def generate_optimized_execution_order(table_names, dependency_dict=None):
    """
    生成优化的执行顺序，处理循环依赖
    
    参数:
        table_names: 表名列表
        dependency_dict: 依赖关系字典 {表名: [依赖表1, 依赖表2, ...]}
                        如果为None，则通过get_model_dependency_graph获取
    
    返回:
        list: 优化后的执行顺序列表
    """
    # 创建有向图
    G = nx.DiGraph()
    
    # 添加所有节点
    for table_name in table_names:
        G.add_node(table_name)
    
    # 获取依赖关系
    if dependency_dict is None:
        # 使用原始utils.py的get_model_dependency_graph获取依赖
        dependency_dict = get_model_dependency_graph(table_names)
        # 添加依赖边 - 从上游指向目标
        for target, upstreams in dependency_dict.items():
            for upstream in upstreams:
                G.add_edge(upstream, target)
    else:
        # 使用提供的dependency_dict - 从依赖指向目标
        for target, sources in dependency_dict.items():
            for source in sources:
                if source in table_names:  # 确保只考虑目标表集合中的表
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


def check_table_relationship(table1, table2):
    """
    直接检查Neo4j中两个表之间的关系
    
    参数:
        table1: 第一个表名
        table2: 第二个表名
    
    返回:
        关系信息字典
    """
    uri = NEO4J_CONFIG['uri']
    auth = (NEO4J_CONFIG['user'], NEO4J_CONFIG['password'])
    driver = GraphDatabase.driver(uri, auth=auth)
    
    relationship_info = {}
    
    try:
        with driver.session() as session:
            # 检查 table1 -> table2 方向
            forward_query = """
                MATCH (a:DataModel {en_name: $table1})-[r:DERIVED_FROM]->(b:DataModel {en_name: $table2})
                RETURN count(r) > 0 AS has_relationship, r.script_name AS script_name
            """
            forward_result = session.run(forward_query, table1=table1, table2=table2)
            forward_record = forward_result.single()
            
            if forward_record and forward_record['has_relationship']:
                relationship_info['forward'] = {
                    'exists': True,
                    'direction': f"{table1} -> {table2}",
                    'script_name': forward_record.get('script_name')
                }
                logger.info(f"发现关系: {table1} -[:DERIVED_FROM]-> {table2}, 脚本: {forward_record.get('script_name')}")
            else:
                relationship_info['forward'] = {'exists': False}
                
            # 检查 table2 -> table1 方向
            backward_query = """
                MATCH (a:DataModel {en_name: $table2})-[r:DERIVED_FROM]->(b:DataModel {en_name: $table1})
                RETURN count(r) > 0 AS has_relationship, r.script_name AS script_name
            """
            backward_result = session.run(backward_query, table1=table1, table2=table2)
            backward_record = backward_result.single()
            
            if backward_record and backward_record['has_relationship']:
                relationship_info['backward'] = {
                    'exists': True,
                    'direction': f"{table2} -> {table1}",
                    'script_name': backward_record.get('script_name')
                }
                logger.info(f"发现关系: {table2} -[:DERIVED_FROM]-> {table1}, 脚本: {backward_record.get('script_name')}")
            else:
                relationship_info['backward'] = {'exists': False}
                
    except Exception as e:
        logger.error(f"检查表关系时出错: {str(e)}")
        relationship_info['error'] = str(e)
    finally:
        driver.close()
        
    return relationship_info

def build_model_dependency_dag(table_names, model_tables):
    """
    基于表名列表构建模型依赖DAG，返回优化后的执行顺序和依赖关系图
    
    参数:
        table_names: 表名列表
        model_tables: 表配置列表
        
    返回:
        tuple: (优化后的表执行顺序, 依赖关系图)
    """
    # 使用优化函数生成执行顺序，可以处理循环依赖
    optimized_table_order = generate_optimized_execution_order(table_names)
    logger.info(f"生成优化执行顺序, 共 {len(optimized_table_order)} 个表")
    
    # 获取依赖图
    dependency_graph = get_model_dependency_graph(table_names)
    logger.info(f"构建了 {len(dependency_graph)} 个表的依赖关系图")
    
    return optimized_table_order, dependency_graph


def create_task_dict(optimized_table_order, model_tables, dag, execution_type, **task_options):
    """
    根据优化后的表执行顺序创建任务字典
    
    参数:
        optimized_table_order: 优化后的表执行顺序
        model_tables: 表配置列表
        dag: Airflow DAG对象
        execution_type: 执行类型（daily, monthly等）
        task_options: 任务创建的额外选项
        
    返回:
        dict: 任务字典 {表名: 任务对象}
    """
    from airflow.operators.python import PythonOperator
    
    task_dict = {}
    for table_name in optimized_table_order:
        # 获取表的配置信息
        table_config = next((t for t in model_tables if t['table_name'] == table_name), None)
        if table_config:
            try:
                # 构建基础参数
                task_params = {
                    "task_id": f"process_{execution_type}_{table_name}",
                    "python_callable": run_model_script,
                    "op_kwargs": {"table_name": table_name, "update_mode": table_config['update_mode']},
                    "dag": dag
                }
                
                # 添加额外选项
                if task_options:
                    # 如果有表特定的选项，使用它们
                    if table_name in task_options:
                        task_params.update(task_options[table_name])
                    # 如果有全局选项，使用它们
                    elif 'default' in task_options:
                        task_params.update(task_options['default'])
                
                task = PythonOperator(**task_params)
                task_dict[table_name] = task
                logger.info(f"创建模型处理任务: {task_params['task_id']}")
            except Exception as e:
                logger.error(f"创建任务 process_{execution_type}_{table_name} 时出错: {str(e)}")
                raise
    return task_dict


def build_task_dependencies(task_dict, dependency_graph):
    """
    根据依赖图设置任务间的依赖关系
    
    参数:
        task_dict: 任务字典
        dependency_graph: 依赖关系图
    
    返回:
        tuple: (tasks_with_upstream, tasks_with_downstream, dependency_count)
    """
    tasks_with_upstream = set()  # 用于跟踪已经有上游任务的节点
    dependency_count = 0
    
    for target, upstream_list in dependency_graph.items():
        if target in task_dict:
            for upstream in upstream_list:
                if upstream in task_dict:
                    logger.info(f"建立任务依赖: {upstream} >> {target}")
                    task_dict[upstream] >> task_dict[target]
                    tasks_with_upstream.add(target)  # 记录此任务已有上游
                    dependency_count += 1
    
    # 找出有下游任务的节点
    tasks_with_downstream = set()
    for target, upstream_list in dependency_graph.items():
        if target in task_dict:  # 目标任务在当前DAG中
            for upstream in upstream_list:
                if upstream in task_dict:  # 上游任务也在当前DAG中
                    tasks_with_downstream.add(upstream)  # 这个上游任务有下游
    
    logger.info(f"总共建立了 {dependency_count} 个任务之间的依赖关系")
    logger.info(f"已有上游任务的节点: {tasks_with_upstream}")
    
    return tasks_with_upstream, tasks_with_downstream, dependency_count


def connect_start_and_end_tasks(task_dict, tasks_with_upstream, tasks_with_downstream, 
                               wait_task, completed_task, dag_type):
    """
    连接开始节点到等待任务，末端节点到完成标记
    
    参数:
        task_dict: 任务字典
        tasks_with_upstream: 有上游任务的节点集合
        tasks_with_downstream: 有下游任务的节点集合
        wait_task: 等待任务
        completed_task: 完成标记任务
        dag_type: DAG类型名称（用于日志）
    
    返回:
        tuple: (start_tasks, end_tasks)
    """
    # 连接开始节点
    start_tasks = []
    for table_name, task in task_dict.items():
        if table_name not in tasks_with_upstream:
            start_tasks.append(table_name)
            logger.info(f"任务 {table_name} 没有上游任务，应该连接到{dag_type}等待任务")
    
    logger.info(f"需要连接到{dag_type}等待任务的任务: {start_tasks}")
    
    for task_name in start_tasks:
        wait_task >> task_dict[task_name]
        logger.info(f"连接 {wait_task.task_id} >> {task_name}")
    
    # 连接末端节点
    end_tasks = []
    for table_name, task in task_dict.items():
        if table_name not in tasks_with_downstream:
            end_tasks.append(table_name)
            logger.info(f"任务 {table_name} 没有下游任务，是末端任务")
    
    logger.info(f"需要连接到{dag_type}完成标记的末端任务: {end_tasks}")
    
    for end_task in end_tasks:
        task_dict[end_task] >> completed_task
        logger.info(f"连接 {end_task} >> {completed_task.task_id}")
    
    # 处理特殊情况
    logger.info("处理特殊情况")
    if not start_tasks:
        logger.warning(f"没有找到开始任务，将{dag_type}等待任务直接连接到完成标记")
        wait_task >> completed_task
    
    if not end_tasks:
        logger.warning(f"没有找到末端任务，将所有任务连接到{dag_type}完成标记")
        for table_name, task in task_dict.items():
            task >> completed_task
            logger.info(f"直接连接任务到完成标记: {table_name} >> {completed_task.task_id}")
    
    return start_tasks, end_tasks


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

def execute_with_monitoring(target_table, script_name, update_mode, exec_date, **kwargs):
    """
    执行脚本并监控执行状态，更新到airflow_exec_plans表
    
    参数:
        target_table: 目标表名
        script_name: 脚本名称
        update_mode: 更新模式(append/full_refresh)
        exec_date: 执行日期
        **kwargs: 其他参数
        
    返回:
        bool: 执行成功返回True，否则返回False
    """
    conn = None
    start_time = datetime.now()
    
    try:
        # 记录任务开始执行
        update_task_start_time(exec_date, target_table, script_name, start_time)
        
        # 执行脚本
        script_path = os.path.join(SCRIPTS_BASE_PATH, script_name)
        
        # 构建执行参数
        exec_kwargs = {
            "table_name": target_table,
            "update_mode": update_mode,
            "exec_date": exec_date,
        }
        
        # 添加其他传入的参数
        exec_kwargs.update(kwargs)
        
        # 检查脚本是否存在
        if not os.path.exists(script_path):
            logger.error(f"脚本文件不存在: {script_path}")
            success = False
        else:
            # 执行脚本
            try:
                # 动态导入模块
                import importlib.util
                import sys
                
                spec = importlib.util.spec_from_file_location("dynamic_module", script_path)
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)
                
                # 检查并调用标准入口函数run
                if hasattr(module, "run"):
                    logger.info(f"调用脚本 {script_name} 的标准入口函数 run()")
                    result = module.run(**exec_kwargs)
                    success = bool(result)  # 确保结果是布尔类型
                else:
                    logger.error(f"脚本 {script_name} 中未定义标准入口函数 run()")
                    success = False
            except Exception as e:
                logger.error(f"执行脚本 {script_name} 时出错: {str(e)}")
                import traceback
                logger.error(traceback.format_exc())
                success = False
        
        # 记录结束时间
        end_time = datetime.now()
        
        # 计算执行时间
        duration = (end_time - start_time).total_seconds()
        
        # 更新任务执行结果
        update_task_completion(exec_date, target_table, script_name, success, end_time, duration)
        
        return success
    except Exception as e:
        # 记录结束时间
        end_time = datetime.now()
        
        # 计算执行时间
        duration = (end_time - start_time).total_seconds()
        
        # 更新任务执行失败
        try:
            update_task_completion(exec_date, target_table, script_name, False, end_time, duration)
        except Exception as update_err:
            logger.error(f"更新任务状态失败: {str(update_err)}")
        
        logger.error(f"执行脚本 {script_name} 发生未处理的异常: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return False

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

def get_today_date():
    """获取今天的日期，返回YYYY-MM-DD格式字符串"""
    return datetime.now().strftime("%Y-%m-%d")

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

def get_table_label(table_name):
    """确定表的标签类型（DataModel or DataResource）"""
    driver = GraphDatabase.driver(
        NEO4J_CONFIG['uri'], 
        auth=(NEO4J_CONFIG['user'], NEO4J_CONFIG['password'])
    )
    query = """
        MATCH (n {en_name: $table_name})
        RETURN labels(n) AS labels
    """
    try:
        with driver.session() as session:
            result = session.run(query, table_name=table_name)
            record = result.single()
            if record and record.get("labels"):
                labels = record.get("labels")
                if "DataModel" in labels:
                    return "DataModel"
                elif "DataResource" in labels:
                    return "DataResource"
                elif "DataSource" in labels:
                    return "DataSource"
            return None
    except Exception as e:
        logger.error(f"获取表 {table_name} 的标签时出错: {str(e)}")
        return None
    finally:
        driver.close()

def get_complete_script_info(script_name=None, target_table=None):
    """
    一次性从Neo4j获取脚本和表的完整信息，包括update_mode, schedule_frequency等
    
    参数:
        script_name (str, optional): 脚本名称
        target_table (str): 目标表名
        
    返回:
        dict: 包含完整脚本信息的字典
    """
    if not target_table:
        return None
        
    logger.info(f"从Neo4j获取表 {target_table} 的完整信息")
    
    # 连接Neo4j
    driver = GraphDatabase.driver(
        NEO4J_CONFIG['uri'], 
        auth=(NEO4J_CONFIG['user'], NEO4J_CONFIG['password'])
    )
    
    # 获取表的标签类型
    table_label = get_table_label(target_table)
    
    script_info = {
        'script_name': script_name,
        'target_table': target_table,
        'script_id': f"{script_name.replace('.', '_') if script_name else ''}_{target_table}",
        'target_table_label': table_label,
        'source_tables': [],
        'script_type': 'python_script',  # 默认类型
        'update_mode': 'append',  # 默认更新模式
        'schedule_frequency': 'daily',  # 默认调度频率
        'schedule_status': 'enabled'  # 默认调度状态
    }
    
    try:
        with driver.session() as session:
            # 检查是否为structure类型的DataResource
            if table_label == 'DataResource':
                query_structure = """
                    MATCH (n:DataResource {en_name: $table_name})
                    RETURN n.type AS type, n.storage_location AS storage_location, 
                           n.schedule_frequency AS schedule_frequency,
                           n.update_mode AS update_mode,
                           n.schedule_status AS schedule_status
                """
                
                result = session.run(query_structure, table_name=target_table)
                record = result.single()
                
                if record and record.get("type") == "structure":
                    logger.info(f"表 {target_table} 是structure类型的DataResource")
                    
                    # 设置特殊属性
                    script_info['target_type'] = 'structure'
                    
                    # 从节点属性获取信息
                    if record.get("storage_location"):
                        script_info['storage_location'] = record.get("storage_location")
                    
                    # 获取调度频率
                    if record.get("schedule_frequency"):
                        script_info['schedule_frequency'] = record.get("schedule_frequency")
                    
                    # 获取更新模式
                    if record.get("update_mode"):
                        script_info['update_mode'] = record.get("update_mode")
                    
                    # 获取调度状态
                    if record.get("schedule_status"):
                        script_info['schedule_status'] = record.get("schedule_status")
                    
                    # 如果没有指定脚本名称或指定的是default，则设置为load_file.py
                    if not script_name or script_name.lower() == 'default' or script_name == 'load_file.py':
                        script_info['script_name'] = 'load_file.py'
                        script_info['script_id'] = f"load_file_py_{target_table}"
                        return script_info
            
            # 非structure类型，或structure类型但有指定脚本名称
            # 根据表标签类型查询脚本信息和依赖关系
            if script_info['target_table_label'] == 'DataModel':
                # 查询DataModel的所有属性和依赖
                query = """
                    MATCH (target:DataModel {en_name: $table_name})-[rel:DERIVED_FROM]->(source)
                    RETURN source.en_name AS source_table, 
                           rel.script_name AS script_name, 
                           rel.script_type AS script_type,
                           rel.update_mode AS update_mode,
                           rel.schedule_frequency AS schedule_frequency,
                           rel.schedule_status AS schedule_status
                """
                result = session.run(query, table_name=target_table)
                
                for record in result:
                    source_table = record.get("source_table")
                    db_script_name = record.get("script_name")
                    
                    # 验证脚本名称匹配或未指定脚本名称
                    if not script_name or (db_script_name and db_script_name == script_name):
                        if source_table and source_table not in script_info['source_tables']:
                            script_info['source_tables'].append(source_table)
                        
                        # 只在匹配脚本名称时更新这些属性
                        if db_script_name and db_script_name == script_name:
                            # 更新脚本信息
                            script_info['script_type'] = record.get("script_type", script_info['script_type'])
                            script_info['update_mode'] = record.get("update_mode", script_info['update_mode'])
                            script_info['schedule_frequency'] = record.get("schedule_frequency", script_info['schedule_frequency'])
                            script_info['schedule_status'] = record.get("schedule_status", script_info['schedule_status'])
                            
                            # 如果未指定脚本名称，则使用查询到的脚本名称
                            if not script_info['script_name'] and db_script_name:
                                script_info['script_name'] = db_script_name
                                script_info['script_id'] = f"{db_script_name.replace('.', '_')}_{target_table}"
            
            elif script_info['target_table_label'] == 'DataResource':
                # 查询DataResource的所有属性和依赖
                query = """
                    MATCH (target:DataResource {en_name: $table_name})-[rel:ORIGINATES_FROM]->(source)
                    RETURN source.en_name AS source_table, 
                           rel.script_name AS script_name, 
                           rel.script_type AS script_type,
                           rel.update_mode AS update_mode,
                           rel.schedule_frequency AS schedule_frequency,
                           rel.schedule_status AS schedule_status
                """
                result = session.run(query, table_name=target_table)
                
                for record in result:
                    source_table = record.get("source_table")
                    db_script_name = record.get("script_name")
                    
                    # 验证脚本名称匹配或未指定脚本名称
                    if not script_name or (db_script_name and db_script_name == script_name):
                        if source_table and source_table not in script_info['source_tables']:
                            script_info['source_tables'].append(source_table)
                        
                        # 只在匹配脚本名称时更新这些属性
                        if db_script_name and db_script_name == script_name:
                            # 更新脚本信息
                            script_info['script_type'] = record.get("script_type", script_info['script_type'])
                            script_info['update_mode'] = record.get("update_mode", script_info['update_mode'])
                            script_info['schedule_frequency'] = record.get("schedule_frequency", script_info['schedule_frequency'])
                            script_info['schedule_status'] = record.get("schedule_status", script_info['schedule_status'])
                            
                            # 如果未指定脚本名称，则使用查询到的脚本名称
                            if not script_info['script_name'] and db_script_name:
                                script_info['script_name'] = db_script_name
                                script_info['script_id'] = f"{db_script_name.replace('.', '_')}_{target_table}"
    
    except Exception as e:
        logger.error(f"从Neo4j获取表 {target_table} 的信息时出错: {str(e)}")
    finally:
        if driver:
            driver.close()
    
    logger.info(f"获取到完整脚本信息: {script_info}")
    return script_info