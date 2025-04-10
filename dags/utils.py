# utils.py
import psycopg2
from neo4j import GraphDatabase
from config import PG_CONFIG, NEO4J_CONFIG, SCRIPTS_BASE_PATH
import logging
import importlib.util
from pathlib import Path
import networkx as nx
import os
from airflow.exceptions import AirflowFailException

# 创建统一的日志记录器
logger = logging.getLogger("airflow.task")

def get_pg_conn():
    return psycopg2.connect(**PG_CONFIG)

def get_subscribed_tables(freq: str) -> list[dict]:
    """
    根据调度频率获取启用的订阅表列表，附带 execution_mode 参数
    返回结果示例：
    [
        {'table_name': 'region_sales', 'execution_mode': 'append'},
        {'table_name': 'catalog_sales', 'execution_mode': 'full_refresh'}
    ]
    """
    conn = get_pg_conn()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT table_name, execution_mode 
        FROM table_schedule 
        WHERE is_enabled = TRUE AND schedule_frequency = %s
    """, (freq,))
    result = cursor.fetchall()
    cursor.close()
    conn.close()
    return [{"table_name": r[0], "execution_mode": r[1]} for r in result]


def get_neo4j_dependencies(table_name: str) -> list:
    """
    查询 Neo4j 中某个模型的 DERIVED_FROM 依赖（上游表名）
    """
    uri = NEO4J_CONFIG['uri']
    auth = (NEO4J_CONFIG['user'], NEO4J_CONFIG['password'])
    driver = GraphDatabase.driver(uri, auth=auth)
    query = """
        MATCH (a:Table {name: $name})<-[:DERIVED_FROM]-(b:Table)
        RETURN b.name
    """
    with driver.session() as session:
        records = session.run(query, name=table_name)
        return [record["b.name"] for record in records]

# def get_script_name_from_neo4j(table_name: str) -> str:
#     """
#     从Neo4j数据库中查询表对应的脚本名称
#     查询的是 DataResource 和 DataSource 之间的 ORIGINATES_FROM 关系中的 script_name 属性
    
#     参数:
#         table_name (str): 数据资源表名
        
#     返回:
#         str: 脚本名称，如果未找到则返回None
#     """
#     logger = logging.getLogger("airflow.task")
    
#     driver = GraphDatabase.driver(**NEO4J_CONFIG)
#     query = """
#         MATCH (dr:DataResource {en_name: $table_name})-[rel:ORIGINATES_FROM]->(ds:DataSource)
#         RETURN rel.script_name AS script_name
#     """
#     try:
#         with driver.session() as session:
#             result = session.run(query, table_name=table_name)
#             record = result.single()
#             if record and 'script_name' in record:
#                 return record['script_name']
#             else:
#                 logger.warning(f"没有找到表 {table_name} 对应的脚本名称")
#                 return None
#     except Exception as e:
#         logger.error(f"从Neo4j查询脚本名称时出错: {str(e)}")
#         return None
#     finally:
#         driver.close()

def execute_script(script_name: str, table_name: str, execution_mode: str) -> bool:
    """
    根据脚本名称动态导入并执行对应的脚本        
    返回:
        bool: 执行成功返回True，否则返回False
    """
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
            module.run(table_name=table_name, execution_mode=execution_mode)
            return True
        else:
            logger.warning(f"脚本 {script_name} 未定义标准入口函数 run()，无法执行")
            return False
    except Exception as e:
        logger.error(f"执行脚本 {script_name} 时出错: {str(e)}")
        return False


# def get_enabled_tables(frequency: str) -> list:
#     conn = get_pg_conn()
#     cursor = conn.cursor()
#     cursor.execute("""
#         SELECT table_name, execution_mode
#         FROM table_schedule
#         WHERE is_enabled = TRUE AND schedule_frequency = %s
#     """, (frequency,))
#     result = cursor.fetchall()
#     cursor.close()
#     conn.close()

#     output = []
#     for r in result:
#         output.append({"table_name": r[0], "execution_mode": r[1]})
#     return output

# def is_data_resource_table(table_name: str) -> bool:
#     driver = GraphDatabase.driver(NEO4J_CONFIG['uri'], auth=(NEO4J_CONFIG['user'], NEO4J_CONFIG['password']))
#     query = """
#         MATCH (n:DataResource {en_name: $table_name}) RETURN count(n) > 0 AS exists
#     """
#     try:
#         with driver.session() as session:
#             result = session.run(query, table_name=table_name)
#             record = result.single()
#             return record and record["exists"]
#     finally:
#         driver.close()

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
    conn = get_pg_conn()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT table_name, execution_mode
        FROM table_schedule
        WHERE is_enabled = TRUE AND schedule_frequency = %s
    """, (frequency,))
    result = cursor.fetchall()
    cursor.close()
    conn.close()

    output = []
    for r in result:
        output.append({"table_name": r[0], "execution_mode": r[1]})
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

def run_model_script(table_name, execution_mode):
    """
    根据表名查找并执行对应的模型脚本
    
    参数:
        table_name (str): 要处理的表名
        execution_mode (str): 执行模式 (append/full_refresh)
    
    返回:
        bool: 执行成功返回True，否则返回False
        
    抛出:
        AirflowFailException: 如果脚本不存在或执行失败
    """
    # 从Neo4j获取脚本名称
    script_name = get_script_name_from_neo4j(table_name)
    if not script_name:
        error_msg = f"未找到表 {table_name} 的脚本名称，任务失败"
        logger.error(error_msg)
        raise AirflowFailException(error_msg)
    
    logger.info(f"从Neo4j获取到表 {table_name} 的脚本名称: {script_name}")
    
    # 检查脚本文件是否存在
    exists, script_path = check_script_exists(script_name)
    if not exists:
        error_msg = f"表 {table_name} 的脚本文件 {script_name} 不存在，任务失败"
        logger.error(error_msg)
        raise AirflowFailException(error_msg)
    
    # 执行脚本
    logger.info(f"开始执行脚本: {script_path}")
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
            module.run(table_name=table_name, execution_mode=execution_mode)
            logger.info(f"脚本 {script_name} 执行成功")
            return True
        else:
            error_msg = f"脚本 {script_name} 中未定义标准入口函数 run()，任务失败"
            logger.error(error_msg)
            raise AirflowFailException(error_msg)
    except AirflowFailException:
        # 直接重新抛出Airflow异常
        raise
    except Exception as e:
        error_msg = f"执行脚本 {script_name} 时出错: {str(e)}"
        logger.error(error_msg)
        import traceback
        logger.error(traceback.format_exc())
        raise AirflowFailException(error_msg)

# 从 Neo4j 获取指定 DataModel 表之间的依赖关系图
# 返回值为 dict：{目标表: [上游依赖表1, 上游依赖表2, ...]}
# def get_model_dependency_graph(table_names: list) -> dict:
#     graph = {}
#     uri = NEO4J_CONFIG['uri']
#     auth = (NEO4J_CONFIG['user'], NEO4J_CONFIG['password'])
#     driver = GraphDatabase.driver(uri, auth=auth)
#     try:
#         with driver.session() as session:
#             for table_name in table_names:
#                 query = """
#                     MATCH (t:DataModel {en_name: $table_name})<-[:DERIVED_FROM]-(up:DataModel)
#                     RETURN up.en_name AS upstream
#                 """
#                 result = session.run(query, table_name=table_name)
#                 deps = [record['upstream'] for record in result if 'upstream' in record]
#                 graph[table_name] = deps
#     finally:
#         driver.close()
#     return graph
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


def generate_optimized_execution_order(table_names: list) -> list:
    """
    生成优化的执行顺序，可处理循环依赖
    
    参数:
        table_names: 表名列表
    
    返回:
        list: 优化后的执行顺序列表
    """
    # 创建依赖图
    G = nx.DiGraph()
    
    # 添加所有节点
    for table_name in table_names:
        G.add_node(table_name)
    
    # 添加依赖边
    dependency_dict = get_model_dependency_graph(table_names)
    for target, upstreams in dependency_dict.items():
        for upstream in upstreams:
            G.add_edge(upstream, target)
    
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



def identify_common_paths(table_names: list) -> dict:
    """
    识别多个表之间的公共执行路径
    
    参数:
        table_names: 表名列表
    
    返回:
        dict: 公共路径信息 {(path_tuple): 使用次数}
    """
    # 创建依赖图
    G = nx.DiGraph()
    
    # 添加所有节点和直接依赖边
    dependency_dict = get_model_dependency_graph(table_names)
    for target, upstreams in dependency_dict.items():
        G.add_node(target)
        for upstream in upstreams:
            G.add_node(upstream)
            G.add_edge(upstream, target)
    
    # 找出所有路径
    all_paths = []
    # 找出所有源节点（没有入边的节点）和终节点（没有出边的节点）
    sources = [n for n in G.nodes() if G.in_degree(n) == 0]
    targets = [n for n in G.nodes() if G.out_degree(n) == 0]
    
    # 获取所有源到目标的路径
    for source in sources:
        for target in targets:
            try:
                # 限制路径长度，避免组合爆炸
                paths = list(nx.all_simple_paths(G, source, target, cutoff=10))
                all_paths.extend(paths)
            except nx.NetworkXNoPath:
                continue
    
    # 统计路径段使用频率
    path_segments = {}
    for path in all_paths:
        # 只考虑长度>=2的路径段（至少有一条边）
        for i in range(len(path)-1):
            for j in range(i+2, min(i+6, len(path)+1)):  # 限制段长，避免组合爆炸
                segment = tuple(path[i:j])
                if segment not in path_segments:
                    path_segments[segment] = 0
                path_segments[segment] += 1
    
    # 过滤出重复使用的路径段
    common_paths = {seg: count for seg, count in path_segments.items() 
                    if count > 1 and len(seg) >= 3}  # 至少3个节点，2条边
    
    # 按使用次数排序
    common_paths = dict(sorted(common_paths.items(), key=lambda x: x[1], reverse=True))
    
    return common_paths

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
                    "op_kwargs": {"table_name": table_name, "execution_mode": table_config['execution_mode']},
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


def process_model_tables(enabled_tables, dag_type, wait_task, completed_task, dag, **task_options):
    """
    处理模型表并构建DAG
    
    参数:
        enabled_tables: 已启用的表列表
        dag_type: DAG类型 (daily, monthly等)
        wait_task: 等待任务
        completed_task: 完成标记任务
        dag: Airflow DAG对象
        task_options: 创建任务的额外选项
    """
    model_tables = [t for t in enabled_tables if is_data_model_table(t['table_name'])]
    logger.info(f"获取到 {len(model_tables)} 个启用的 {dag_type} 模型表")
    
    if not model_tables:
        # 如果没有模型表需要处理，直接将等待任务与完成标记相连接
        logger.info(f"没有找到需要处理的{dag_type}模型表，DAG将直接标记为完成")
        wait_task >> completed_task
        return
    
    # 获取表名列表
    table_names = [t['table_name'] for t in model_tables]
    
    try:
        # 构建模型依赖DAG
        optimized_table_order, dependency_graph = build_model_dependency_dag(table_names, model_tables)
        
        # 创建任务字典
        task_dict = create_task_dict(optimized_table_order, model_tables, dag, dag_type, **task_options)
        
        # 建立任务依赖关系
        tasks_with_upstream, tasks_with_downstream, _ = build_task_dependencies(task_dict, dependency_graph)
        
        # 连接开始节点和末端节点
        connect_start_and_end_tasks(task_dict, tasks_with_upstream, tasks_with_downstream, 
                                  wait_task, completed_task, dag_type)
        
    except Exception as e:
        logger.error(f"处理{dag_type}模型表时出错: {str(e)}")
        # 出错时也要确保完成标记被触发
        wait_task >> completed_task
        raise