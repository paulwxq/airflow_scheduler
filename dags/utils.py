# utils.py
import psycopg2
from neo4j import GraphDatabase
from config import PG_CONFIG, NEO4J_CONFIG, SCRIPTS_BASE_PATH
import logging
import importlib.util
from pathlib import Path
import sys
import os

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
    logger = logging.getLogger("airflow.task")
    
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
    query = """
        MATCH (target:DataModel {en_name: $table_name})-[r:DERIVED_FROM]->(n)
        WHERE n:DataModel OR n:DataResource
        RETURN r.script_name AS script_name
    """
    try:
        with driver.session() as session:
            result = session.run(query, table_name=table_name)
            record = result.single()
            if record:
                try:
                    script_name = record['script_name']
                    return script_name
                except (KeyError, TypeError) as e:
                    print(f"[WARN] 记录中不包含script_name字段: {e}")
                    return None
            else:
                return None
    except Exception as e:
        print(f"[ERROR] 查询表 {table_name} 的脚本名称时出错: {str(e)}")
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

# 检查脚本文件是否存在于指定路径
def check_script_exists(script_name):
    """
    检查脚本文件是否存在于配置的脚本目录中
    
    参数:
        script_name (str): 脚本文件名
        
    返回:
        bool: 如果脚本存在返回True，否则返回False
        str: 完整的脚本路径
    """
    from pathlib import Path
    import os
    import logging
    
    logger = logging.getLogger("airflow.task")
    
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

# 更新run_model_script函数以使用上述检查
def run_model_script(table_name, execution_mode):
    """
    根据表名查找并执行对应的模型脚本
    
    参数:
        table_name (str): 要处理的表名
        execution_mode (str): 执行模式 (append/full_refresh)
    
    返回:
        bool: 执行成功返回True，否则返回False
    """
    import logging
    logger = logging.getLogger("airflow.task")
    
    # 从Neo4j获取脚本名称
    script_name = get_script_name_from_neo4j(table_name)
    if not script_name:
        logger.error(f"未找到表 {table_name} 的脚本名称，跳过处理")
        return False
    
    logger.info(f"从Neo4j获取到表 {table_name} 的脚本名称: {script_name}")
    
    # 检查脚本文件是否存在
    exists, script_path = check_script_exists(script_name)
    if not exists:
        logger.error(f"表 {table_name} 的脚本文件 {script_name} 不存在，跳过处理")
        return False
    
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
            logger.error(f"脚本 {script_name} 中未定义标准入口函数 run()，无法执行")
            return False
    except Exception as e:
        logger.error(f"执行脚本 {script_name} 时出错: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return False

# 从 Neo4j 获取指定 DataModel 表之间的依赖关系图
# 返回值为 dict：{目标表: [上游依赖表1, 上游依赖表2, ...]}
def get_model_dependency_graph(table_names: list) -> dict:
    graph = {}
    uri = NEO4J_CONFIG['uri']
    auth = (NEO4J_CONFIG['user'], NEO4J_CONFIG['password'])
    driver = GraphDatabase.driver(uri, auth=auth)
    try:
        with driver.session() as session:
            for table_name in table_names:
                query = """
                    MATCH (t:DataModel {en_name: $table_name})<-[:DERIVED_FROM]-(up:DataModel)
                    RETURN up.en_name AS upstream
                """
                result = session.run(query, table_name=table_name)
                deps = [record['upstream'] for record in result if 'upstream' in record]
                graph[table_name] = deps
    finally:
        driver.close()
    return graph
