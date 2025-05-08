#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
import sys
import os
from datetime import datetime, timedelta
import sqlalchemy
from sqlalchemy import create_engine, inspect, Table, Column, MetaData, text
import pandas as pd
import traceback
import pendulum

# 修改Python导入路径，确保能找到同目录下的script_utils模块
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.insert(0, current_dir)

# 导入脚本工具模块
try:
    import script_utils
    from script_utils import get_pg_config, get_date_range, get_one_day_range, get_neo4j_driver, get_target_dt_column
    logger_utils = logging.getLogger("script_utils")
except ImportError as e:
    logger_utils = None
    print(f"导入script_utils模块失败: {str(e)}")

# 配置日志记录器
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger("load_data")

def get_source_database_info(table_name, script_name=None):
    """
    根据表名和脚本名从Neo4j获取源数据库连接信息
    
    参数:
        table_name (str): 表名
        script_name (str, optional): 脚本名称
        
    返回:
        dict: 数据库连接信息字典
    """
    logger.info(f"获取表 {table_name} 的源数据库连接信息")
    
    driver = get_neo4j_driver()
    try:
        with driver.session() as session:
            # 首先查询表对应的源节点
            query = """
                MATCH (target {en_name: $table_name})-[rel]->(source)
                RETURN source.en_name AS source_name, 
                       source.database AS database,
                       source.host AS host,
                       source.port AS port,
                       source.username AS username,
                       source.password AS password,
                       source.type AS db_type,
                       source.schema AS schema,
                       source.table AS source_table,
                       labels(source) AS labels
            """
            
            result = session.run(query, table_name=table_name)
            record = result.single()
            
            if not record:
                logger.warning(f"未找到表 {table_name} 对应的源节点")
                return None
            
            # 获取源节点的数据库连接信息
            database_info = {
                "source_name": record.get("source_name"),
                "database": record.get("database"),
                "host": record.get("host"),
                "port": record.get("port"),
                "username": record.get("username"),
                "password": record.get("password"),
                "db_type": record.get("db_type"),
                "schema": record.get("schema", "public"),
                "source_table": record.get("source_table"),
                "labels": record.get("labels", [])
            }
            
            # 检查是否包含数据库连接信息
            if not database_info.get("database"):
                logger.warning(f"源节点 {database_info['source_name']} 没有数据库连接信息")
                return None
            
            logger.info(f"成功获取表 {table_name} 的源数据库连接信息: {database_info['host']}:{database_info['port']}/{database_info['database']}")
            return database_info
    except Exception as e:
        logger.error(f"获取源数据库连接信息时出错: {str(e)}")
        logger.error(traceback.format_exc())
        return None
    finally:
        driver.close()

def get_target_database_info():
    """
    获取目标数据库连接信息
    
    返回:
        dict: 数据库连接信息字典
    
    异常:
        Exception: 无法获取目标数据库连接信息时抛出异常
    """
    logger.info("获取目标数据库连接信息")
    
    try:
        # 尝试从script_utils中获取PG_CONFIG
        pg_config = get_pg_config()
        
        if not pg_config:
            raise ValueError("无法获取PG_CONFIG配置")
            
        # 检查必要的配置项
        required_keys = ["host", "port", "user", "password", "database"]
        missing_keys = [key for key in required_keys if key not in pg_config]
        
        if missing_keys:
            raise ValueError(f"PG_CONFIG缺少必要的配置项: {', '.join(missing_keys)}")
        
        # 构建连接信息
        database_info = {
            "host": pg_config.get("host"),
            "port": pg_config.get("port"),
            "username": pg_config.get("user"),
            "password": pg_config.get("password"),
            "database": pg_config.get("database"),
            "db_type": "postgresql",
            "schema": "public"
        }
        
        logger.info(f"成功获取目标数据库连接信息: {database_info['host']}:{database_info['port']}/{database_info['database']}")
        return database_info
    except Exception as e:
        logger.error(f"获取目标数据库连接信息时出错: {str(e)}")
        logger.error(traceback.format_exc())
        raise  # 直接抛出异常，不提供默认连接信息

def get_sqlalchemy_engine(db_info):
    """
    根据数据库连接信息创建SQLAlchemy引擎
    
    参数:
        db_info (dict): 数据库连接信息
        
    返回:
        Engine: SQLAlchemy引擎对象
    """
    if not db_info:
        logger.error("数据库连接信息为空，无法创建SQLAlchemy引擎")
        return None
    
    try:
        db_type = db_info.get("db_type", "").lower()
        
        if db_type == "postgresql":
            url = f"postgresql://{db_info['username']}:{db_info['password']}@{db_info['host']}:{db_info['port']}/{db_info['database']}"
        elif db_type == "mysql":
            url = f"mysql+pymysql://{db_info['username']}:{db_info['password']}@{db_info['host']}:{db_info['port']}/{db_info['database']}"
        elif db_type == "oracle":
            url = f"oracle+cx_oracle://{db_info['username']}:{db_info['password']}@{db_info['host']}:{db_info['port']}/{db_info['database']}"
        elif db_type == "mssql":
            url = f"mssql+pymssql://{db_info['username']}:{db_info['password']}@{db_info['host']}:{db_info['port']}/{db_info['database']}"
        else:
            logger.error(f"不支持的数据库类型: {db_type}")
            return None
        
        # 创建数据库引擎
        engine = create_engine(url)
        return engine
    except Exception as e:
        logger.error(f"创建SQLAlchemy引擎时出错: {str(e)}")
        logger.error(traceback.format_exc())
        return None

def create_table_if_not_exists(source_engine, target_engine, source_table, target_table, schema="public"):
    """
    如果目标表不存在，则从源表复制表结构创建目标表
    
    参数:
        source_engine: 源数据库引擎
        target_engine: 目标数据库引擎
        source_table: 源表名
        target_table: 目标表名
        schema: 模式名称
        
    返回:
        bool: 操作是否成功
    """
    logger.info(f"检查目标表 {target_table} 是否存在，不存在则创建")
    
    try:
        # 检查目标表是否存在
        target_inspector = inspect(target_engine)
        target_exists = target_inspector.has_table(target_table, schema=schema)
        
        if target_exists:
            logger.info(f"目标表 {target_table} 已存在，无需创建")
            return True
        
        # 目标表不存在，从源表获取表结构
        source_inspector = inspect(source_engine)
        
        if not source_inspector.has_table(source_table):
            logger.error(f"源表 {source_table} 不存在")
            return False
        
        # 获取源表的列信息
        source_columns = source_inspector.get_columns(source_table)
        
        if not source_columns:
            logger.error(f"源表 {source_table} 没有列信息")
            return False
        
        # 创建元数据对象
        metadata = MetaData()
        
        # 定义目标表结构
        table_def = Table(
            target_table,
            metadata,
            *[Column(col['name'], col['type']) for col in source_columns],
            schema=schema
        )
        
        # 在目标数据库中创建表
        metadata.create_all(target_engine)
        logger.info(f"成功在目标数据库中创建表 {target_table}")
        
        return True
    except Exception as e:
        logger.error(f"创建表时出错: {str(e)}")
        logger.error(traceback.format_exc())
        return False

def load_data_from_source(table_name, exec_date=None, update_mode=None, script_name=None, 
                          schedule_frequency=None, is_manual_dag_trigger=False, **kwargs):
    """
    从源数据库加载数据到目标数据库
    
    参数:
        table_name (str): 表名
        exec_date: 执行日期
        update_mode (str): 更新模式，append或full_refresh
        script_name (str): 脚本名称
        schedule_frequency (str): 调度频率，daily、weekly、monthly等
        is_manual_dag_trigger (bool): 是否手动DAG触发
        **kwargs: 其他参数
    
    返回:
        bool: 操作是否成功
    """
    # 记录开始时间
    start_time = datetime.now()
    
    # 使用logger.info输出所有参数
    logger.info(f"===== 开始从源加载数据 =====")
    logger.info(f"表名: {table_name}")
    logger.info(f"执行日期: {exec_date}")
    logger.info(f"更新模式: {update_mode}")
    logger.info(f"脚本名称: {script_name}")
    logger.info(f"调度频率: {schedule_frequency}")
    logger.info(f"是否手动DAG触发: {is_manual_dag_trigger}")
    
    # 记录其他参数
    for key, value in kwargs.items():
        logger.info(f"其他参数 - {key}: {value}")
    
    # 如果exec_date为None，使用当前日期
    if exec_date is None:
        exec_date = datetime.now().strftime('%Y-%m-%d')
        logger.info(f"执行日期为空，使用当前日期: {exec_date}")
    
    # 如果schedule_frequency为None，默认使用daily
    if schedule_frequency is None:
        schedule_frequency = "daily"
        logger.info(f"调度频率为空，使用默认值: {schedule_frequency}")
    
    try:
        # 1. 获取源数据库连接信息
        source_db_info = get_source_database_info(table_name, script_name)
        
        if not source_db_info:
            logger.error(f"无法获取表 {table_name} 的源数据库连接信息，无法加载数据")
            return False
        
        # 2. 获取目标数据库连接信息
        target_db_info = get_target_database_info()
        
        # 3. 创建数据库引擎
        source_engine = get_sqlalchemy_engine(source_db_info)
        target_engine = get_sqlalchemy_engine(target_db_info)
        
        if not source_engine or not target_engine:
            logger.error("无法创建数据库引擎，无法加载数据")
            return False
        
        # 获取源表名
        source_table = source_db_info.get("source_table", table_name)
        
        # 4. 检查目标表是否存在，不存在则创建
        if not create_table_if_not_exists(source_engine, target_engine, source_table, table_name):
            logger.error(f"无法创建目标表 {table_name}，无法加载数据")
            return False
        
        # 5. 根据update_mode处理数据
        if update_mode == "full_refresh":
            # 清空目标表
            logger.info(f"执行全量刷新，清空表 {table_name}")
            with target_engine.connect() as conn:
                truncate_sql = f"TRUNCATE TABLE {table_name}"
                conn.execute(text(truncate_sql))
            logger.info(f"成功清空表 {table_name}")
        else:  # append 模式
            # 获取目标日期列
            target_dt_column = get_target_dt_column(table_name, script_name)
            
            if not target_dt_column:
                logger.error(f"无法获取表 {table_name} 的目标日期列，无法执行增量加载")
                return False
            
            # 计算日期范围
            try:
                if is_manual_dag_trigger:
                    # 手动DAG触发，使用target_dt_column进行数据清理
                    start_date, end_date = get_date_range(exec_date, schedule_frequency)
                    logger.info(f"手动DAG触发，日期范围: {start_date} 到 {end_date}")
                    
                    # 删除目标日期范围内的数据
                    delete_sql = f"""
                        DELETE FROM {table_name}
                        WHERE {target_dt_column} >= '{start_date}'
                        AND {target_dt_column} < '{end_date}'
                    """
                    
                    with target_engine.connect() as conn:
                        conn.execute(text(delete_sql))
                        logger.info(f"成功删除表 {table_name} 中 {target_dt_column} 从 {start_date} 到 {end_date} 的数据")
                else:
                    # 自动调度，使用create_time进行数据清理
                    start_datetime, end_datetime = get_one_day_range(exec_date)
                    start_date = start_datetime.strftime('%Y-%m-%d %H:%M:%S')
                    end_date = end_datetime.strftime('%Y-%m-%d %H:%M:%S')
                    logger.info(f"自动调度，日期范围: {start_date} 到 {end_date}")
                    
                    # 删除create_time在当天的数据
                    delete_sql = f"""
                        DELETE FROM {table_name}
                        WHERE create_time >= '{start_date}'
                        AND create_time < '{end_date}'
                    """
                    
                    try:
                        with target_engine.connect() as conn:
                            conn.execute(text(delete_sql))
                            logger.info(f"成功删除表 {table_name} 中 create_time 从 {start_date} 到 {end_date} 的数据")
                    except Exception as del_err:
                        logger.error(f"删除数据时出错: {str(del_err)}")
                        logger.warning("继续执行数据加载")
            except Exception as date_err:
                logger.error(f"计算日期范围时出错: {str(date_err)}")
                logger.error(traceback.format_exc())
                return False
        
        # 6. 加载数据
        try:
            # 构建查询条件
            if update_mode == "full_refresh":
                # 全量加载，不需要条件
                query = f"SELECT * FROM {source_table}"
            else:
                # 增量加载，使用目标日期列过滤
                # 获取日期范围
                start_date, end_date = get_date_range(exec_date, schedule_frequency)
                
                # 检查源表是否有目标日期列
                source_inspector = inspect(source_engine)
                source_columns = [col['name'].lower() for col in source_inspector.get_columns(source_table)]
                
                if target_dt_column.lower() in source_columns:
                    # 源表有目标日期列，使用它过滤数据
                    query = f"""
                        SELECT * FROM {source_table}
                        WHERE {target_dt_column} >= '{start_date}'
                        AND {target_dt_column} < '{end_date}'
                    """
                else:
                    # 源表没有目标日期列，加载全部数据
                    logger.warning(f"源表 {source_table} 没有目标日期列 {target_dt_column}，将加载全部数据")
                    query = f"SELECT * FROM {source_table}"
            
            # 读取源数据
            logger.info(f"执行查询: {query}")
            df = pd.read_sql(query, source_engine)
            
            # 检查数据是否为空
            if df.empty:
                logger.warning(f"查询结果为空，没有数据需要加载")
                return True
            
            # 添加create_time列（如果没有）
            if 'create_time' not in df.columns:
                df['create_time'] = datetime.now()
            
            # 写入目标数据库
            logger.info(f"开始写入数据到目标表 {table_name}，共 {len(df)} 行")
            df.to_sql(
                name=table_name,
                con=target_engine,
                if_exists='append',
                index=False,
                schema=target_db_info.get("schema", "public")
            )
            
            logger.info(f"成功写入数据到目标表 {table_name}")
            return True
        except Exception as load_err:
            logger.error(f"加载数据时出错: {str(load_err)}")
            logger.error(traceback.format_exc())
            return False
    except Exception as e:
        logger.error(f"执行数据加载过程时出错: {str(e)}")
        logger.error(traceback.format_exc())
        return False
    finally:
        # 记录结束时间和耗时
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        logger.info(f"数据加载过程结束，耗时: {int(duration // 60)}分钟 {int(duration % 60)}秒")
        logger.info(f"===== 数据加载结束 =====")

def run(table_name, update_mode, schedule_frequency=None, script_name=None, exec_date=None, is_manual_dag_trigger=False, **kwargs):
    """
    统一入口函数，符合Airflow动态脚本调用规范
    
    参数:
        table_name (str): 要处理的表名
        update_mode (str): 更新模式 (append/full_refresh)
        schedule_frequency (str): 调度频率
        script_name (str): 脚本名称
        exec_date: 执行日期
        is_manual_dag_trigger (bool): 是否手动DAG触发
        **kwargs: 其他可能的参数
    
    返回:
        bool: 执行成功返回True，否则返回False
    """
    logger.info(f"开始执行脚本...")
    
    # 获取当前脚本的文件名（如果没有传入）
    if script_name is None:
        script_name = os.path.basename(__file__)
    
    # 打印所有传入的参数
    logger.info(f"===== 传入参数信息 =====")
    logger.info(f"table_name: {table_name}")
    logger.info(f"update_mode: {update_mode}")
    logger.info(f"schedule_frequency: {schedule_frequency}")
    logger.info(f"exec_date: {exec_date}")
    logger.info(f"script_name: {script_name}")
    logger.info(f"is_manual_dag_trigger: {is_manual_dag_trigger}")
    
    # 打印所有可能的额外参数
    for key, value in kwargs.items():
        logger.info(f"额外参数 - {key}: {value}")
    logger.info(f"========================")
    
    # 实际调用内部处理函数
    return load_data_from_source(
        table_name=table_name, 
        exec_date=exec_date, 
        update_mode=update_mode, 
        script_name=script_name,
        schedule_frequency=schedule_frequency,
        is_manual_dag_trigger=is_manual_dag_trigger,
        **kwargs
    )

if __name__ == "__main__":
    # 直接执行时调用统一入口函数，传入测试参数
    run(
        table_name="test_table", 
        update_mode="append", 
        schedule_frequency="daily",
        exec_date=datetime.now().strftime('%Y-%m-%d'),
        script_name=os.path.basename(__file__),
        is_manual_dag_trigger=True
    )