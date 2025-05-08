#!/usr/bin/env python
# -*- coding: utf-8 -*-
# 这是dataops_scripts目录下的文件 - 用于验证路径修改成功
import logging
import sys
import os
import traceback

# 添加父目录到Python路径，以便能导入dags目录下的config模块
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

import importlib.util
from datetime import datetime, timedelta
import pytz
import re  # 添加re模块以支持正则表达式

# 导入Airflow相关包
try:
    from airflow.models import Variable
except ImportError:
    # 处理在非Airflow环境中运行的情况
    class Variable:
        @staticmethod
        def get(key, default_var=None):
            return default_var

# 配置日志记录器
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger("script_utils")

def get_config_path():
    """
    从Airflow变量中获取DATAOPS_DAGS_PATH
    
    返回:
        str: config.py的完整路径
    """
    try:
        # 从Airflow变量中获取DATAOPS_DAGS_PATH
        dags_path = Variable.get("DATAOPS_DAGS_PATH", "/opt/airflow/dags")
        logger.info(f"从Airflow变量获取到DATAOPS_DAGS_PATH: {dags_path}")
        
        # 构建config.py的完整路径
        config_path = os.path.join(dags_path, "config.py")
        
        if not os.path.exists(config_path):
            logger.warning(f"配置文件路径不存在: {config_path}, 将使用默认路径")
            # 尝试使用相对路径
            alt_config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../dags/config.py"))
            if os.path.exists(alt_config_path):
                logger.info(f"使用替代配置路径: {alt_config_path}")
                return alt_config_path
        
        return config_path
    except Exception as e:
        logger.error(f"获取配置路径时出错: {str(e)}")
        # 使用默认路径
        return os.path.abspath(os.path.join(os.path.dirname(__file__), "../dags/config.py"))

def load_config_module():
    """
    动态加载config.py模块
    
    返回:
        module: 加载的config模块
    """
    try:
        config_path = get_config_path()
        logger.info(f"正在加载配置文件: {config_path}")
        
        # 动态加载config.py模块
        spec = importlib.util.spec_from_file_location("config", config_path)
        config_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(config_module)
        
        return config_module
    except Exception as e:
        logger.error(f"加载配置模块时出错: {str(e)}")
        raise ImportError(f"无法加载配置模块: {str(e)}")
    
def get_neo4j_driver():
    """获取Neo4j连接驱动"""
    try:
        # 使用get_config_path获取config路径
        config_path = get_config_path()
        
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"配置文件不存在: {config_path}")
            
        logger.info(f"使用配置文件路径: {config_path}")
        
        # 动态加载config模块
        spec = importlib.util.spec_from_file_location("config", config_path)
        config_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(config_module)
        
        # 从模块中获取NEO4J_CONFIG
        NEO4J_CONFIG = getattr(config_module, "NEO4J_CONFIG", None)
        
        if not NEO4J_CONFIG:
            raise ValueError(f"配置文件 {config_path} 中未找到NEO4J_CONFIG配置项")
            
        # 验证NEO4J_CONFIG中包含必要的配置项
        required_keys = ["uri", "user", "password"]
        missing_keys = [key for key in required_keys if key not in NEO4J_CONFIG]
        
        if missing_keys:
            raise ValueError(f"NEO4J_CONFIG缺少必要的配置项: {', '.join(missing_keys)}")
        
        # 创建Neo4j驱动
        from neo4j import GraphDatabase
        logger.info(f"使用配置创建Neo4j驱动: {NEO4J_CONFIG['uri']}")
        return GraphDatabase.driver(
            NEO4J_CONFIG['uri'], 
            auth=(NEO4J_CONFIG['user'], NEO4J_CONFIG['password'])
        )
    except Exception as e:
        logger.error(f"创建Neo4j驱动失败: {str(e)}")
        logger.error(traceback.format_exc())
        raise

def get_pg_config():
    """
    从config.py获取PostgreSQL数据库配置
    
    返回:
        dict: PostgreSQL配置字典
    """
    try:
        config_module = load_config_module()
        pg_config = getattr(config_module, "PG_CONFIG", None)
        
        if pg_config is None:
            logger.warning("配置模块中未找到PG_CONFIG")
            # 返回默认配置
            return {
                "host": "localhost",
                "port": 5432,
                "user": "postgres",
                "password": "postgres",
                "database": "dataops"
            }
        
        logger.info(f"已获取PostgreSQL配置: {pg_config}")
        return pg_config
    except Exception as e:
        logger.error(f"获取PostgreSQL配置时出错: {str(e)}")
        # 返回默认配置
        return {
            "host": "localhost",
            "port": 5432,
            "user": "postgres",
            "password": "postgres",
            "database": "dataops"
        }

def get_upload_paths():
    """
    从config.py获取文件上传和归档路径
    
    返回:
        tuple: (上传路径, 归档路径)
    """
    try:
        config_module = load_config_module()
        upload_path = getattr(config_module, "STRUCTURE_UPLOAD_BASE_PATH", "/data/csv")
        archive_path = getattr(config_module, "STRUCTURE_UPLOAD_ARCHIVE_BASE_PATH", "/data/archive")
        
        logger.info(f"已获取上传路径: {upload_path}, 归档路径: {archive_path}")
        return upload_path, archive_path
    except Exception as e:
        logger.error(f"获取上传路径时出错: {str(e)}")
        # 返回默认路径
        return "/data/csv", "/data/archive"

def get_date_range(exec_date, frequency):
    """
    根据执行日期和频率，计算开始日期和结束日期
    
    参数:
        exec_date (str): 执行日期，格式为 YYYY-MM-DD
        frequency (str): 频率，可选值为 daily, weekly, monthly, quarterly, yearly
    
    返回:
        tuple: (start_date, end_date) 格式为 YYYY-MM-DD 的字符串
    """
    logger.info(f"计算日期范围 - 执行日期: {exec_date}, 频率: {frequency}")
    
    # 将输入的日期转换为上海时区的datetime对象
    shanghai_tz = pytz.timezone('Asia/Shanghai')
    
    try:
        # 解析输入的exec_date
        if isinstance(exec_date, str):
            date_obj = datetime.strptime(exec_date, '%Y-%m-%d')
        elif isinstance(exec_date, datetime):
            date_obj = exec_date
        else:
            raise ValueError(f"不支持的exec_date类型: {type(exec_date)}")
        
        # 转换为上海时区
        date_obj = shanghai_tz.localize(date_obj)
        logger.info(f"上海时区的执行日期: {date_obj}")
        
        # 根据不同频率计算日期范围
        if frequency.lower() == 'daily':
            # 每日: start_date = exec_date, end_date = exec_date + 1 day
            start_date = date_obj.strftime('%Y-%m-%d')
            end_date = (date_obj + timedelta(days=1)).strftime('%Y-%m-%d')
            
        elif frequency.lower() == 'weekly':
            # 每周: start_date = 本周一, end_date = 下周一
            days_since_monday = date_obj.weekday()  # 0=周一, 6=周日
            monday = date_obj - timedelta(days=days_since_monday)
            next_monday = monday + timedelta(days=7)
            
            start_date = monday.strftime('%Y-%m-%d')
            end_date = next_monday.strftime('%Y-%m-%d')
            
        elif frequency.lower() == 'monthly':
            # 每月: start_date = 本月第一天, end_date = 下月第一天
            first_day = date_obj.replace(day=1)
            
            # 计算下个月的第一天
            if first_day.month == 12:
                next_month_first_day = first_day.replace(year=first_day.year + 1, month=1)
            else:
                next_month_first_day = first_day.replace(month=first_day.month + 1)
                
            start_date = first_day.strftime('%Y-%m-%d')
            end_date = next_month_first_day.strftime('%Y-%m-%d')
            
        elif frequency.lower() == 'quarterly':
            # 每季度: start_date = 本季度第一天, end_date = 下季度第一天
            quarter = (date_obj.month - 1) // 3 + 1  # 1-4季度
            first_month_of_quarter = (quarter - 1) * 3 + 1  # 季度的第一个月
            
            quarter_first_day = date_obj.replace(month=first_month_of_quarter, day=1)
            
            # 计算下个季度的第一天
            if quarter == 4:
                next_quarter_first_day = quarter_first_day.replace(year=quarter_first_day.year + 1, month=1)
            else:
                next_quarter_first_day = quarter_first_day.replace(month=first_month_of_quarter + 3)
                
            start_date = quarter_first_day.strftime('%Y-%m-%d')
            end_date = next_quarter_first_day.strftime('%Y-%m-%d')
            
        elif frequency.lower() == 'yearly':
            # 每年: start_date = 本年第一天, end_date = 下年第一天
            year_first_day = date_obj.replace(month=1, day=1)
            next_year_first_day = date_obj.replace(year=date_obj.year + 1, month=1, day=1)
            
            start_date = year_first_day.strftime('%Y-%m-%d')
            end_date = next_year_first_day.strftime('%Y-%m-%d')
            
        else:
            logger.error(f"不支持的频率: {frequency}")
            raise ValueError(f"不支持的频率: {frequency}")
        
        logger.info(f"计算结果 - 开始日期: {start_date}, 结束日期: {end_date}")
        return start_date, end_date
        
    except Exception as e:
        logger.error(f"计算日期范围时出错: {str(e)}", exc_info=True)
        raise 


import re
from typing import Dict, List, Optional, Set

def extract_source_fields_linked_to_template(sql: str, jinja_vars: List[str]) -> Set[str]:
    """
    从 SQL 中提取和 jinja 模板变量绑定的源字段（支持各种形式）
    """
    fields = set()
    sql = re.sub(r"\s+", " ", sql)

    for var in jinja_vars:
        # 普通比较、函数包裹
        pattern = re.compile(
            r"""
            (?P<field>
                (?:\w+\s*\(\s*)?           # 可选函数开始（如 DATE(
                [\w\.]+                    # 字段名
                (?:\s+AS\s+\w+)?           # 可选 CAST 形式
                \)?                        # 可选右括号
            )
            \s*(=|<|>|<=|>=)\s*['"]?\{\{\s*""" + var + r"""\s*\}\}['"]?
            """, re.IGNORECASE | re.VERBOSE
        )
        fields.update(match.group("field").strip() for match in pattern.finditer(sql))

        # BETWEEN '{{ start_date }}' AND '{{ end_date }}'
        if var == "start_date":
            pattern_between = re.compile(
                r"""(?P<field>
                        (?:\w+\s*\(\s*)?[\w\.]+(?:\s+AS\s+\w+)?\)?  # 字段（函数包裹可选）
                    )
                    \s+BETWEEN\s+['"]?\{\{\s*start_date\s*\}\}['"]?\s+AND\s+['"]?\{\{\s*end_date\s*\}\}
                """, re.IGNORECASE | re.VERBOSE
            )
            fields.update(match.group("field").strip() for match in pattern_between.finditer(sql))

    return {extract_core_field(f) for f in fields}

def extract_core_field(expr: str) -> str:
    """
    清洗函数包裹的字段表达式：DATE(sd.sale_date) -> sd.sale_date, CAST(...) -> ...
    """
    expr = re.sub(r"CAST\s*\(\s*([\w\.]+)\s+AS\s+\w+\s*\)", r"\1", expr, flags=re.IGNORECASE)
    expr = re.sub(r"\b\w+\s*\(\s*([\w\.]+)\s*\)", r"\1", expr)
    return expr.strip()

def parse_select_aliases(sql: str) -> Dict[str, str]:
    """
    提取 SELECT 中的字段别名映射：原字段 -> 目标别名
    """
    sql = re.sub(r"\s+", " ", sql)
    select_clause_match = re.search(r"SELECT\s+(.*?)\s+FROM", sql, re.IGNORECASE)
    if not select_clause_match:
        return {}

    select_clause = select_clause_match.group(1)
    mappings = {}
    for expr in select_clause.split(","):
        expr = expr.strip()
        alias_match = re.match(r"([\w\.]+)\s+AS\s+([\w]+)", expr, re.IGNORECASE)
        if alias_match:
            source, alias = alias_match.groups()
            mappings[source.strip()] = alias.strip()

    return mappings

def find_target_date_field(sql: str, jinja_vars: List[str] = ["start_date", "end_date"]) -> Optional[str]:
    """
    从 SQL 中找出与模板时间变量绑定的目标表字段（只返回一个）
    """
    source_fields = extract_source_fields_linked_to_template(sql, jinja_vars)
    alias_map = parse_select_aliases(sql)

    # 匹配 SELECT 中的映射字段
    for src_field in source_fields:
        if src_field in alias_map:
            return alias_map[src_field]  # 源字段映射的目标字段

    # 若未通过 AS 映射，可能直接 SELECT sd.sale_date（裸字段）
    for src_field in source_fields:
        if '.' not in src_field:
            return src_field  # 裸字段直接作为目标字段名

    return None


def generate_delete_sql(sql_content, target_table=None):
    """
    根据SQL脚本内容生成用于清理数据的DELETE语句
    
    参数:
        sql_content (str): 原始SQL脚本内容
        target_table (str, optional): 目标表名，如果SQL脚本中无法解析出表名时使用
    
    返回:
        str: DELETE语句，用于清理数据
    """
    logger.info("生成清理SQL语句，实现ETL作业幂等性")
    
    # 如果提供了目标表名，直接使用
    if target_table:
        logger.info(f"使用提供的目标表名: {target_table}")
        delete_stmt = f"""DELETE FROM {target_table}
WHERE summary_date >= '{{{{ start_date }}}}'
  AND summary_date < '{{{{ end_date }}}}';"""
        logger.info(f"生成的清理SQL: {delete_stmt}")
        return delete_stmt
    
    # 尝试从SQL内容中解析出目标表名
    try:
        # 简单解析，尝试找出INSERT语句的目标表
        # 匹配 INSERT INTO xxx 或 INSERT INTO "xxx" 或 INSERT INTO `xxx` 或 INSERT INTO [xxx]
        insert_match = re.search(r'INSERT\s+INTO\s+(?:["\[`])?([a-zA-Z0-9_\.]+)(?:["\]`])?', sql_content, re.IGNORECASE)
        
        if insert_match:
            table_name = insert_match.group(1)
            logger.info(f"从SQL中解析出目标表名: {table_name}")
            
            delete_stmt = f"""DELETE FROM {table_name}
WHERE summary_date >= '{{{{ start_date }}}}'
  AND summary_date < '{{{{ end_date }}}}';"""
            logger.info(f"生成的清理SQL: {delete_stmt}")
            return delete_stmt
        else:
            logger.warning("无法从SQL中解析出目标表名，无法生成清理SQL")
            return None
            
    except Exception as e:
        logger.error(f"解析SQL生成清理语句时出错: {str(e)}", exc_info=True)
        return None

def get_one_day_range(exec_date):
    """
    根据exec_date返回当天的00:00:00和次日00:00:00，均为datetime对象
    参数：
        exec_date (str 或 datetime): 执行日期，格式为YYYY-MM-DD或datetime对象
    返回：
        tuple(datetime, datetime): (start_datetime, end_datetime)
    """
    shanghai_tz = pytz.timezone('Asia/Shanghai')
    if isinstance(exec_date, str):
        date_obj = datetime.strptime(exec_date, '%Y-%m-%d')
    elif isinstance(exec_date, datetime):
        date_obj = exec_date
    else:
        raise ValueError(f"不支持的exec_date类型: {type(exec_date)}")
    # 当天00:00:00
    start_datetime = shanghai_tz.localize(datetime(date_obj.year, date_obj.month, date_obj.day, 0, 0, 0))
    # 次日00:00:00
    end_datetime = start_datetime + timedelta(days=1)
    return start_datetime, end_datetime

def get_target_dt_column(table_name, script_name=None):
    """
    从Neo4j或data_transform_scripts表获取目标日期列
    
    参数:
        table_name (str): 表名
        script_name (str, optional): 脚本名称
        
    返回:
        str: 目标日期列名
    """
    logger.info(f"获取表 {table_name} 的目标日期列")
    
    try:
        # 首先从Neo4j获取
        driver = get_neo4j_driver()
        with driver.session() as session:
            # 尝试从DataModel节点的relations关系属性中获取
            query = """
                MATCH (n {en_name: $table_name})
                RETURN n.target_dt_column AS target_dt_column
            """
            
            result = session.run(query, table_name=table_name)
            record = result.single()
            
            if record and record.get("target_dt_column"):
                target_dt_column = record.get("target_dt_column")
                logger.info(f"从Neo4j获取到表 {table_name} 的目标日期列: {target_dt_column}")
                return target_dt_column
        
        # 导入需要的模块以连接数据库
        import sqlalchemy
        from sqlalchemy import create_engine, text
        
        # Neo4j中找不到，尝试从data_transform_scripts表获取
        # 获取目标数据库连接
        pg_config = get_pg_config()
        
        if not pg_config:
            logger.error("无法获取PG_CONFIG配置，无法连接数据库查询目标日期列")
            return None
            
        # 创建数据库引擎
        db_url = f"postgresql://{pg_config['user']}:{pg_config['password']}@{pg_config['host']}:{pg_config['port']}/{pg_config['database']}"
        engine = create_engine(db_url)
        
        if not engine:
            logger.error("无法创建数据库引擎，无法获取目标日期列")
            return None
        
        # 查询data_transform_scripts表
        try:
            query = f"""
                SELECT target_dt_column
                FROM data_transform_scripts
                WHERE target_table = '{table_name}'
            """
            if script_name:
                query += f" AND script_name = '{script_name}'"
            
            query += " LIMIT 1"
            
            with engine.connect() as conn:
                result = conn.execute(text(query))
                row = result.fetchone()
                
                if row and row[0]:
                    target_dt_column = row[0]
                    logger.info(f"从data_transform_scripts表获取到表 {table_name} 的目标日期列: {target_dt_column}")
                    return target_dt_column
        except Exception as db_err:
            logger.error(f"从data_transform_scripts表获取目标日期列时出错: {str(db_err)}")
            logger.error(traceback.format_exc())
        
        # 都找不到，使用默认值
        logger.warning(f"未找到表 {table_name} 的目标日期列，将使用默认值 'data_date'")
        return "data_date"
    except Exception as e:
        logger.error(f"获取目标日期列时出错: {str(e)}")
        logger.error(traceback.format_exc())
        return None
    finally:
        if 'driver' in locals() and driver:
            driver.close()