#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
import sys
import os
import pandas as pd
import psycopg2
from datetime import datetime
import csv
from dags.config import PG_CONFIG

# 配置日志记录器
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger("load_file")

def get_pg_conn():
    """获取PostgreSQL连接"""
    return psycopg2.connect(**PG_CONFIG)

def get_table_columns(table_name):
    """
    获取表的列信息，包括列名和注释
    
    返回:
        dict: {列名: 列注释} 的字典
    """
    conn = get_pg_conn()
    cursor = conn.cursor()
    try:
        # 查询表列信息
        cursor.execute("""
            SELECT 
                column_name, 
                col_description((table_schema || '.' || table_name)::regclass::oid, ordinal_position) as column_comment
            FROM 
                information_schema.columns
            WHERE 
                table_name = %s
            ORDER BY 
                ordinal_position
        """, (table_name,))
        
        columns = {}
        for row in cursor.fetchall():
            col_name = row[0]
            col_comment = row[1] if row[1] else col_name  # 如果注释为空，使用列名
            columns[col_name] = col_comment
            
        return columns
    except Exception as e:
        logger.error(f"获取表 {table_name} 的列信息时出错: {str(e)}")
        return {}
    finally:
        cursor.close()
        conn.close()

def match_csv_columns(csv_headers, table_columns):
    """
    匹配CSV列名与表列名
    
    策略:
    1. 尝试通过表字段注释匹配CSV列名
    2. 尝试通过名称直接匹配
    
    参数:
        csv_headers (list): CSV文件的列名列表
        table_columns (dict): {列名: 列注释} 的字典
    
    返回:
        dict: {CSV列名: 表列名} 的映射字典
    """
    mapping = {}
    
    # 通过注释匹配
    comment_to_column = {comment: col for col, comment in table_columns.items()}
    for header in csv_headers:
        if header in comment_to_column:
            mapping[header] = comment_to_column[header]
            continue
        
        # 尝试直接名称匹配
        if header in table_columns:
            mapping[header] = header
    
    return mapping

def load_csv_to_table(csv_file, table_name, execution_mode='append'):
    """
    将CSV文件数据加载到目标表
    
    参数:
        csv_file (str): CSV文件路径
        table_name (str): 目标表名
        execution_mode (str): 执行模式，'append'或'full_refresh'
    
    返回:
        bool: 成功返回True，失败返回False
    """
    conn = None
    try:
        # 读取CSV文件，尝试自动检测编码
        try:
            df = pd.read_csv(csv_file, encoding='utf-8')
        except UnicodeDecodeError:
            try:
                df = pd.read_csv(csv_file, encoding='gbk')
            except UnicodeDecodeError:
                df = pd.read_csv(csv_file, encoding='latin1')
        
        logger.info(f"成功读取CSV文件: {csv_file}, 共 {len(df)} 行")
        
        # 获取CSV列名
        csv_headers = df.columns.tolist()
        logger.info(f"CSV列名: {csv_headers}")
        
        # 获取表结构
        table_columns = get_table_columns(table_name)
        if not table_columns:
            logger.error(f"无法获取表 {table_name} 的列信息")
            return False
        
        logger.info(f"表 {table_name} 的列信息: {table_columns}")
        
        # 匹配CSV列与表列
        column_mapping = match_csv_columns(csv_headers, table_columns)
        logger.info(f"列映射关系: {column_mapping}")
        
        if not column_mapping:
            logger.error(f"无法建立CSV列与表列的映射关系")
            return False
        
        # 筛选和重命名列
        df_mapped = df[list(column_mapping.keys())].rename(columns=column_mapping)
        
        # 连接数据库
        conn = get_pg_conn()
        cursor = conn.cursor()
        
        # 根据执行模式确定操作
        if execution_mode == 'full_refresh':
            # 如果是全量刷新，先清空表
            logger.info(f"执行全量刷新，清空表 {table_name}")
            cursor.execute(f"TRUNCATE TABLE {table_name}")
        
        # 构建INSERT语句
        columns = ', '.join(df_mapped.columns)
        placeholders = ', '.join(['%s'] * len(df_mapped.columns))
        insert_sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        
        # 批量插入数据
        rows = [tuple(row) for row in df_mapped.values]
        cursor.executemany(insert_sql, rows)
        
        # 提交事务
        conn.commit()
        logger.info(f"成功插入 {len(rows)} 行数据到表 {table_name}")
        
        return True
    except Exception as e:
        logger.error(f"加载CSV数据到表时出错: {str(e)}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            conn.close()

def run(table_name, execution_mode='append', exec_date=None, target_type=None, 
        storage_location=None, frequency=None, **kwargs):
    """
    统一入口函数，符合Airflow动态脚本调用规范
    
    参数:
        table_name (str): 要处理的表名
        execution_mode (str): 执行模式 (append/full_refresh)
        exec_date: 执行日期
        target_type: 目标类型，对于CSV文件应为'structure'
        storage_location: CSV文件路径
        frequency: 更新频率
        **kwargs: 其他可能的参数
    
    返回:
        bool: 执行成功返回True，否则返回False
    """
    logger.info(f"===== 开始执行CSV文件加载 =====")
    logger.info(f"表名: {table_name}")
    logger.info(f"执行模式: {execution_mode}")
    logger.info(f"执行日期: {exec_date}")
    logger.info(f"目标类型: {target_type}")
    logger.info(f"文件路径: {storage_location}")
    logger.info(f"更新频率: {frequency}")
    
    # 记录其他参数
    for key, value in kwargs.items():
        logger.info(f"其他参数 - {key}: {value}")
    
    # 检查必要参数
    if not storage_location:
        logger.error("未提供CSV文件路径")
        return False
    
    # 检查文件是否存在
    if not os.path.exists(storage_location):
        logger.error(f"CSV文件不存在: {storage_location}")
        return False
    
    # 记录执行开始时间
    start_time = datetime.now()
    
    try:
        # 加载CSV数据到表
        result = load_csv_to_table(storage_location, table_name, execution_mode)
        
        # 记录执行结束时间
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        if result:
            logger.info(f"CSV文件加载成功，耗时: {duration:.2f}秒")
        else:
            logger.error(f"CSV文件加载失败，耗时: {duration:.2f}秒")
        
        return result
    except Exception as e:
        # 记录执行结束时间
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        logger.error(f"CSV文件加载过程中出错: {str(e)}")
        logger.error(f"CSV文件加载失败，耗时: {duration:.2f}秒")
        
        return False
    finally:
        logger.info(f"===== CSV文件加载执行完成 =====")

if __name__ == "__main__":
    # 直接执行时的测试代码
    import argparse
    
    parser = argparse.ArgumentParser(description='从CSV文件加载数据到表')
    parser.add_argument('--table', type=str, required=True, help='目标表名')
    parser.add_argument('--file', type=str, required=True, help='CSV文件路径')
    parser.add_argument('--mode', type=str, default='append', help='执行模式: append或full_refresh')
    
    args = parser.parse_args()
    
    success = run(
        table_name=args.table,
        execution_mode=args.mode,
        storage_location=args.file,
        target_type='structure'
    )
    
    if success:
        print("CSV文件加载成功")
        sys.exit(0)
    else:
        print("CSV文件加载失败")
        sys.exit(1)