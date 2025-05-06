#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import sys
import os
from datetime import datetime

# 配置日志记录器
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger("load_data")

def load_data_from_source(source_name="default", execution_date=None, execution_mode=None, script_name=None):
    """从数据源加载数据的示例函数"""
    if execution_date is None:
        execution_date = datetime.now()
    
    # 获取当前脚本的文件名（如果没有传入）
    if script_name is None:
        script_name = os.path.basename(__file__)
    
    
    # 使用logger.info输出所有参数
    logger.info(f"===== 参数信息 (logger输出) =====")
    logger.info(f"table_name: {source_name}")
    logger.info(f"exec_date: {execution_date}")
    logger.info(f"execution_mode: {execution_mode}")
    logger.info(f"script_name: {script_name}")
    logger.info(f"================================")
    
    return True

def run(table_name, execution_mode, exec_date=None, script_name=None, **kwargs):
    """
    统一入口函数，符合Airflow动态脚本调用规范
    
    参数:
        table_name (str): 要处理的表名
        execution_mode (str): 执行模式 (append/full_refresh)
        exec_date: 执行日期
        script_name: 脚本名称
        **kwargs: 其他可能的参数
    
    返回:
        bool: 执行成功返回True，否则返回False
    """
    logger.info(f"开始执行脚本...")
    
    # 打印所有传入的参数
    logger.info(f"===== 传入参数信息 =====")
    logger.info(f"table_name: {table_name}")
    logger.info(f"execution_mode: {execution_mode}")
    logger.info(f"exec_date: {exec_date}")
    logger.info(f"script_name: {script_name}")
    
    # 打印所有可能的额外参数
    for key, value in kwargs.items():
        logger.info(f"额外参数 - {key}: {value}")
    logger.info(f"========================")
    
    # 实际调用内部处理函数
    return load_data_from_source(
        source_name=table_name, 
        execution_date=exec_date, 
        execution_mode=execution_mode, 
        script_name=script_name
    )

if __name__ == "__main__":
    # 直接执行时调用统一入口函数，传入测试参数
    run(
        table_name="test_table", 
        execution_mode="append", 
        exec_date=datetime.now(),
        script_name=os.path.basename(__file__)
    )
