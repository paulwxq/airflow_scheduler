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

def load_data_from_source(source_name="default", execution_date=None):
    """从数据源加载数据的示例函数"""
    if execution_date is None:
        execution_date = datetime.now()
    
    # 获取当前脚本的文件名
    script_name = os.path.basename(__file__)
    
    # 使用print输出脚本名称
    print(f"当前脚本名称是 {script_name} - 来自print输出 - 正在处理{source_name}数据")
    
    # 使用logger.info输出脚本名称
    logger.info(f"当前脚本名称是 {script_name} - 来自logger.info输出 - 执行日期: {execution_date}")
    
    return True

def run(table_name, execution_mode, **kwargs):
    """
    统一入口函数，符合Airflow动态脚本调用规范
    
    参数:
        table_name (str): 要处理的表名
        execution_mode (str): 执行模式 (append/full_refresh)
        **kwargs: 其他可能的参数
    
    返回:
        bool: 执行成功返回True，否则返回False
    """
    logger.info(f"通过统一入口函数run()调用 - 处理表: {table_name}, 模式: {execution_mode}")
    
    # 获取当前脚本的文件名
    script_name = os.path.basename(__file__)
    logger.info(f"[统一入口] 脚本 {script_name} 正在处理表 {table_name}, 模式: {execution_mode}")
    
    # 实际调用内部处理函数
    return load_data_from_source(source_name=table_name)

if __name__ == "__main__":
    # 直接执行时调用统一入口函数
    run(table_name="test_table", execution_mode="append")
