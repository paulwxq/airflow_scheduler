#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import sys
import os

# 配置日志记录器
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger("book_total_process")

def process_book_data():
    """处理图书数据的示例函数"""
    # 获取当前脚本的文件名
    script_name = os.path.basename(__file__)
       
    # 使用logger.info输出脚本名称
    logger.info(f"当前脚本名称是 {script_name} - 来自logger.info输出")
    
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
    
    # 同时使用print和logger输出以便比较
    logger.info(f"[统一入口] 脚本 {script_name} 正在处理表 {table_name}, 模式: {execution_mode}")
    
    # 实际调用内部处理函数
    return process_book_data()

if __name__ == "__main__":
    # 直接执行时调用统一入口函数
    run(table_name="books", execution_mode="full_refresh")
