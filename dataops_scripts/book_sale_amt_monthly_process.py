#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import sys
import os
from datetime import datetime, timedelta

# 配置日志记录器
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger("book_sale_amt_monthly_process")

def process_monthly_book_sales():
    """处理月度图书销售额数据的函数"""
    # 获取当前脚本的文件名
    script_name = os.path.basename(__file__)
    
    logger.info(f"开始执行月度图书销售额处理 - 脚本: {script_name}")
    
    try:
        # 模拟数据处理过程
        logger.info("从数据源获取原始销售数据...")
        # 实际应用中这里会连接到数据库或其他数据源
        
        logger.info("按月汇总销售额...")
        # 模拟处理步骤
        current_month = datetime.now().strftime("%Y-%m")
        logger.info(f"正在处理 {current_month} 的数据")
        
        logger.info("数据处理完成，准备保存结果...")
        # 实际应用中这里会将结果保存到数据库
        
        return True
    except Exception as e:
        logger.error(f"处理月度图书销售额时出错: {str(e)}")
        return False

def run(table_name, update_mode='append', **kwargs):
    """
    统一入口函数，符合Airflow动态脚本调用规范
    
    参数:
        table_name (str): 要处理的表名
        update_mode (str): 更新模式 (append/full_refresh)
        **kwargs: 其他可能的参数
    
    返回:
        bool: 执行成功返回True，否则返回False
    """
    logger.info(f"通过统一入口函数run()调用 - 处理表: {table_name}, 模式: {update_mode}")
    
    # 获取当前脚本的文件名
    script_name = os.path.basename(__file__)
    
    # 记录详细的执行信息
    logger.info(f"[统一入口] 脚本 {script_name} 正在处理表 {table_name}, 模式: {update_mode}")
    logger.info(f"开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # 根据执行模式判断处理逻辑
    if update_mode == "full_refresh":
        logger.info("执行完全刷新模式 - 将处理所有历史数据")
    else:  # append
        logger.info("执行增量模式 - 只处理最新数据")
    
    # 调用实际处理函数
    result = process_monthly_book_sales()
    
    logger.info(f"结束时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"处理结果: {'成功' if result else '失败'}")
    
    return result

if __name__ == "__main__":
    # 直接执行时调用统一入口函数
    run(table_name="book_sale_amt_monthly", update_mode="append") 