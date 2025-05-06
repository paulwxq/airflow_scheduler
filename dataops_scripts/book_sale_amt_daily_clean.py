#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import sys
import os
from datetime import datetime, timedelta
import time
import random

# 配置日志记录器
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger("book_sale_amt_daily_clean")

def clean_daily_book_sales(table_name=None, exec_date=None, execution_mode=None, script_name=None):
    """清洗日度图书销售额数据的函数"""
    # 获取当前脚本的文件名（如果没有传入）
    if script_name is None:
        script_name = os.path.basename(__file__)
    
    # 打印所有传入的参数
    logger.info(f"===== 传入参数信息 (处理函数内) =====")
    logger.info(f"table_name: {table_name}")
    logger.info(f"exec_date: {exec_date}")
    logger.info(f"execution_mode: {execution_mode}")
    logger.info(f"script_name: {script_name}")
    logger.info(f"======================================")
    
    logger.info(f"开始执行日度图书销售额数据清洗 - 脚本: {script_name}")
    
    try:
        # 模拟数据处理过程
        logger.info("从数据源获取原始销售数据...")
        # 实际应用中这里会连接到数据库或其他数据源
        
        logger.info("执行数据清洗流程...")
        
        # 尝试使用传入的日期，如果没有则使用昨天
        if exec_date:
            if isinstance(exec_date, str):
                try:
                    date_obj = datetime.strptime(exec_date, '%Y-%m-%d')
                    date_str = exec_date
                except ValueError:
                    today = datetime.now()
                    yesterday = today - timedelta(days=1)
                    date_str = yesterday.strftime('%Y-%m-%d')
                    logger.warning(f"无法解析传入的exec_date: {exec_date}，使用昨天日期: {date_str}")
            else:
                try:
                    date_str = exec_date.strftime('%Y-%m-%d')
                except:
                    today = datetime.now()
                    yesterday = today - timedelta(days=1)
                    date_str = yesterday.strftime('%Y-%m-%d')
                    logger.warning(f"无法格式化传入的exec_date，使用昨天日期: {date_str}")
        else:
            today = datetime.now()
            yesterday = today - timedelta(days=1)
            date_str = yesterday.strftime('%Y-%m-%d')
            logger.info(f"未传入exec_date，使用昨天日期: {date_str}")
        
        logger.info(f"正在清洗 {date_str} 的数据")
        
        logger.info("检查数据完整性...")
        logger.info("检测并处理异常值...")
        logger.info("填充缺失数据...")
        logger.info("标准化数据格式...")
        logger.info("去除重复记录...")
        
        logger.info("数据清洗完成，准备保存结果...")
        # 实际应用中这里会将结果保存到数据库
        
        # 模拟处理时间
        processing_time = random.uniform(0.5, 2.0)
        logger.info(f"开始处理数据，预计需要 {processing_time:.2f} 秒")
        time.sleep(processing_time)
        
        # 模拟处理逻辑
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(f"数据处理中... 当前时间: {current_time}")
        
        # 模拟数据清洗操作
        logger.info(f"执行数据清洗操作: 移除异常值、填充缺失值、标准化格式")
        time.sleep(processing_time)
        
        # 模拟写入数据库
        success_rate = random.random()
        if success_rate > 0.1:  # 90%的成功率
            logger.info(f"表 {table_name} 数据清洗成功，处理日期: {date_str}")
            return True
        else:
            logger.error(f"表 {table_name} 数据清洗或写入过程中出现随机错误")
            return False
    except Exception as e:
        logger.error(f"清洗日度图书销售额数据时出错: {str(e)}")
        return False

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
    # 打印所有传入的参数
    logger.info(f"===== 传入参数信息 (入口函数内) =====")
    logger.info(f"table_name: {table_name}")
    logger.info(f"execution_mode: {execution_mode}")
    logger.info(f"exec_date: {exec_date}")
    logger.info(f"script_name: {script_name}")
    
    # 打印所有可能的额外参数
    for key, value in kwargs.items():
        logger.info(f"额外参数 - {key}: {value}")
    logger.info(f"======================================")
    
    # 如果没有提供脚本名，使用当前脚本的文件名
    if script_name is None:
        script_name = os.path.basename(__file__)
    
    # 记录详细的执行信息
    logger.info(f"开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # 根据执行模式判断处理逻辑
    if execution_mode == "full_refresh":
        logger.info("执行完全刷新模式 - 将处理所有历史数据")
        logger.info("获取过去30天的历史数据进行清洗...")
    else:  # append
        logger.info("执行增量模式 - 只清洗最新一天的数据")
    
    # 调用实际处理函数
    result = clean_daily_book_sales(
        table_name=table_name, 
        exec_date=exec_date,
        execution_mode=execution_mode, 
        script_name=script_name
    )
    
    logger.info(f"结束时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"处理结果: {'成功' if result else '失败'}")
    
    return result

if __name__ == "__main__":
    # 直接执行时调用统一入口函数，带上所有参数作为测试
    run(
        table_name="book_sale_amt_daily", 
        execution_mode="append",
        exec_date=datetime.now().strftime('%Y-%m-%d'),
        script_name=os.path.basename(__file__)
    ) 