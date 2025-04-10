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

logger = logging.getLogger("book_sale_amt_2weekly_process")

def process_2weekly_book_sales():
    """处理双周图书销售额数据的函数"""
    # 获取当前脚本的文件名
    script_name = os.path.basename(__file__)
    
    logger.info(f"开始执行双周图书销售额处理 - 脚本: {script_name}")
    
    try:
        # 模拟数据处理过程
        logger.info("从数据源获取原始销售数据...")
        # 实际应用中这里会连接到数据库或其他数据源
        
        logger.info("按双周期汇总销售额...")
        # 模拟处理步骤
        today = datetime.now()
        # 计算当前所在的周数
        week_number = int(today.strftime("%U"))
        # 计算当前双周期
        biweekly_number = (week_number // 2) + 1
        # 计算双周期的开始和结束日期
        start_day = datetime.strptime(f"{today.year}-W{biweekly_number*2-1}-1", "%Y-W%W-%w")
        end_day = start_day + timedelta(days=13)  # 两周共14天
        date_range = f"{start_day.strftime('%Y-%m-%d')} 至 {end_day.strftime('%Y-%m-%d')}"
        
        logger.info(f"正在处理第 {biweekly_number} 个双周期 ({date_range}) 的数据")
        
        logger.info("汇总统计双周期内每天的销售数据...")
        logger.info("计算与上一个双周期的对比...")
        logger.info("计算热销书籍排行榜...")
        logger.info("生成双周期销售趋势分析...")
        
        logger.info("数据处理完成，准备保存结果...")
        # 实际应用中这里会将结果保存到数据库
        
        return True
    except Exception as e:
        logger.error(f"处理双周图书销售额时出错: {str(e)}")
        return False

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
    
    # 记录详细的执行信息
    logger.info(f"[统一入口] 脚本 {script_name} 正在处理表 {table_name}, 模式: {execution_mode}")
    logger.info(f"开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # 根据执行模式判断处理逻辑
    if execution_mode == "full_refresh":
        logger.info("执行完全刷新模式 - 将处理所有历史数据")
        logger.info("获取过去12个双周期的历史数据进行分析...")
    else:  # append
        logger.info("执行增量模式 - 只处理最新双周期数据")
    
    # 调用实际处理函数
    result = process_2weekly_book_sales()
    
    logger.info(f"结束时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"处理结果: {'成功' if result else '失败'}")
    
    return result

if __name__ == "__main__":
    # 直接执行时调用统一入口函数
    run(table_name="book_sale_amt_2weekly", execution_mode="append") 