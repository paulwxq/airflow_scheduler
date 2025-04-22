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

logger = logging.getLogger("emp_training_stats_table")

def process_emp_training_stats(table_name=None, exec_date=None, execution_mode=None, script_name=None, **kwargs):
    """处理员工培训统计数据的模拟函数"""
    # 获取当前脚本的文件名（如果没有传入）
    if script_name is None:
        script_name = os.path.basename(__file__)

    # 打印所有传入的参数
    logger.info(f"===== 传入参数信息 (处理函数内) =====")
    logger.info(f"table_name: {table_name}")
    logger.info(f"exec_date: {exec_date}")
    logger.info(f"execution_mode: {execution_mode}")
    logger.info(f"script_name: {script_name}")
    # 打印所有可能的额外参数
    for key, value in kwargs.items():
        logger.info(f"额外参数 - {key}: {value}")
    logger.info(f"======================================")

    logger.info(f"开始执行员工培训统计数据处理 - 脚本: {script_name}, 表: {table_name}")

    try:
        # 模拟数据处理过程
        logger.info("模拟处理员工培训统计数据...")
        # 在实际应用中，这里可以添加具体的数据处理逻辑
        logger.info(f"处理日期: {exec_date}, 模式: {execution_mode}")
        
        # 模拟处理成功
        logger.info(f"表 {table_name} 数据处理成功")
        return True
    except Exception as e:
        logger.error(f"处理员工培训统计数据时出错: {str(e)}")
        return False

def run(table_name, execution_mode, exec_date=None, script_name=None, **kwargs):
    """
    统一入口函数，符合Airflow动态脚本调用规范

    参数:
        table_name (str): 要处理的表名
        execution_mode (str): 执行模式 (append/full_refresh)
        exec_date: 执行日期 (可以是字符串 YYYY-MM-DD 或 datetime 对象)
        script_name: 脚本名称
        **kwargs: 其他可能的参数

    返回:
        bool: 执行成功返回True，否则返回False
    """
    # 打印所有传入的参数 (在入口函数再次打印，确保信息完整)
    logger.info(f"===== 传入参数信息 (入口函数 run 内) =====")
    logger.info(f"table_name: {table_name}")
    logger.info(f"execution_mode: {execution_mode}")
    logger.info(f"exec_date: {exec_date} (类型: {type(exec_date)})")
    logger.info(f"script_name: {script_name}")
    # 打印所有可能的额外参数
    for key, value in kwargs.items():
        logger.info(f"额外参数 - {key}: {value}")
    logger.info(f"=========================================")

    # 如果没有提供脚本名，使用当前脚本的文件名
    if script_name is None:
        script_name = os.path.basename(__file__)

    # 记录详细的执行信息
    start_time = datetime.now()
    logger.info(f"脚本 '{script_name}' 开始执行: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

    # 调用实际处理函数
    result = process_emp_training_stats(
        table_name=table_name,
        exec_date=exec_date,
        execution_mode=execution_mode,
        script_name=script_name,
        **kwargs  # 将额外参数传递给处理函数
    )

    end_time = datetime.now()
    logger.info(f"脚本 '{script_name}' 结束执行: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"总耗时: {end_time - start_time}")
    logger.info(f"处理结果: {'成功' if result else '失败'}")

    return result

if __name__ == "__main__":
    # 提供一些默认值以便直接运行脚本进行测试
    test_params = {
        "table_name": "emp_training_stats",
        "execution_mode": "append",
        "exec_date": datetime.now().strftime('%Y-%m-%d'),
        "script_name": os.path.basename(__file__),
        "test_param1": "value1",
        "test_param2": 123
    }
    logger.info(f"以主脚本方式运行，使用测试参数: {test_params}")
    run(**test_params) 