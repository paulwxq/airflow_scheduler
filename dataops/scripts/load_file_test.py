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

logger = logging.getLogger("load_file_mock") # 使用 mock 后缀以区分

def mock_load_file(table_name=None, execution_mode='append', exec_date=None, 
                   target_type=None, storage_location=None, frequency=None, script_name=None, **kwargs):
    """模拟加载文件数据，仅打印参数"""
    # 获取当前脚本的文件名（如果没有传入）
    if script_name is None:
        script_name = os.path.basename(__file__)

    # 打印所有传入的参数
    logger.info(f"===== 传入参数信息 (模拟处理函数内) =====")
    logger.info(f"table_name: {table_name}")
    logger.info(f"execution_mode: {execution_mode}")
    logger.info(f"exec_date: {exec_date}")
    logger.info(f"target_type: {target_type}")
    logger.info(f"storage_location: {storage_location}")
    logger.info(f"frequency: {frequency}")
    logger.info(f"script_name: {script_name}")
    # 打印所有可能的额外参数
    for key, value in kwargs.items():
        logger.info(f"额外参数 - {key}: {value}")
    logger.info(f"=========================================")

    logger.info(f"开始模拟文件加载 - 脚本: {script_name}, 表: {table_name}")

    try:
        logger.info("模拟检查参数...")
        if not storage_location:
            logger.warning("警告: 未提供 storage_location (文件路径)")
        else:
            logger.info(f"模拟检查文件是否存在: {storage_location}")

        logger.info(f"模拟执行模式: {execution_mode}")
        if execution_mode == 'full_refresh':
            logger.info(f"模拟: 如果是全量刷新，将清空表 {table_name}")
        
        logger.info("模拟读取和处理文件...")
        # 模拟成功
        logger.info(f"模拟: 表 {table_name} 文件加载成功")
        return True
    except Exception as e:
        logger.error(f"模拟加载文件时出错: {str(e)}")
        return False

def run(table_name, execution_mode='append', exec_date=None, target_type=None, 
        storage_location=None, frequency=None, script_name=None, **kwargs):
    """
    统一入口函数，符合Airflow动态脚本调用规范 (模拟版本)

    参数:
        table_name (str): 要处理的表名
        execution_mode (str): 执行模式 (append/full_refresh)
        exec_date: 执行日期
        target_type: 目标类型
        storage_location: 文件路径
        frequency: 更新频率
        script_name: 脚本名称
        **kwargs: 其他可能的参数

    返回:
        bool: 执行成功返回True，否则返回False
    """
    # 打印所有传入的参数 (在入口函数再次打印，确保信息完整)
    logger.info(f"===== 传入参数信息 (入口函数 run 内) =====")
    logger.info(f"table_name: {table_name}")
    logger.info(f"execution_mode: {execution_mode}")
    logger.info(f"exec_date: {exec_date} (类型: {type(exec_date)}) ")
    logger.info(f"target_type: {target_type}")
    logger.info(f"storage_location: {storage_location}")
    logger.info(f"frequency: {frequency}")
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
    logger.info(f"脚本 '{script_name}' (模拟) 开始执行: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

    # 调用实际处理函数 (模拟版本)
    result = mock_load_file(
        table_name=table_name,
        execution_mode=execution_mode,
        exec_date=exec_date,
        target_type=target_type,
        storage_location=storage_location,
        frequency=frequency,
        script_name=script_name,
        **kwargs  # 将额外参数传递给处理函数
    )

    end_time = datetime.now()
    logger.info(f"脚本 '{script_name}' (模拟) 结束执行: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"总耗时: {end_time - start_time}")
    logger.info(f"处理结果: {'成功' if result else '失败'}")

    return result

if __name__ == "__main__":
    # 提供一些默认值以便直接运行脚本进行测试
    test_params = {
        "table_name": "sample_table",
        "execution_mode": "full_refresh",
        "exec_date": datetime.now().strftime('%Y-%m-%d'),
        "target_type": "structure",
        "storage_location": "/path/to/mock/file.csv",
        "frequency": "daily",
        "script_name": os.path.basename(__file__),
        "custom_param": "abc",
        "another_param": 456
    }
    logger.info(f"以主脚本方式运行 (模拟)，使用测试参数: {test_params}")
    run(**test_params) 