from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta
from utils import (
    get_enabled_tables, is_data_model_table, run_model_script, get_model_dependency_graph, 
    check_table_relationship, process_model_tables
)
from config import NEO4J_CONFIG, TASK_RETRY_CONFIG
import pendulum
import logging
import networkx as nx

# 创建日志记录器
logger = logging.getLogger(__name__)

def generate_optimized_execution_order(table_names: list) -> list:
    """
    生成优化的执行顺序，可处理循环依赖    
    参数:
        table_names: 表名列表    
    返回:
        list: 优化后的执行顺序列表
    """
    # 创建依赖图
    G = nx.DiGraph()
    
    # 添加所有节点
    for table_name in table_names:
        G.add_node(table_name)
    
    # 添加依赖边
    dependency_dict = get_model_dependency_graph(table_names)
    for target, upstreams in dependency_dict.items():
        for upstream in upstreams:
            if upstream in table_names:  # 确保只考虑目标表集合中的表
                G.add_edge(upstream, target)
    
    # 检测循环依赖
    cycles = list(nx.simple_cycles(G))
    if cycles:
        logger.warning(f"检测到循环依赖，将尝试打破循环: {cycles}")
        # 打破循环依赖（简单策略：移除每个循环中的一条边）
        for cycle in cycles:
            # 移除循环中的最后一条边
            G.remove_edge(cycle[-1], cycle[0])
            logger.info(f"打破循环依赖: 移除 {cycle[-1]} -> {cycle[0]} 的依赖")
    
    # 生成拓扑排序
    try:
        execution_order = list(nx.topological_sort(G))
        return execution_order
    except Exception as e:
        logger.error(f"生成执行顺序失败: {str(e)}")
        # 返回原始列表作为备选
        return table_names

def is_first_day():
    return True
    # 生产环境中应使用实际判断
    # return pendulum.now().day == 1

with DAG("dag_data_model_monthly", start_date=datetime(2024, 1, 1), schedule_interval="@daily", catchup=False) as dag:
    logger.info("初始化 dag_data_model_monthly DAG")
    
    # 修改依赖关系：直接依赖于daily.py而不是weekly.py
    wait_for_daily = ExternalTaskSensor(
        task_id="wait_for_daily_model",
        external_dag_id="dag_data_model_daily",
        external_task_id="daily_processing_completed",  # 指定完成标记任务
        mode="poke",
        timeout=3600,
        poke_interval=30
    )
    logger.info("创建日模型等待任务 - wait_for_daily_model")
    
    # 创建一个完成标记任务，确保即使没有处理任务也能标记DAG完成
    monthly_completed = EmptyOperator(
        task_id="monthly_processing_completed",
        dag=dag
    )
    logger.info("创建任务完成标记 - monthly_processing_completed")
    
    # 检查今天是否是月初
    if is_first_day():
        logger.info("今天是月初，开始处理月模型")
        # 获取启用的 monthly 模型表
        try:
            enabled_tables = get_enabled_tables("monthly")
            
            # 特别检查两个表之间的关系（这是monthly.py特有的）
            table_names = [t['table_name'] for t in enabled_tables if is_data_model_table(t['table_name'])]
            if 'book_sale_amt_yearly' in table_names and 'book_sale_amt_monthly' in table_names:
                logger.info("特别检查 book_sale_amt_yearly 和 book_sale_amt_monthly 之间的关系")
                relationship = check_table_relationship('book_sale_amt_yearly', 'book_sale_amt_monthly')
                logger.info(f"关系检查结果: {relationship}")
            
            # 定义monthly特有的任务选项
            task_options = {
                'default': {
                    'retries': TASK_RETRY_CONFIG["retries"],
                    'retry_delay': timedelta(minutes=TASK_RETRY_CONFIG["retry_delay_minutes"])
                },
                'book_sale_amt_monthly': {
                    'trigger_rule': "none_failed",
                    'retries': TASK_RETRY_CONFIG["retries"],
                    'retry_delay': timedelta(minutes=TASK_RETRY_CONFIG["retry_delay_minutes"])
                }
            }
            
            # 使用公共函数处理模型表 - 修改依赖任务名称
            process_model_tables(enabled_tables, "monthly", wait_for_daily, monthly_completed, dag, **task_options)
            
        except Exception as e:
            logger.error(f"获取 monthly 模型表时出错: {str(e)}")
            # 出错时也要确保完成标记被触发 - 修改依赖任务名称
            wait_for_daily >> monthly_completed
            raise
    else:
        # 如果不是月初，直接将等待任务与完成标记相连接，跳过处理 - 修改依赖任务名称
        logger.info("今天不是月初，跳过月模型处理")
        wait_for_daily >> monthly_completed