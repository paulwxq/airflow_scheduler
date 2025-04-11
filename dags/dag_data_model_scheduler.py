# dag_data_model_scheduler.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta
import pendulum
import logging
import networkx as nx
from utils import (
    get_enabled_tables,
    is_data_model_table,
    run_model_script,
    get_model_dependency_graph,
    check_script_exists,
    get_script_name_from_neo4j
)
from config import TASK_RETRY_CONFIG

# 创建日志记录器
logger = logging.getLogger(__name__)

def get_all_enabled_tables_for_today():
    """
    根据当前日期获取所有需要处理的表
    
    返回:
        list: 需要处理的表配置列表
    """
    today = pendulum.today()
    # 原始代码（注释）
    # is_monday = today.day_of_week == 0
    # is_first_day_of_month = today.day == 1
    # is_first_day_of_year = today.month == 1 and today.day == 1
    
    # 测试用：所有条件设为True
    is_monday = True
    is_first_day_of_month = True
    is_first_day_of_year = True
    
    logger.info(f"今日日期: {today.to_date_string()}")
    logger.info(f"日期特性: 是否周一={is_monday}, 是否月初={is_first_day_of_month}, 是否年初={is_first_day_of_year}")
    
    all_tables = []
    
    # 每天都处理daily表
    daily_tables = get_enabled_tables("daily")
    all_tables.extend(daily_tables)
    logger.info(f"添加daily表: {len(daily_tables)}个")
    
    # 周一处理weekly表
    if is_monday:
        weekly_tables = get_enabled_tables("weekly")
        all_tables.extend(weekly_tables)
        logger.info(f"今天是周一，添加weekly表: {len(weekly_tables)}个")
    
    # 月初处理monthly表
    if is_first_day_of_month:
        monthly_tables = get_enabled_tables("monthly")
        all_tables.extend(monthly_tables)
        logger.info(f"今天是月初，添加monthly表: {len(monthly_tables)}个")
    
    # 年初处理yearly表
    if is_first_day_of_year:
        yearly_tables = get_enabled_tables("yearly")
        all_tables.extend(yearly_tables)
        logger.info(f"今天是年初，添加yearly表: {len(yearly_tables)}个")
    
    # 去重
    unique_tables = {}
    for item in all_tables:
        table_name = item["table_name"]
        if table_name not in unique_tables:
            unique_tables[table_name] = item
        else:
            # 如果存在重复，优先保留execution_mode为full_refresh的配置
            if item["execution_mode"] == "full_refresh":
                unique_tables[table_name] = item
    
    result_tables = list(unique_tables.values())
    logger.info(f"去重后，共 {len(result_tables)} 个表需要处理")
    
    # 记录所有需要处理的表
    for idx, item in enumerate(result_tables, 1):
        logger.info(f"表[{idx}]: {item['table_name']}, 执行模式: {item['execution_mode']}")
    
    return result_tables

def optimize_execution_plan(tables):
    """
    优化表的执行计划
    
    参数:
        tables (list): 表配置列表
        
    返回:
        tuple: (优化后的表执行顺序, 依赖关系图)
    """
    logger.info("开始优化执行计划...")
    
    # 筛选出DataModel类型的表
    model_tables = []
    for table in tables:
        table_name = table["table_name"]
        if is_data_model_table(table_name):
            model_tables.append(table)
    
    logger.info(f"筛选出 {len(model_tables)} 个DataModel类型的表")
    
    if not model_tables:
        logger.warning("没有找到DataModel类型的表，无需优化执行计划")
        return [], {}
    
    # 获取表名列表
    table_names = [t["table_name"] for t in model_tables]
    
    # 创建有向图
    G = nx.DiGraph()
    
    # 添加所有节点
    for table_name in table_names:
        G.add_node(table_name)
    
    # 获取依赖关系
    dependency_dict = get_model_dependency_graph(table_names)
    logger.info(f"获取到 {len(dependency_dict)} 个表的依赖关系")
    
    # 添加依赖边
    edge_count = 0
    for target, upstreams in dependency_dict.items():
        for upstream in upstreams:
            if upstream in table_names:  # 确保只考虑当前处理的表
                G.add_edge(upstream, target)  # 从上游指向下游
                edge_count += 1
    
    logger.info(f"依赖图中添加了 {edge_count} 条边")
    
    # 检测循环依赖
    cycles = list(nx.simple_cycles(G))
    if cycles:
        logger.warning(f"检测到 {len(cycles)} 个循环依赖，将尝试打破循环")
        # 打破循环依赖（简单策略：移除每个循环中的最后一条边）
        for cycle in cycles:
            # 移除循环中的最后一条边
            G.remove_edge(cycle[-1], cycle[0])
            logger.info(f"打破循环依赖: 移除 {cycle[-1]} -> {cycle[0]} 的依赖")
    
    # 生成拓扑排序
    try:
        # 拓扑排序会从没有入边的节点开始，这些节点是上游节点，因此排序结果是从上游到下游
        execution_order = list(nx.topological_sort(G))
        logger.info(f"成功生成执行顺序，按从上游到下游顺序共 {len(execution_order)} 个表")
        
        # 创建结果依赖字典，包含所有表（即使没有依赖）
        result_dependency_dict = {name: [] for name in table_names}
        
        # 添加实际依赖关系
        for target, upstreams in dependency_dict.items():
            if target in table_names:  # 确保只考虑当前处理的表
                result_dependency_dict[target] = [u for u in upstreams if u in table_names]
        
        return execution_order, result_dependency_dict
    except Exception as e:
        logger.error(f"生成执行顺序失败: {str(e)}")
        # 如果拓扑排序失败，返回原始表名列表和空依赖图
        return table_names, {name: [] for name in table_names}

with DAG(
    "dag_data_model_scheduler", 
    start_date=datetime(2024, 1, 1), 
    schedule_interval="@daily", 
    catchup=False
) as dag:
    logger.info("初始化 dag_data_model_scheduler DAG")
    
    # 等待资源表 DAG 完成
    wait_for_resource = ExternalTaskSensor(
        task_id="wait_for_resource_loading",
        external_dag_id="dag_data_resource_scheduler",
        external_task_id="resource_loading_completed",
        mode="poke",
        timeout=3600,
        poke_interval=30
    )
    logger.info("创建资源表等待任务 - wait_for_resource_loading")

    # 创建一个完成标记任务
    model_processing_completed = EmptyOperator(
        task_id="model_processing_completed",
        dag=dag
    )
    logger.info("创建模型处理完成标记 - model_processing_completed")

    try:
        # 获取今日需要处理的所有表
        all_enabled_tables = get_all_enabled_tables_for_today()
        
        if not all_enabled_tables:
            logger.info("今天没有需要处理的表，直接连接开始和结束任务")
            wait_for_resource >> model_processing_completed
        else:
            # 优化执行计划
            execution_order, dependency_dict = optimize_execution_plan(all_enabled_tables)
            
            if not execution_order:
                logger.info("执行计划为空，直接连接开始和结束任务")
                wait_for_resource >> model_processing_completed
            else:
                # 创建任务字典
                task_dict = {}
                
                # 为每个表创建处理任务
                for table_name in execution_order:
                    # 查找表配置
                    table_config = next((t for t in all_enabled_tables if t["table_name"] == table_name), None)
                    
                    if table_config:
                        logger.info(f"为表 {table_name} 创建处理任务，执行模式: {table_config['execution_mode']}")
                        
                        # 创建任务
                        task = PythonOperator(
                            task_id=f"process_{table_name}",
                            python_callable=run_model_script,
                            op_kwargs={
                                "table_name": table_name,
                                "execution_mode": table_config["execution_mode"]
                            },
                            retries=TASK_RETRY_CONFIG["retries"],
                            retry_delay=timedelta(minutes=TASK_RETRY_CONFIG["retry_delay_minutes"]),
                            dag=dag
                        )
                        
                        # 将任务添加到字典
                        task_dict[table_name] = task
                
                # 设置任务间的依赖关系
                for table_name, task in task_dict.items():
                    # 获取上游依赖
                    upstream_tables = dependency_dict.get(table_name, [])
                    
                    if not upstream_tables:
                        # 如果没有上游依赖，直接连接到资源表等待任务
                        logger.info(f"表 {table_name} 没有上游依赖，连接到资源表等待任务")
                        wait_for_resource >> task
                    else:
                        # 设置与上游表的依赖关系
                        for upstream_table in upstream_tables:
                            if upstream_table in task_dict:
                                logger.info(f"设置依赖: {upstream_table} >> {table_name}")
                                task_dict[upstream_table] >> task
                    
                    # 检查是否是末端节点（没有下游节点）
                    is_terminal = True
                    for target, upstreams in dependency_dict.items():
                        if table_name in upstreams:
                            is_terminal = False
                            break
                    
                    # 如果是末端节点，连接到模型处理完成标记
                    if is_terminal:
                        logger.info(f"表 {table_name} 是末端节点，连接到模型处理完成标记")
                        task >> model_processing_completed
                
                # 处理特殊情况：检查是否有任务连接到完成标记
                has_connection_to_completed = False
                for task in task_dict.values():
                    for downstream in task.downstream_list:
                        if downstream.task_id == model_processing_completed.task_id:
                            has_connection_to_completed = True
                            break
                
                # 如果没有任务连接到完成标记，连接所有任务到完成标记
                if not has_connection_to_completed and task_dict:
                    logger.info("没有发现连接到完成标记的任务，连接所有任务到完成标记")
                    for task in task_dict.values():
                        task >> model_processing_completed
                
                # 处理特殊情况：如果资源等待任务没有下游任务，直接连接到完成标记
                if not wait_for_resource.downstream_list:
                    logger.info("资源等待任务没有下游任务，直接连接到完成标记")
                    wait_for_resource >> model_processing_completed
    
    except Exception as e:
        logger.error(f"构建DAG时出错: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        # 确保出错时也有完整的执行流
        wait_for_resource >> model_processing_completed