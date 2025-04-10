from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime
from utils import get_enabled_tables, is_data_model_table, run_model_script, get_model_dependency_graph, check_table_relationship
from config import NEO4J_CONFIG
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
    
    # 等待周模型 DAG 完成
    wait_for_weekly = ExternalTaskSensor(
        task_id="wait_for_weekly_model",
        external_dag_id="dag_data_model_weekly",
        external_task_id="weekly_processing_completed",  # 指定完成标记任务
        mode="poke",
        timeout=3600,
        poke_interval=30
    )
    logger.info("创建周模型等待任务 - wait_for_weekly_model")
    
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
            model_tables = [t for t in enabled_tables if is_data_model_table(t['table_name'])]
            logger.info(f"获取到 {len(model_tables)} 个启用的 monthly 模型表")
            
            if not model_tables:
                # 如果没有模型表需要处理，直接将等待任务与完成标记相连接
                logger.info("没有找到需要处理的月模型表，DAG将直接标记为完成")
                wait_for_weekly >> monthly_completed
            else:
                # 获取表名列表
                table_names = [t['table_name'] for t in model_tables]
                
                # 特别检查两个表之间的关系
                if 'book_sale_amt_yearly' in table_names and 'book_sale_amt_monthly' in table_names:
                    logger.info("特别检查 book_sale_amt_yearly 和 book_sale_amt_monthly 之间的关系")
                    relationship = check_table_relationship('book_sale_amt_yearly', 'book_sale_amt_monthly')
                    logger.info(f"关系检查结果: {relationship}")
                
                # 使用优化函数生成执行顺序，可以处理循环依赖
                optimized_table_order = generate_optimized_execution_order(table_names)
                logger.info(f"生成优化执行顺序, 共 {len(optimized_table_order)} 个表")
                
                # 获取依赖图 (仍然需要用于设置任务依赖关系)
                try:
                    dependency_graph = get_model_dependency_graph(table_names)
                    logger.info(f"构建了 {len(dependency_graph)} 个表的依赖关系图")
                except Exception as e:
                    logger.error(f"构建依赖关系图时出错: {str(e)}")
                    # 出错时也要确保完成标记被触发
                    wait_for_weekly >> monthly_completed
                    raise

                # 构建 task 对象
                task_dict = {}
                for table_name in optimized_table_order:
                    # 获取表的配置信息
                    table_config = next((t for t in model_tables if t['table_name'] == table_name), None)
                    if table_config:
                        try:
                            task = PythonOperator(
                                task_id=f"process_monthly_{table_name}",
                                python_callable=run_model_script,
                                op_kwargs={"table_name": table_name, "execution_mode": table_config['execution_mode']},
                            )
                            task_dict[table_name] = task
                            logger.info(f"创建模型处理任务: process_monthly_{table_name}")
                        except Exception as e:
                            logger.error(f"创建任务 process_monthly_{table_name} 时出错: {str(e)}")
                            # 出错时也要确保完成标记被触发
                            wait_for_weekly >> monthly_completed
                            raise

                # 建立任务依赖（基于 DERIVED_FROM 图）
                dependency_count = 0
                logger.info("开始建立任务依赖关系...")
                for target, upstream_list in dependency_graph.items():
                    logger.info(f"处理目标表 {target} 的依赖关系")
                    for upstream in upstream_list:
                        if upstream in task_dict and target in task_dict:
                            logger.info(f"建立依赖边: {upstream} >> {target}")
                            task_dict[upstream] >> task_dict[target]
                            dependency_count += 1
                            logger.debug(f"建立依赖关系: {upstream} >> {target}")
                        else:
                            missing = []
                            if upstream not in task_dict:
                                missing.append(f"上游表 {upstream}")
                            if target not in task_dict:
                                missing.append(f"目标表 {target}")
                            missing_str = " 和 ".join(missing)
                            logger.warning(f"无法建立依赖关系: {upstream} >> {target}，缺少任务: {missing_str}")

                logger.info(f"总共建立了 {dependency_count} 个任务依赖关系")
                logger.info(f"任务字典中的所有表: {list(task_dict.keys())}")

                # 最顶层的 task（没有任何上游）需要依赖周模型任务完成
                all_upstreams = set()
                for upstreams in dependency_graph.values():
                    all_upstreams.update(upstreams)
                top_level_tasks = [t for t in table_names if t not in all_upstreams]
                logger.info(f"所有上游表集合: {all_upstreams}")
                logger.info(f"识别出的顶层表: {top_level_tasks}")
                
                if top_level_tasks:
                    logger.info(f"发现 {len(top_level_tasks)} 个顶层任务: {', '.join(top_level_tasks)}")
                    for name in top_level_tasks:
                        if name in task_dict:
                            wait_for_weekly >> task_dict[name]
                else:
                    logger.warning("没有找到顶层任务，请检查依赖关系图是否正确")
                    # 如果没有顶层任务，直接将等待任务与完成标记相连接
                    wait_for_weekly >> monthly_completed
                
                # 连接所有末端任务（没有下游任务的）到完成标记
                # 找出所有没有下游任务的任务（即终端任务）
                terminal_tasks = []
                for table_name, task in task_dict.items():
                    is_terminal = True
                    for upstream_list in dependency_graph.values():
                        if table_name in upstream_list:
                            is_terminal = False
                            break
                    if is_terminal:
                        terminal_tasks.append(task)
                        logger.debug(f"发现终端任务: {table_name}")
                
                # 如果有终端任务，将它们连接到完成标记
                if terminal_tasks:
                    logger.info(f"连接 {len(terminal_tasks)} 个终端任务到完成标记")
                    for task in terminal_tasks:
                        task >> monthly_completed
                else:
                    # 如果没有终端任务（可能是因为存在循环依赖），直接将等待任务与完成标记相连接
                    logger.warning("没有找到终端任务，直接将等待任务与完成标记相连接")
                    wait_for_weekly >> monthly_completed
        except Exception as e:
            logger.error(f"获取 monthly 模型表时出错: {str(e)}")
            # 出错时也要确保完成标记被触发
            wait_for_weekly >> monthly_completed
            raise
    else:
        # 如果不是月初，直接将等待任务与完成标记相连接，跳过处理
        logger.info("今天不是月初，跳过月模型处理")
        wait_for_weekly >> monthly_completed