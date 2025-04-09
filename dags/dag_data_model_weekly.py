# dag_data_model_weekly.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime
from utils import get_enabled_tables, is_data_model_table, run_model_script, get_model_dependency_graph
from config import NEO4J_CONFIG
import pendulum
import logging

# 创建日志记录器
logger = logging.getLogger(__name__)

def is_monday():
    return True
    #return pendulum.now().day_of_week == 0

with DAG("dag_data_model_weekly", start_date=datetime(2024, 1, 1), schedule_interval="@daily", catchup=False) as dag:
    logger.info("初始化 dag_data_model_weekly DAG")
    
    # 等待日模型 DAG 完成
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
    weekly_completed = EmptyOperator(
        task_id="weekly_processing_completed",
        dag=dag
    )
    logger.info("创建任务完成标记 - weekly_processing_completed")
    
    # 检查今天是否是周一
    if is_monday():
        logger.info("今天是周一，开始处理周模型")
        # 获取启用的 weekly 模型表
        try:
            enabled_tables = get_enabled_tables("weekly")
            model_tables = [t for t in enabled_tables if is_data_model_table(t['table_name'])]
            logger.info(f"获取到 {len(model_tables)} 个启用的 weekly 模型表")
            
            if not model_tables:
                # 如果没有模型表需要处理，直接将等待任务与完成标记相连接
                logger.info("没有找到需要处理的周模型表，DAG将直接标记为完成")
                wait_for_daily >> weekly_completed
            else:
                # 获取依赖图
                try:
                    table_names = [t['table_name'] for t in model_tables]
                    dependency_graph = get_model_dependency_graph(table_names)
                    logger.info(f"构建了 {len(dependency_graph)} 个表的依赖关系图")
                except Exception as e:
                    logger.error(f"构建依赖关系图时出错: {str(e)}")
                    # 出错时也要确保完成标记被触发
                    wait_for_daily >> weekly_completed
                    raise

                # 构建 task 对象
                task_dict = {}
                for item in model_tables:
                    try:
                        task = PythonOperator(
                            task_id=f"process_weekly_{item['table_name']}",
                            python_callable=run_model_script,
                            op_kwargs={"table_name": item['table_name'], "execution_mode": item['execution_mode']},
                        )
                        task_dict[item['table_name']] = task
                        logger.info(f"创建模型处理任务: process_weekly_{item['table_name']}")
                    except Exception as e:
                        logger.error(f"创建任务 process_weekly_{item['table_name']} 时出错: {str(e)}")
                        # 出错时也要确保完成标记被触发
                        wait_for_daily >> weekly_completed
                        raise

                # 建立任务依赖（基于 DERIVED_FROM 图）
                dependency_count = 0
                for target, upstream_list in dependency_graph.items():
                    for upstream in upstream_list:
                        if upstream in task_dict and target in task_dict:
                            task_dict[upstream] >> task_dict[target]
                            dependency_count += 1
                            logger.debug(f"建立依赖关系: {upstream} >> {target}")
                        else:
                            logger.warning(f"无法建立依赖关系，缺少任务: {upstream} 或 {target}")

                logger.info(f"总共建立了 {dependency_count} 个任务依赖关系")

                # 最顶层的 task（没有任何上游）需要依赖日模型任务完成
                all_upstreams = set()
                for upstreams in dependency_graph.values():
                    all_upstreams.update(upstreams)
                top_level_tasks = [t for t in table_names if t not in all_upstreams]
                
                if top_level_tasks:
                    logger.info(f"发现 {len(top_level_tasks)} 个顶层任务: {', '.join(top_level_tasks)}")
                    for name in top_level_tasks:
                        wait_for_daily >> task_dict[name]
                else:
                    logger.warning("没有找到顶层任务，请检查依赖关系图是否正确")
                    # 如果没有顶层任务，直接将等待任务与完成标记相连接
                    wait_for_daily >> weekly_completed
                
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
                        task >> weekly_completed
                else:
                    # 如果没有终端任务（可能是因为存在循环依赖），直接将等待任务与完成标记相连接
                    logger.warning("没有找到终端任务，直接将等待任务与完成标记相连接")
                    wait_for_daily >> weekly_completed
        except Exception as e:
            logger.error(f"获取 weekly 模型表时出错: {str(e)}")
            # 出错时也要确保完成标记被触发
            wait_for_daily >> weekly_completed
            raise
    else:
        # 如果不是周一，直接将等待任务与完成标记相连接，跳过处理
        logger.info("今天不是周一，跳过周模型处理")
        wait_for_daily >> weekly_completed