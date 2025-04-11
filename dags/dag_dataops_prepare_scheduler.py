# dag_dataops_prepare_scheduler.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import pendulum
import logging
from common import get_pg_conn, get_neo4j_driver, get_today_date
from config import PG_CONFIG, NEO4J_CONFIG
import networkx as nx

# 创建日志记录器
logger = logging.getLogger(__name__)

def get_enabled_tables():
    """获取所有启用的表"""
    conn = get_pg_conn()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT owner_id, table_name 
            FROM schedule_status 
            WHERE schedule_is_enabled = TRUE
        """)
        result = cursor.fetchall()
        return [row[1] for row in result]  # 只返回表名
    except Exception as e:
        logger.error(f"获取启用表失败: {str(e)}")
        return []
    finally:
        cursor.close()
        conn.close()

def check_table_directly_subscribed(table_name):
    """检查表是否在schedule_status表中直接订阅"""
    conn = get_pg_conn()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT schedule_is_enabled
            FROM schedule_status 
            WHERE table_name = %s
        """, (table_name,))
        result = cursor.fetchone()
        return result and result[0] is True
    except Exception as e:
        logger.error(f"检查表订阅状态失败: {str(e)}")
        return False
    finally:
        cursor.close()
        conn.close()

def get_table_info_from_neo4j(table_name):
    """从Neo4j获取表的详细信息"""
    driver = get_neo4j_driver()
     # 检查表是否直接订阅
    is_directly_schedule = check_table_directly_subscribed(table_name)

    table_info = {
        'target_table': table_name,
        'is_directly_schedule': is_directly_schedule,  # 初始值设为True，从schedule_status表获取
    }
    
    try:
        with driver.session() as session:
            # 查询表标签和状态
            query_table = """
                MATCH (t {en_name: $table_name})
                RETURN labels(t) AS labels, t.status AS status, t.frequency AS frequency
            """
            result = session.run(query_table, table_name=table_name)
            record = result.single()
            
            if record:
                labels = record.get("labels", [])
                table_info['target_table_label'] = [label for label in labels if label in ["DataResource", "DataModel", "DataSource"]][0] if labels else None
                table_info['target_table_status'] = record.get("status", True)  # 默认为True
                table_info['default_update_frequency'] = record.get("frequency")
                
                # 根据标签类型查询关系和脚本信息
                if "DataResource" in labels:
                    query_rel = """
                        MATCH (target {en_name: $table_name})-[rel:ORIGINATES_FROM]->(source)
                        RETURN source.en_name AS source_table, rel.script_name AS script_name,
                               rel.script_type AS script_type, rel.script_exec_mode AS script_exec_mode
                    """
                elif "DataModel" in labels:
                    query_rel = """
                        MATCH (target {en_name: $table_name})-[rel:DERIVED_FROM]->(source)
                        RETURN source.en_name AS source_table, rel.script_name AS script_name,
                               rel.script_type AS script_type, rel.script_exec_mode AS script_exec_mode
                    """
                else:
                    logger.warning(f"表 {table_name} 不是DataResource或DataModel类型")
                    return table_info
                
                result = session.run(query_rel, table_name=table_name)
                record = result.single()
                
                if record:
                    table_info['source_table'] = record.get("source_table")     

                    # 检查script_name是否为空
                    script_name = record.get("script_name")
                    if not script_name:
                        logger.warning(f"表 {table_name} 的关系中没有script_name属性，可能导致后续处理出错")
                    table_info['script_name'] = script_name
                    
                    # 设置默认值，确保即使属性为空也有默认值
                    table_info['script_type'] = record.get("script_type", "python")  # 默认为python
                    table_info['script_exec_mode'] = record.get("script_exec_mode", "append")  # 默认为append
                else:
                    logger.warning(f"未找到表 {table_name} 的关系信息")
            else:
                logger.warning(f"在Neo4j中找不到表 {table_name} 的信息")
    except Exception as e:
        logger.error(f"获取表 {table_name} 的信息时出错: {str(e)}")
    finally:
        driver.close()
    
    return table_info

def process_dependencies(tables_info):
    """处理表间依赖关系，添加被动调度的表"""
    # 存储所有表信息的字典
    all_tables = {t['target_table']: t for t in tables_info}
    driver = get_neo4j_driver()
    
    try:
        with driver.session() as session:
            for table_name, table_info in list(all_tables.items()):
                if table_info.get('target_table_label') == 'DataModel':
                    # 查询其依赖表
                    query = """
                        MATCH (dm {en_name: $table_name})-[:DERIVED_FROM]->(dep)
                        RETURN dep.en_name AS dep_name, labels(dep) AS dep_labels, 
                               dep.status AS dep_status, dep.frequency AS dep_frequency
                    """
                    result = session.run(query, table_name=table_name)
                    
                    for record in result:
                        dep_name = record.get("dep_name")
                        dep_labels = record.get("dep_labels", [])
                        dep_status = record.get("dep_status", True)
                        dep_frequency = record.get("dep_frequency")
                        
                        # 处理未被直接调度的依赖表
                        if dep_name and dep_name not in all_tables:
                            logger.info(f"发现被动依赖表: {dep_name}, 标签: {dep_labels}")
                            
                            # 获取依赖表详细信息
                            dep_info = get_table_info_from_neo4j(dep_name)
                            dep_info['is_directly_schedule'] = False
                            
                            # 处理调度频率继承
                            if not dep_info.get('default_update_frequency'):
                                dep_info['default_update_frequency'] = table_info.get('default_update_frequency')
                            
                            all_tables[dep_name] = dep_info
    except Exception as e:
        logger.error(f"处理依赖关系时出错: {str(e)}")
    finally:
        driver.close()
    
    return list(all_tables.values())

def filter_invalid_tables(tables_info):
    """过滤无效表及其依赖，使用NetworkX构建依赖图"""
    # 构建表名到索引的映射
    table_dict = {t['target_table']: i for i, t in enumerate(tables_info)}
    
    # 找出无效表
    invalid_tables = set()
    for table in tables_info:
        if table.get('target_table_status') is False:
            invalid_tables.add(table['target_table'])
            logger.info(f"表 {table['target_table']} 的状态为无效")
    
    # 构建依赖图
    G = nx.DiGraph()
    
    # 添加所有节点
    for table in tables_info:
        G.add_node(table['target_table'])
    
    # 查询并添加依赖边
    driver = get_neo4j_driver()
    try:
        with driver.session() as session:
            for table in tables_info:
                if table.get('target_table_label') == 'DataModel':
                    query = """
                        MATCH (source {en_name: $table_name})-[:DERIVED_FROM]->(target)
                        RETURN target.en_name AS target_name
                    """
                    result = session.run(query, table_name=table['target_table'])
                    
                    for record in result:
                        target_name = record.get("target_name")
                        if target_name and target_name in table_dict:
                            # 添加从目标到源的边，表示目标依赖于源
                            G.add_edge(table['target_table'], target_name)
                            logger.debug(f"添加依赖边: {table['target_table']} -> {target_name}")
    except Exception as e:
        logger.error(f"构建依赖图时出错: {str(e)}")
    finally:
        driver.close()
    
    # 找出依赖于无效表的所有表
    downstream_invalid = set()
    for invalid_table in invalid_tables:
        # 获取可从无效表到达的所有节点
        try:
            descendants = nx.descendants(G, invalid_table)
            downstream_invalid.update(descendants)
            logger.info(f"表 {invalid_table} 的下游无效表: {descendants}")
        except Exception as e:
            logger.error(f"处理表 {invalid_table} 的下游依赖时出错: {str(e)}")
    
    # 合并所有无效表
    all_invalid = invalid_tables.union(downstream_invalid)
    logger.info(f"总共 {len(all_invalid)} 个表被标记为无效: {all_invalid}")
    
    # 过滤出有效表
    valid_tables = [t for t in tables_info if t['target_table'] not in all_invalid]
    logger.info(f"过滤后保留 {len(valid_tables)} 个有效表")
    
    return valid_tables

def write_to_airflow_dag_schedule(exec_date, tables_info):
    """将表信息写入airflow_dag_schedule表"""
    conn = get_pg_conn()
    cursor = conn.cursor()
    
    try:
        # 清理当日数据，避免重复
        cursor.execute("""
            DELETE FROM airflow_dag_schedule WHERE exec_date = %s
        """, (exec_date,))
        logger.info(f"已清理执行日期 {exec_date} 的现有数据")
        
        # 批量插入新数据
        inserted_count = 0
        for table in tables_info:
            cursor.execute("""
                INSERT INTO airflow_dag_schedule (
                    exec_date, source_table, target_table, target_table_label,
                    target_table_status, is_directly_schedule, default_update_frequency,
                    script_name, script_type, script_exec_mode
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                exec_date,
                table.get('source_table'),
                table['target_table'],
                table.get('target_table_label'),
                table.get('target_table_status', True),
                table.get('is_directly_schedule', False),
                table.get('default_update_frequency'),
                table.get('script_name'),
                table.get('script_type', 'python'),
                table.get('script_exec_mode', 'append')
            ))
            inserted_count += 1
        
        conn.commit()
        logger.info(f"成功插入 {inserted_count} 条记录到 airflow_dag_schedule 表")
        return inserted_count
    except Exception as e:
        logger.error(f"写入 airflow_dag_schedule 表时出错: {str(e)}")
        conn.rollback()
        # 不要返回0，而是重新抛出异常，确保错误被正确传播
        raise
    finally:
        cursor.close()
        conn.close()

def prepare_dag_schedule(**kwargs):
    """准备DAG调度任务的主函数"""
    exec_date = kwargs.get('ds') or get_today_date()
    logger.info(f"开始准备执行日期 {exec_date} 的调度任务")
    
    # 1. 获取启用的表
    enabled_tables = get_enabled_tables()
    logger.info(f"从schedule_status表获取到 {len(enabled_tables)} 个启用的表")
    
    if not enabled_tables:
        logger.warning("没有找到启用的表，准备工作结束")
        return 0
    
    # 2. 获取表的详细信息
    tables_info = []
    for table_name in enabled_tables:
        table_info = get_table_info_from_neo4j(table_name)
        if table_info:
            tables_info.append(table_info)
    
    logger.info(f"成功获取 {len(tables_info)} 个表的详细信息")
    
    # 3. 处理依赖关系，添加被动调度的表
    enriched_tables = process_dependencies(tables_info)
    logger.info(f"处理依赖后，总共有 {len(enriched_tables)} 个表")
    
    # 4. 过滤无效表及其依赖
    valid_tables = filter_invalid_tables(enriched_tables)
    logger.info(f"过滤无效表后，最终有 {len(valid_tables)} 个有效表")
    
    # 5. 写入airflow_dag_schedule表
    inserted_count = write_to_airflow_dag_schedule(exec_date, valid_tables)
    
    # 6. 检查插入操作是否成功，如果失败则抛出异常
    if inserted_count == 0 and valid_tables:
        error_msg = f"插入操作失败，无记录被插入到airflow_dag_schedule表，但有{len(valid_tables)}个有效表需要处理"
        logger.error(error_msg)
        raise Exception(error_msg)
    
    return inserted_count

# 创建DAG
with DAG(
    "dag_dataops_prepare_scheduler",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    }
) as dag:
    
    # 任务开始标记
    start_preparation = EmptyOperator(
        task_id="start_preparation",
        dag=dag
    )
    
    # 准备调度任务
    prepare_task = PythonOperator(
        task_id="prepare_dag_schedule",
        python_callable=prepare_dag_schedule,
        provide_context=True,
        dag=dag
    )
    
    # 准备完成标记
    preparation_completed = EmptyOperator(
        task_id="preparation_completed",
        dag=dag
    )
    
    # 设置任务依赖
    start_preparation >> prepare_task >> preparation_completed