#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
import sys
import os
from datetime import datetime
import psycopg2
import jinja2
# 修改导入方式，避免使用相对导入
import sys
import os

# 配置日志记录器
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger("execution_sql")

# 将同级目录加入到Python搜索路径
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.append(current_dir)

# 尝试导入script_utils
try:
    import script_utils
    logger.info("成功导入script_utils模块")
except ImportError as e:
    logger.error(f"无法直接导入script_utils: {str(e)}")
    
    # 尝试备用方法1：完整路径导入
    try:
        sys.path.append(os.path.dirname(current_dir))  # 添加父目录
        import dataops_scripts.script_utils as script_utils
        logger.info("使用完整路径成功导入script_utils模块")
    except ImportError as e2:
        logger.error(f"使用完整路径导入失败: {str(e2)}")
        
        # 尝试备用方法2：动态导入
        try:
            import importlib.util
            script_utils_path = os.path.join(current_dir, "script_utils.py")
            logger.info(f"尝试从路径动态导入: {script_utils_path}")
            
            spec = importlib.util.spec_from_file_location("script_utils", script_utils_path)
            script_utils = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(script_utils)
            logger.info("通过动态导入成功加载script_utils模块")
        except Exception as e3:
            logger.error(f"动态导入也失败: {str(e3)}")
            raise ImportError(f"无法导入script_utils模块，所有方法都失败")

# 添加健壮的导入机制
def get_config():
    """
    从config模块导入配置
    
    返回:
        dict: PG_CONFIG 数据库连接配置
    """
    # 默认配置
    default_pg_config = {
        "host": "localhost",
        "port": 5432,
        "user": "postgres",
        "password": "postgres",
        "database": "dataops",
    }
    
    try:
        # 动态导入，避免IDE警告
        config = __import__('config')
        logger.info("从config模块直接导入配置")
        pg_config = getattr(config, 'PG_CONFIG', default_pg_config)
        return pg_config
    except ImportError:
        # 使用默认配置
        logger.warning("无法导入config模块，使用默认值")
        return default_pg_config

# 导入配置
PG_CONFIG = get_config()
logger.info(f"配置加载完成: 数据库连接={PG_CONFIG['host']}:{PG_CONFIG['port']}/{PG_CONFIG['database']}")

def get_pg_conn():
    """获取PostgreSQL连接"""
    return psycopg2.connect(**PG_CONFIG)

def get_script_content(target_table, script_name):
    """
    从data_transform_scripts表中获取脚本内容和目标日期列
    
    参数:
        target_table (str): 目标表名
        script_name (str): 脚本名称
    
    返回:
        tuple: (script_content, target_dt_column) 脚本内容和目标日期列
    """
    logger.info(f"正在从data_transform_scripts表获取脚本内容，目标表: {target_table}，脚本名称: {script_name}")
    conn = None
    cursor = None
    try:
        conn = get_pg_conn()
        cursor = conn.cursor()
        
        query = """
            SELECT script_content, target_dt_column 
            FROM data_transform_scripts 
            WHERE target_table = %s AND script_name = %s 
            LIMIT 1
        """
        
        logger.info(f"执行SQL查询: {query}")
        logger.info(f"查询参数: target_table={target_table}, script_name={script_name}")
        
        cursor.execute(query, (target_table, script_name))
        result = cursor.fetchone()
        
        if result is None:
            logger.error(f"未找到目标表 '{target_table}' 和脚本名 '{script_name}' 对应的脚本")
            return None, None
        
        # 获取脚本内容和目标日期列    
        script_content = result[0]
        target_dt_column = result[1] if len(result) > 1 else None
        
        # 记录结果
        logger.info(f"目标日期列: {target_dt_column if target_dt_column else '未设置'}")
        
        # 记录脚本内容，但可能很长，只记录前500个字符和后100个字符
        if len(script_content) > 600:
            logger.info(f"成功获取脚本内容，总长度: {len(script_content)}字符")
            logger.info(f"脚本内容前500字符: \n{script_content[:500]}")
            logger.info(f"脚本内容后100字符: \n{script_content[-100:]}")
        else:
            logger.info(f"成功获取脚本内容，内容如下: \n{script_content}")
            
        return script_content, target_dt_column
        
    except Exception as e:
        logger.error(f"获取脚本内容时出错: {str(e)}", exc_info=True)
        return None, None
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def execute_sql(sql, params=None):
    """
    执行SQL语句
    
    参数:
        sql (str): SQL语句
        params (dict, optional): SQL参数
    
    返回:
        tuple: (成功标志, 影响的行数或结果)
    """
    conn = None
    cursor = None
    try:
        conn = get_pg_conn()
        cursor = conn.cursor()
        
        # 记录SQL（不包含敏感参数）
        # 由于SQL可能很长，只记录前200个字符
        if len(sql) > 200:
            logger.info(f"执行SQL (前200字符): {sql[:200]}...")
        else:
            logger.info(f"执行SQL: {sql}")
            
        if params:
            logger.info(f"SQL参数: {params}")
        
        # 执行SQL
        cursor.execute(sql, params)
        
        # 获取影响的行数
        if cursor.rowcount >= 0:
            affected_rows = cursor.rowcount
            logger.info(f"SQL执行成功，影响了 {affected_rows} 行")
        else:
            affected_rows = 0
            logger.info("SQL执行成功，但无法确定影响的行数")
        
        # 如果是SELECT语句，获取结果
        if sql.strip().upper().startswith("SELECT"):
            result = cursor.fetchall()
            logger.info(f"查询返回 {len(result)} 行结果")
            conn.commit()
            return True, {"affected_rows": affected_rows, "result": result}
        else:
            # 对于非SELECT语句，提交事务
            conn.commit()
            return True, {"affected_rows": affected_rows}
            
    except Exception as e:
        logger.error(f"执行SQL时出错: {str(e)}", exc_info=True)
        if conn:
            conn.rollback()
        return False, {"error": str(e)}
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def render_sql_template(sql_content, template_params):
    """
    使用Jinja2渲染SQL模板
    
    参数:
        sql_content (str): SQL模板内容
        template_params (dict): 模板参数
    
    返回:
        str: 渲染后的SQL
    """
    try:
        # 创建Jinja2环境
        env = jinja2.Environment(
            autoescape=False,
            undefined=jinja2.StrictUndefined  # 严格模式，未定义变量会抛出异常
        )
        
        # 创建模板并渲染
        template = env.from_string(sql_content)
        rendered_sql = template.render(**template_params)
        
        logger.info("SQL模板渲染成功")
        
        # 记录渲染后的SQL，只记录前200个字符和后100个字符
        if len(rendered_sql) > 300:
            logger.info(f"渲染后SQL前200字符: \n{rendered_sql[:200]}...")
            logger.info(f"渲染后SQL后100字符: \n...{rendered_sql[-100:]}")
        else:
            logger.info(f"渲染后SQL: \n{rendered_sql}")
            
        return rendered_sql
    except Exception as e:
        logger.error(f"渲染SQL模板时出错: {str(e)}", exc_info=True)
        raise

def run(script_type=None, target_table=None, script_name=None, exec_date=None, 
        frequency=None, **kwargs):
    """
    执行SQL脚本主入口函数
    
    参数:
        script_type (str): 脚本类型，必须为'sql'
        target_table (str): 目标表名
        script_name (str): 脚本名称
        exec_date (str): 执行日期，格式为YYYY-MM-DD
        frequency (str): 频率，可选值为 daily, weekly, monthly, quarterly, yearly
        **kwargs: 其他参数
    
    返回:
        bool: 是否执行成功
    """
    # 记录开始执行的时间
    start_time = datetime.now()
    logger.info(f"===== 开始执行 SQL 脚本 =====")
    logger.info(f"脚本类型: {script_type}")
    logger.info(f"目标表: {target_table}")
    logger.info(f"脚本名称: {script_name}")
    logger.info(f"执行日期: {exec_date}")
    logger.info(f"频率: {frequency}")
    
    # 记录其他参数
    for key, value in kwargs.items():
        logger.info(f"其他参数 - {key}: {value}")
    
    # 验证必要参数
    if not script_type or script_type.lower() != 'sql':
        logger.error(f"脚本类型必须为'sql'，当前为: {script_type}")
        return False
    
    if not target_table:
        logger.error("未提供目标表名")
        return False
    
    if not script_name:
        logger.error("未提供脚本名称")
        return False
    
    if not exec_date:
        logger.error("未提供执行日期")
        return False
    
    if not frequency:
        logger.error("未提供频率")
        return False
    
    try:
        # 获取脚本内容，直接从数据库获取，不检查文件是否存在
        sql_content, target_dt_column = get_script_content(target_table, script_name)
        if not sql_content:
            logger.error("无法获取脚本内容，执行失败")
            return False
        
        logger.info(f"成功获取脚本内容，长度: {len(sql_content)} 字符")
        
        # 计算日期范围
        try:
            start_date, end_date = script_utils.get_date_range(exec_date, frequency)
            logger.info(f"计算得到的日期范围: start_date={start_date}, end_date={end_date}")
        except Exception as date_err:
            logger.error(f"计算日期范围时出错: {str(date_err)}", exc_info=True)
            return False
        
        # 准备模板参数
        template_params = {
            'start_date': start_date,
            'end_date': end_date,
            # 可以添加更多默认参数
        }
        
        # 检查是否开启ETL幂等性
        target_table_label = kwargs.get('target_table_label', '')
        script_exec_mode = kwargs.get('execution_mode', 'append')  # 默认为append
        
        logger.info(f"脚本执行模式: {script_exec_mode}")
        
        # 导入config模块获取幂等性开关
        try:
            config = __import__('config')
            enable_idempotency = getattr(config, 'ENABLE_ETL_IDEMPOTENCY', False)
        except ImportError:
            logger.warning("无法导入config模块获取幂等性开关，默认为False")
            enable_idempotency = False
        
        logger.info(f"ETL幂等性开关状态: {enable_idempotency}")
        logger.info(f"目标表标签: {target_table_label}")
        
        # 如果开启了ETL幂等性且是SQL类型
        if script_type.lower() == 'sql' and enable_idempotency:
            
            # 处理append模式
            if script_exec_mode.lower() == 'append':
                logger.info("当前为append模式，开始考虑ETL幂等性处理")
                
                # 检查是否有目标日期列
                if target_dt_column:
                    logger.info(f"找到目标日期列 {target_dt_column}，将生成DELETE语句")
                    
                    # 生成DELETE语句
                    delete_sql = f"""DELETE FROM {target_table}
WHERE {target_dt_column} >= '{{{{ start_date }}}}'
  AND {target_dt_column} < '{{{{ end_date }}}}';"""
                    
                    logger.info(f"生成的DELETE语句: {delete_sql}")
                    
                    # 渲染DELETE SQL
                    try:
                        rendered_delete_sql = render_sql_template(delete_sql, template_params)
                        logger.info("成功渲染清理SQL")
                    except Exception as render_err:
                        logger.error(f"渲染清理SQL时出错: {str(render_err)}", exc_info=True)
                        # 即使清理SQL失败，仍然继续执行后续SQL
                        logger.warning("继续执行原始SQL")
                    else:
                        # 执行DELETE SQL
                        logger.info("执行清理SQL以实现幂等性")
                        delete_success, delete_result = execute_sql(rendered_delete_sql)
                        
                        if delete_success:
                            if isinstance(delete_result, dict) and "affected_rows" in delete_result:
                                logger.info(f"清理SQL执行成功，删除了 {delete_result['affected_rows']} 行数据")
                            else:
                                logger.info("清理SQL执行成功")
                        else:
                            logger.error(f"清理SQL执行失败: {delete_result.get('error', '未知错误')}")
                            # 继续执行原始SQL
                            logger.warning("继续执行原始SQL")
                else:
                    logger.warning(f"目标表 {target_table} 没有设置目标日期列(target_dt_column)，无法生成DELETE语句实现幂等性")
                    logger.warning("将直接执行原始SQL，可能导致数据重复")
            
            # 处理full_refresh模式
            elif script_exec_mode.lower() == 'full_refresh':
                logger.info("当前为full_refresh模式，将执行TRUNCATE操作")
                
                # 构建TRUNCATE语句
                truncate_sql = f"TRUNCATE TABLE {target_table};"
                logger.info(f"生成的TRUNCATE SQL: {truncate_sql}")
                
                # 执行TRUNCATE操作
                truncate_success, truncate_result = execute_sql(truncate_sql)
                
                if truncate_success:
                    logger.info(f"TRUNCATE TABLE {target_table} 执行成功，表已清空")
                else:
                    error_msg = truncate_result.get("error", "未知错误")
                    logger.error(f"TRUNCATE TABLE执行失败: {error_msg}")
                    # 继续执行原始SQL
                    logger.warning("TRUNCATE失败，继续执行原始SQL")
            
            else:
                logger.info(f"当前执行模式 {script_exec_mode} 不是append或full_refresh，不执行幂等性处理")
        else:
            logger.info("未满足ETL幂等性处理条件，直接执行原始SQL")
        
        # 渲染原始SQL模板
        try:
            rendered_sql = render_sql_template(sql_content, template_params)
        except Exception as render_err:
            logger.error(f"渲染SQL模板时出错: {str(render_err)}", exc_info=True)
            return False
        
        # 执行原始SQL
        success, result = execute_sql(rendered_sql)
        
        if success:
            # 记录执行结果
            if isinstance(result, dict):
                if "affected_rows" in result:
                    logger.info(f"SQL执行成功，影响了 {result['affected_rows']} 行数据")
                if "result" in result:
                    logger.info(f"SQL查询返回 {len(result['result'])} 行结果")
            else:
                logger.info("SQL执行成功")
            
            # 记录执行时间
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            logger.info(f"===== SQL脚本执行完成 (成功) =====")
            logger.info(f"总耗时: {duration:.2f}秒")
            
            return True
        else:
            # 记录错误信息
            error_msg = result.get("error", "未知错误")
            logger.error(f"SQL执行失败: {error_msg}")
            
            # 记录执行时间
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            logger.info(f"===== SQL脚本执行完成 (失败) =====")
            logger.info(f"总耗时: {duration:.2f}秒")
            
            return False
            
    except Exception as e:
        # 捕获所有未处理的异常
        logger.error(f"执行SQL脚本时发生未预期的错误: {str(e)}", exc_info=True)
        return False

if __name__ == "__main__":
    # 直接执行时的测试代码
    import argparse
    
    parser = argparse.ArgumentParser(description='执行SQL脚本')
    parser.add_argument('--target-table', type=str, required=True, help='目标表名')
    parser.add_argument('--script-name', type=str, required=True, help='脚本名称')
    parser.add_argument('--exec-date', type=str, required=True, help='执行日期 (YYYY-MM-DD)')
    parser.add_argument('--frequency', type=str, required=True, 
                        choices=['daily', 'weekly', 'monthly', 'quarterly', 'yearly'], 
                        help='频率: daily, weekly, monthly, quarterly, yearly')
    
    args = parser.parse_args()
    
    # 构造必要的 kwargs
    run_kwargs = {
        "script_type": "sql",
        "target_table": args.target_table,
        "script_name": args.script_name,
        "exec_date": args.exec_date,
        "frequency": args.frequency,
    }

    logger.info("命令行测试执行参数: " + str(run_kwargs))

    success = run(**run_kwargs)
    
    if success:
        logger.info("SQL脚本执行成功")
        sys.exit(0)
    else:
        logger.error("SQL脚本执行失败")
        sys.exit(1) 