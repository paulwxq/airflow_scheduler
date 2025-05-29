#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
import sys
import os
import pandas as pd
import psycopg2
from datetime import datetime, timedelta
import csv
import glob
import shutil
import re
import argparse
import psycopg2.extras

# 修改Python导入路径，确保能找到同目录下的script_utils模块
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.insert(0, current_dir)
# 先导入整个模块，确保script_utils对象在全局作用域可用
import script_utils
# 再导入具体的方法
from script_utils import get_pg_config, get_upload_paths, get_config_param, logger as utils_logger
SCHEDULE_TABLE_SCHEMA = get_config_param("SCHEDULE_TABLE_SCHEMA")

# 配置日志记录器
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger("load_file")

# 使用script_utils中的方法获取配置
try:
    # 获取PostgreSQL配置
    PG_CONFIG = get_pg_config()
    # 获取上传和归档路径
    STRUCTURE_UPLOAD_BASE_PATH, STRUCTURE_UPLOAD_ARCHIVE_BASE_PATH = get_upload_paths()
    logger.info(f"通过script_utils获取配置成功: 上传路径={STRUCTURE_UPLOAD_BASE_PATH}, 归档路径={STRUCTURE_UPLOAD_ARCHIVE_BASE_PATH}")
except Exception as e:
    logger.error(f"获取配置失败，使用默认值: {str(e)}")
    raise

def get_pg_conn():
    """获取PostgreSQL连接"""
    return psycopg2.connect(**PG_CONFIG)

def get_table_columns(table_name):
    """
    获取表的列信息，包括列名和注释
    
    返回:
        dict: {列名: 列注释} 的字典
    """
    conn = get_pg_conn()
    cursor = conn.cursor()
    try:
        # 查询表列信息
        cursor.execute("""
            SELECT 
                column_name, 
                col_description((table_schema || '.' || table_name)::regclass::oid, ordinal_position) as column_comment
            FROM 
                information_schema.columns
            WHERE 
                table_schema = 'public' -- 明确指定 schema，如果需要
                AND table_name = %s
            ORDER BY 
                ordinal_position
        """, (table_name.lower(),))
        
        columns = {}
        empty_comment_columns = []  # 记录注释为空的列
        
        for row in cursor.fetchall():
            col_name = row[0]
            col_comment = row[1]
            
            # 检查注释是否为空
            if not col_comment:
                empty_comment_columns.append(col_name)
                col_comment = col_name  # 如果注释为空，使用列名
            
            columns[col_name] = col_comment
            
        if not columns:
             logger.warning(f"未能获取到表 '{table_name}' 的列信息，请检查表是否存在、schema是否正确以及权限。")
        else:
            # 检查是否有注释为空的列，如果有则发出告警
            if empty_comment_columns:
                logger.warning(f"表 '{table_name}' 中以下列的注释为空: {empty_comment_columns}")
            else:
                logger.info(f"表 '{table_name}' 的所有列都有注释，数据匹配将更加准确")

        return columns
    except Exception as e:
        logger.error(f"获取表 '{table_name}' 的列信息时出错: {str(e)}")
        return {}
    finally:
        cursor.close()
        conn.close()

def match_file_columns(file_headers, table_columns):
    """
    匹配文件列名与表列名
    
    策略:
    1. 尝试通过表字段注释匹配文件列名 (忽略大小写和空格)
    2. 尝试通过名称直接匹配 (忽略大小写和空格)
    
    参数:
        file_headers (list): 文件的列名列表
        table_columns (dict): {数据库列名: 列注释} 的字典
    
    返回:
        dict: {文件列名: 数据库列名} 的映射字典
    """
    mapping = {}
    matched_table_cols = set()

    # 数据库列名通常不区分大小写（除非加引号），注释可能区分
    # 为了匹配更健壮，我们将文件和数据库列名/注释都转为小写处理
    processed_table_columns_lower = {col.lower(): col for col in table_columns.keys()}
    processed_comment_to_column_lower = {
        str(comment).lower(): col
        for col, comment in table_columns.items() if comment
    }

    # 预处理文件headers
    processed_file_headers_lower = {str(header).lower(): header for header in file_headers}

    # 添加调试日志
    logger.info(f"开始匹配过程，文件列数: {len(file_headers)}, 表列数: {len(table_columns)}")
    logger.info(f"表列及其注释: {table_columns}")
    logger.info(f"处理后的注释到列名映射: {processed_comment_to_column_lower}")

    # 1. 通过注释匹配 (忽略大小写)
    logger.info("=== 开始通过注释匹配 ===")
    for processed_header, original_header in processed_file_headers_lower.items():
        logger.info(f"尝试匹配文件列 '{original_header}' (处理后: '{processed_header}')")
        if processed_header in processed_comment_to_column_lower:
            table_col_original_case = processed_comment_to_column_lower[processed_header]
            if table_col_original_case not in matched_table_cols:
                mapping[original_header] = table_col_original_case
                matched_table_cols.add(table_col_original_case)
                logger.info(f"通过注释匹配: 文件列 '{original_header}' -> 表列 '{table_col_original_case}'")
            else:
                logger.warning(f"表列 '{table_col_original_case}' 已被匹配，跳过文件列 '{original_header}'")
        else:
            logger.info(f"文件列 '{original_header}' 在注释中未找到匹配")

    # 2. 通过名称直接匹配 (忽略大小写)，仅匹配尚未映射的列
    logger.info("=== 开始通过名称直接匹配 ===")
    for processed_header, original_header in processed_file_headers_lower.items():
         if original_header not in mapping: # 仅当此文件列尚未映射时才进行名称匹配
            logger.info(f"尝试名称匹配文件列 '{original_header}' (处理后: '{processed_header}')")
            if processed_header in processed_table_columns_lower:
                table_col_original_case = processed_table_columns_lower[processed_header]
                if table_col_original_case not in matched_table_cols:
                    mapping[original_header] = table_col_original_case
                    matched_table_cols.add(table_col_original_case)
                    logger.info(f"通过名称匹配: 文件列 '{original_header}' -> 表列 '{table_col_original_case}'")
                else:
                    logger.warning(f"表列 '{table_col_original_case}' 已被匹配，跳过文件列 '{original_header}'")
            else:
                logger.info(f"文件列 '{original_header}' 在表列名中未找到匹配")
         else:
            logger.info(f"文件列 '{original_header}' 已通过注释匹配，跳过名称匹配")

    logger.info(f"=== 匹配完成，成功匹配 {len(mapping)} 个列 ===")

    unmapped_file = [h for h in file_headers if h not in mapping]
    if unmapped_file:
         logger.warning(f"以下文件列未能匹配到表列: {unmapped_file}")

    unmapped_table = [col for col in table_columns if col not in matched_table_cols]
    if unmapped_table:
        logger.warning(f"以下表列未能匹配到文件列: {unmapped_table}")

    return mapping

def read_excel_file(excel_file, sheet_name=0):
    """
    读取Excel文件内容
    
    参数:
        excel_file (str): Excel文件路径
        sheet_name: 工作表名称或索引，默认为第一个工作表
    
    返回:
        pandas.DataFrame: 读取的数据
    """
    try:
        # 尝试读取Excel文件，使用dtype=str确保所有列按字符串读取
        df = pd.read_excel(excel_file, sheet_name=sheet_name, keep_default_na=False, 
                          na_values=[r'\N', '', 'NULL', 'null'], dtype=str)
        logger.info(f"成功读取Excel文件: {os.path.basename(excel_file)}, 共 {len(df)} 行")
        return df
    except Exception as e:
        logger.error(f"读取Excel文件 {excel_file} 时发生错误: {str(e)}")
        raise

def read_csv_file(csv_file):
    """
    读取CSV文件内容，尝试自动检测编码
    
    参数:
        csv_file (str): CSV文件路径
    
    返回:
        pandas.DataFrame: 读取的数据
    """
    try:
        # 使用 dtype=str 确保所有列按字符串读取，避免类型推断问题
        df = pd.read_csv(csv_file, encoding='utf-8', keep_default_na=False, 
                        na_values=[r'\N', '', 'NULL', 'null'], dtype=str)
        return df
    except UnicodeDecodeError:
        try:
            logger.warning(f"UTF-8 读取失败，尝试 GBK: {csv_file}")
            df = pd.read_csv(csv_file, encoding='gbk', keep_default_na=False, 
                            na_values=[r'\N', '', 'NULL', 'null'], dtype=str)
            return df
        except UnicodeDecodeError:
            logger.warning(f"GBK 读取也失败，尝试 latin1: {csv_file}")
            df = pd.read_csv(csv_file, encoding='latin1', keep_default_na=False, 
                            na_values=[r'\N', '', 'NULL', 'null'], dtype=str)
            return df
    except Exception as e:
        logger.error(f"读取CSV文件 {csv_file} 时发生错误: {str(e)}")
        raise

def load_dataframe_to_table(df, file_path, table_name):
    """
    将DataFrame数据加载到目标表
    
    参数:
        df (pandas.DataFrame): 需要加载的数据
        file_path (str): 源文件路径（仅用于日志记录）
        table_name (str): 目标表名
    
    返回:
        bool: 成功返回True，失败返回False
    """
    conn = None
    cursor = None
    try:
        # 清理列名中的潜在空白符
        df.columns = df.columns.str.strip()

        # 如果DataFrame为空，则直接认为成功并返回
        if df.empty:
            logger.info(f"文件 {file_path} 为空，无需加载数据。")
            return True

        # 获取文件列名（清理后）
        file_headers = df.columns.tolist()
        logger.info(f"清理后的文件列名: {file_headers}")
        
        # 获取表结构
        table_columns = get_table_columns(table_name)
        if not table_columns:
            logger.error(f"无法获取表 '{table_name}' 的列信息，跳过文件 {file_path}")
            return False
        
        # 匹配文件列与表列
        column_mapping = match_file_columns(file_headers, table_columns)
        
        # 检查是否有任何列成功映射
        if not column_mapping:
            logger.error(f"文件 {file_path} 的列无法与表 '{table_name}' 的列建立任何映射关系，跳过此文件。")
            return False
        
        # 仅选择成功映射的列进行加载
        mapped_file_headers = list(column_mapping.keys())
        # 避免SettingWithCopyWarning
        df_mapped = df[mapped_file_headers].copy()
        df_mapped.rename(columns=column_mapping, inplace=True)
        logger.info(f"将加载以下映射后的列: {df_mapped.columns.tolist()}")

        # 将空字符串替换为None
        for col in df_mapped.columns:
            df_mapped[col] = df_mapped[col].map(lambda x: None if isinstance(x, str) and x == '' else x)
        
        # 记录处理前的行数
        original_row_count = len(df_mapped)
        
        # 丢弃全部为空值的行
        df_mapped = df_mapped.dropna(how='all')
        final_row_count = len(df_mapped)
        
        # 计算删除的空行数量
        empty_rows_count = original_row_count - final_row_count
        
        if empty_rows_count > 0:
            logger.warning(f"发现并删除了 {empty_rows_count} 行全空行")
            logger.info(f"丢弃全空行后剩余数据行数: {final_row_count}")
        else:
            logger.info(f"数据行数: {final_row_count}")

        # 检查目标表是否有create_time字段，如果有则添加当前时间
        if 'create_time' in table_columns:
            current_time = datetime.now()
            df_mapped['create_time'] = current_time
            logger.info(f"目标表有 create_time 字段，设置值为: {current_time}")
        else:
            logger.warning(f"目标表 '{table_name}' 没有 create_time 字段，跳过添加时间戳")

        # 连接数据库
        conn = get_pg_conn()
        cursor = conn.cursor()
        
        # 构建INSERT语句
        columns = ', '.join([f'"{col}"' for col in df_mapped.columns])
        placeholders = ', '.join(['%s'] * len(df_mapped.columns))
        insert_sql = f'INSERT INTO public."{table_name}" ({columns}) VALUES ({placeholders})'
        
        # 批量插入数据
        rows = [tuple(row) for row in df_mapped.values]
        
        try:
            cursor.executemany(insert_sql, rows)
            conn.commit()
            logger.info(f"成功将文件 {os.path.basename(file_path)} 的 {len(rows)} 行数据插入到表 '{table_name}'")
            return True
        except Exception as insert_err:
            logger.error(f"向表 '{table_name}' 插入数据时出错: {str(insert_err)}")
            logger.error(f"出错的 SQL 语句大致为: {insert_sql}")
            try:
                logger.error(f"出错的前3行数据 (部分): {rows[:3]}")
            except:
                pass
            conn.rollback()
            return False
            
    except Exception as e:
        logger.error(f"处理文件 {file_path} 加载到表 '{table_name}' 时发生意外错误", exc_info=True)
        if conn:
            conn.rollback()
        return False
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def load_file_to_table(file_path, table_name, update_mode='append'):
    """
    加载文件到表，支持不同的文件类型
    
    参数:
        file_path (str): 文件路径
        table_name (str): 目标表名
        update_mode (str): 更新模式，'append'或'full_refresh'
    
    返回:
        bool: 成功返回True，失败返回False
    """
    logger.info(f"开始处理文件: {file_path}")
    try:
        file_extension = os.path.splitext(file_path)[1].lower()
        
        # 根据文件扩展名选择合适的加载方法
        if file_extension == '.csv':
            # CSV文件处理
            df = read_csv_file(file_path)
            return load_dataframe_to_table(df, file_path, table_name)
        elif file_extension in ['.xlsx', '.xls']:
            # Excel文件处理
            df = read_excel_file(file_path)
            return load_dataframe_to_table(df, file_path, table_name)
        else:
            logger.error(f"不支持的文件类型: {file_extension}，文件: {file_path}")
            return False
            
    except pd.errors.EmptyDataError:
        logger.info(f"文件 {file_path} 为空或只有表头，无需加载数据。")
        return True
    except Exception as e:
        logger.error(f"处理文件 {file_path} 时发生意外错误", exc_info=True)
        return False

def run(table_name, update_mode='append', exec_date=None, target_type=None, 
        storage_location=None, schedule_frequency=None, script_name=None, is_manual_dag_trigger=False, **kwargs):
    """
    统一入口函数，支持通配符路径，处理并归档文件
    """
    if script_name is None:
        script_name = os.path.basename(__file__)

    # 修正之前的日志记录格式错误
    update_mode_str = '全量刷新' if update_mode == 'full_refresh' else '增量追加'
    logger.info(f"===== 开始执行 {script_name} ({update_mode_str}) =====")
    logger.info(f"表名: {table_name}")
    logger.info(f"更新模式: {update_mode}")
    logger.info(f"执行日期: {exec_date}")
    logger.info(f"目标类型: {target_type}")
    logger.info(f"资源类型: {target_type}, 文件相对路径模式: {storage_location}")
    logger.info(f"基准上传路径: {STRUCTURE_UPLOAD_BASE_PATH}")
    logger.info(f"基准归档路径: {STRUCTURE_UPLOAD_ARCHIVE_BASE_PATH}")
    logger.info(f"更新频率: {schedule_frequency}")
    logger.info(f"是否手工触发 manual DAG: {is_manual_dag_trigger}")
    
    # 记录其他参数
    for key, value in kwargs.items():
        logger.info(f"其他参数 - {key}: {value}")
    
    # 检查必要参数
    if not storage_location:
        logger.error("未提供 storage_location (文件查找路径模式)")
        return False
    if not STRUCTURE_UPLOAD_BASE_PATH:
         logger.error("配置错误: STRUCTURE_UPLOAD_BASE_PATH 未设置")
         return False
    if not STRUCTURE_UPLOAD_ARCHIVE_BASE_PATH:
         logger.error("配置错误: STRUCTURE_UPLOAD_ARCHIVE_BASE_PATH 未设置")
         return False

    # 记录执行开始时间
    overall_start_time = datetime.now()
    
    # 构建完整搜索路径
    # 使用 os.path.normpath 确保路径分隔符正确
    # 如果storage_location以斜杠开头，移除开头的斜杠以避免被当作绝对路径处理
    if storage_location.startswith('/'):
        storage_location = storage_location.lstrip('/')
        logger.info(f"检测到storage_location以斜杠开头，已移除: {storage_location}")
    
    # 检查storage_location是否包含扩展名
    has_extension = bool(re.search(r'\.[a-zA-Z0-9]+$', storage_location))
    
    full_search_patterns = []
    if has_extension:
        # 如果指定了扩展名，使用原始模式
        full_search_patterns.append(os.path.normpath(os.path.join(STRUCTURE_UPLOAD_BASE_PATH, storage_location)))
    else:
        # 如果没有指定扩展名，自动添加所有支持的扩展名
        base_pattern = storage_location.rstrip('/')
        if base_pattern.endswith('*'):
            # 如果已经以*结尾，添加扩展名
            for ext in ['.csv', '.xlsx', '.xls']:
                pattern = os.path.normpath(os.path.join(STRUCTURE_UPLOAD_BASE_PATH, f"{base_pattern}{ext}"))
                full_search_patterns.append(pattern)
        else:
            # 如果不以*结尾，添加/*.扩展名
            if not base_pattern.endswith('/*'):
                base_pattern = f"{base_pattern}/*"
            for ext in ['.csv', '.xlsx', '.xls']:
                pattern = os.path.normpath(os.path.join(STRUCTURE_UPLOAD_BASE_PATH, f"{base_pattern}{ext}"))
                full_search_patterns.append(pattern)
    
    logger.info(f"完整文件搜索模式: {full_search_patterns}")
    
    # 查找匹配的文件
    found_files = []
    for search_pattern in full_search_patterns:
        # 检查目录是否存在
        search_dir = os.path.dirname(search_pattern)
        if not os.path.exists(search_dir):
            logger.warning(f"搜索目录不存在: {search_dir}")
            continue
            
        try:
            # 查找匹配的文件
            matching_files = glob.glob(search_pattern, recursive=False)
            if matching_files:
                found_files.extend(matching_files)
                logger.info(f"在模式 {search_pattern} 下找到 {len(matching_files)} 个文件")
        except Exception as glob_err:
            logger.error(f"查找文件时发生错误 (模式: {search_pattern}): {str(glob_err)}")
            # 继续查找其他模式
    
    if not found_files:
        logger.warning(f"使用搜索模式 {full_search_patterns} 未找到任何匹配文件")
        return True  # 找不到文件视为正常情况，返回成功

    found_files = list(set(found_files))  # 去重
    logger.info(f"总共找到 {len(found_files)} 个匹配文件: {found_files}")

    # 检查是否开启ETL幂等性
    try:
        config = __import__('config')
        enable_idempotency = getattr(config, 'ENABLE_ETL_IDEMPOTENCY', False)
    except ImportError:
        logger.warning("无法导入config模块获取幂等性开关，默认为False")
        enable_idempotency = False
    
    logger.info(f"ETL幂等性开关状态: {enable_idempotency}")
    
    # 如果开启了ETL幂等性处理
    if enable_idempotency:
        # 处理append模式
        if update_mode.lower() == 'append':
            logger.info("当前为append模式，且ETL幂等性开关被打开")
            conn = None
            cursor = None
            try:
                conn = get_pg_conn()
                cursor = conn.cursor()
                
                if is_manual_dag_trigger:
                    logger.info("manual dag被手工触发")
                    # 计算日期范围
                    try:
                        start_date, end_date = script_utils.get_date_range(exec_date, schedule_frequency)
                        logger.info(f"计算得到的日期范围: start_date={start_date}, end_date={end_date}")
                        
                        # 尝试获取目标表的日期列
                        logger.info(f"查询表 {table_name} 的target_dt_column...")
                        target_dt_query = """
                            SELECT target_dt_column 
                            FROM {}.data_transform_scripts 
                            WHERE target_table = %s 
                            LIMIT 1
                        """.format(SCHEDULE_TABLE_SCHEMA)
                        cursor.execute(target_dt_query, (table_name,))
                        result = cursor.fetchone()
                        
                        target_dt_column = result[0] if result and result[0] else None
                        
                        if target_dt_column:
                            logger.info(f"找到目标日期列 {target_dt_column}，将生成DELETE语句")
                            # 生成DELETE语句
                            delete_sql = f"""DELETE FROM {table_name}
    WHERE {target_dt_column} >= '{start_date}'
    AND {target_dt_column} < '{end_date}';"""
                            
                            logger.info(f"生成的DELETE语句: {delete_sql}")
                            
                            # 执行DELETE操作
                            cursor.execute(delete_sql)
                            affected_rows = cursor.rowcount
                            conn.commit()
                            logger.info(f"清理SQL执行成功，删除了 {affected_rows} 行数据")
                        else:
                            logger.warning(f"目标表 {table_name} 没有设置目标日期列(target_dt_column)，无法生成DELETE语句实现幂等性")
                            logger.warning("将直接执行原始导入，可能导致数据重复")
                    except Exception as date_err:
                        logger.error(f"计算日期范围或执行DELETE时出错: {str(date_err)}", exc_info=True)
                        # 继续执行后续导入
                        logger.warning("继续执行原始导入")
                else:
                    logger.info(f"不是manual dag手工触发，对目标表 {table_name} create_time 进行数据清理")
                    
                    # 获取一天范围的日期
                    start_date, end_date = script_utils.get_one_day_range(exec_date)
                    logger.info(f"使用一天范围的日期: start_date={start_date}, end_date={end_date}")
                    
                    # 生成基于create_time的DELETE语句
                    delete_sql = f"""DELETE FROM {table_name}
    WHERE create_time >= '{start_date}'
    AND create_time < '{end_date}';"""
                    
                    logger.info(f"生成的基于create_time的DELETE语句: {delete_sql}")
                    
                    try:
                        # 执行DELETE操作
                        cursor.execute(delete_sql)
                        affected_rows = cursor.rowcount
                        conn.commit()
                        logger.info(f"基于create_time,清理SQL执行成功，删除了 {affected_rows} 行数据")
                    except Exception as del_err:
                        logger.error(f"基于create_time,清理SQL执行失败: {str(del_err)}", exc_info=True)
                        conn.rollback()
                        # 继续执行后续导入
                        logger.warning("继续执行原始导入")
            except Exception as e:
                logger.error(f"ETL幂等性处理出错: {str(e)}", exc_info=True)
                if conn and conn.status == psycopg2.extensions.STATUS_IN_TRANSACTION:
                    conn.rollback()
            finally:
                if cursor:
                    cursor.close()
                if conn:
                    conn.close()
        
        # 处理full_refresh模式
        elif update_mode.lower() == 'full_refresh':
            # 保持原有的full_refresh逻辑，不做修改
            logger.info("当前为full_refresh模式，将执行TRUNCATE操作")
            conn = None
            cursor = None
            try:
                conn = get_pg_conn()
                cursor = conn.cursor()
                # 假设表在 public schema，并为表名加引号
                logger.info(f"执行全量刷新，清空表 public.\"{table_name}\"")
                cursor.execute(f'TRUNCATE TABLE public.\"{table_name}\"')
                conn.commit()
                logger.info("表 public.\"" + table_name + "\" 已清空。")
            except Exception as e:
                logger.error("清空表 public.\"" + table_name + "\" 时出错: " + str(e))
                if conn:
                    conn.rollback()
                return False # 清空失败则直接失败退出
            finally:
                if cursor:
                     cursor.close()
                if conn:
                     conn.close()
        else:
            logger.info(f"当前更新模式 {update_mode} 不是append或full_refresh，不执行幂等性处理")
    else:
        # 如果未开启ETL幂等性处理，保持原有的full_refresh逻辑
        if update_mode == 'full_refresh':
            logger.info("ETL幂等性未开启，但当前为full_refresh模式，仍将执行TRUNCATE操作")
            conn = None
            cursor = None
            try:
                conn = get_pg_conn()
                cursor = conn.cursor()
                # 假设表在 public schema，并为表名加引号
                logger.info(f"执行全量刷新，清空表 public.\"{table_name}\"")
                cursor.execute(f'TRUNCATE TABLE public.\"{table_name}\"')
                conn.commit()
                logger.info("表 public.\"" + table_name + "\" 已清空。")
            except Exception as e:
                logger.error("清空表 public.\"" + table_name + "\" 时出错: " + str(e))
                if conn:
                    conn.rollback()
                return False # 清空失败则直接失败退出
            finally:
                if cursor:
                     cursor.close()
                if conn:
                     conn.close()
        else:
            logger.info("ETL幂等性未开启，直接执行文件加载")

    # 处理并归档每个找到的文件
    processed_files_count = 0

    for file_path in found_files:
        file_start_time = datetime.now()
        # 使用 normpath 统一路径表示
        normalized_file_path = os.path.normpath(file_path)
        logger.info(f"--- 开始处理文件: {os.path.basename(normalized_file_path)} ---")
        try:
            # 根据文件类型加载数据到表
            load_success = load_file_to_table(normalized_file_path, table_name, 'append')
            
            if load_success:
                logger.info(f"文件 {os.path.basename(normalized_file_path)} 加载成功。")
                processed_files_count += 1
                # 归档文件
                try:
                    # 计算相对路径部分
                    relative_dir = os.path.dirname(storage_location)
                    
                    # 获取当前日期
                    date_str = datetime.now().strftime('%Y-%m-%d')
                    
                    # 构建归档目录路径
                    archive_dir = os.path.normpath(os.path.join(STRUCTURE_UPLOAD_ARCHIVE_BASE_PATH, relative_dir, date_str))
                    
                    # 创建归档目录（如果不存在）
                    os.makedirs(archive_dir, exist_ok=True)
                    
                    # 获取当前unix时间戳并转换为字符串
                    unix_timestamp = int(datetime.now().timestamp())
                    
                    # 修改文件名，添加时间戳
                    original_filename = os.path.basename(normalized_file_path)
                    filename_parts = os.path.splitext(original_filename)
                    new_filename = f"{filename_parts[0]}_{unix_timestamp}{filename_parts[1]}"
                    
                    # 构建文件在归档目录中的最终路径
                    archive_dest_path = os.path.join(archive_dir, new_filename)
                    
                    # 移动文件
                    shutil.move(normalized_file_path, archive_dest_path)
                    logger.info(f"文件已成功移动到归档目录并重命名: {archive_dest_path}")
                    
                except Exception as move_err:
                    # 记录错误，但由于数据已加载，不将整体任务标记为失败
                    logger.error(f"加载文件 {os.path.basename(normalized_file_path)} 成功，但移动到归档目录时出错", exc_info=True)
                    logger.error(f"原始文件路径: {normalized_file_path}")
            else:
                logger.error(f"文件 {os.path.basename(normalized_file_path)} 加载失败，中止处理。")
                # 任何一个文件加载失败就直接返回 False
                overall_end_time = datetime.now()
                overall_duration = (overall_end_time - overall_start_time).total_seconds()
                logger.info(f"===== {script_name} 执行完成 (失败) =====")
                logger.info(f"总耗时: {overall_duration:.2f}秒")
                logger.info(f"共找到文件: {len(found_files)}")
                logger.info(f"成功处理文件数: {processed_files_count}")
                logger.error(f"首个失败文件: {os.path.basename(normalized_file_path)}")
                return False

        except Exception as file_proc_err:
            logger.error(f"处理文件 {os.path.basename(normalized_file_path)} 时发生意外错误", exc_info=True)
            # 任何一个文件处理异常就直接返回 False
            overall_end_time = datetime.now()
            overall_duration = (overall_end_time - overall_start_time).total_seconds()
            logger.info(f"===== {script_name} 执行完成 (异常) =====")
            logger.info(f"总耗时: {overall_duration:.2f}秒")
            logger.info(f"共找到文件: {len(found_files)}")
            logger.info(f"成功处理文件数: {processed_files_count}")
            logger.error(f"首个异常文件: {os.path.basename(normalized_file_path)}")
            return False
            
        finally:
            file_end_time = datetime.now()
            file_duration = (file_end_time - file_start_time).total_seconds()
            logger.info(f"--- 文件 {os.path.basename(normalized_file_path)} 处理结束，耗时: {file_duration:.2f}秒 ---")

    # 记录总体执行结果（全部成功的情况）
    overall_end_time = datetime.now()
    overall_duration = (overall_end_time - overall_start_time).total_seconds()
    
    logger.info(f"===== {script_name} 执行完成 (成功) =====")
    logger.info(f"总耗时: {overall_duration:.2f}秒")
    logger.info(f"共找到文件: {len(found_files)}")
    logger.info(f"成功处理文件数: {processed_files_count}")

    return True  # 所有文件处理成功

if __name__ == "__main__":
    # 直接执行时的测试代码
    parser = argparse.ArgumentParser(description='加载文件到表')
    parser.add_argument('--table', type=str, required=True, help='目标表名')
    parser.add_argument('--file', type=str, required=True, help='文件路径')
    parser.add_argument('--update-mode', type=str, default='append', choices=['append', 'full_refresh'], help='更新模式: append 或 full_refresh')
    
    args = parser.parse_args()
    
    # 构建参数字典
    run_args = {
        "table_name": args.table,
        "update_mode": args.update_mode,
        "storage_location": args.file
    }

    logger.info("命令行测试执行参数: " + str(run_args))

    success = run(**run_args)
    
    if success:
        print("文件加载任务执行完毕，所有文件处理成功。")
        sys.exit(0)
    else:
        print("文件加载任务执行完毕，但有部分或全部文件处理失败。")
        sys.exit(1)