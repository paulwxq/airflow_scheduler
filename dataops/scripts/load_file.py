#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
import sys
import os
import pandas as pd
import psycopg2
from datetime import datetime
import csv
import glob
import shutil

# 配置日志记录器
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger("load_file")

# 添加健壮的导入机制
def get_config():
    """
    从config模块导入配置
    
    返回:
        tuple: (PG_CONFIG, STRUCTURE_UPLOAD_BASE_PATH, STRUCTURE_UPLOAD_ARCHIVE_BASE_PATH)
    """
    # 默认配置
    default_pg_config = {
        "host": "localhost",
        "port": 5432,
        "user": "postgres",
        "password": "postgres",
        "database": "dataops",
    }
    default_upload_path = '/tmp/uploads'
    default_archive_path = '/tmp/uploads/archive'
    
    try:
        # 动态导入，避免IDE警告
        config = __import__('config')
        logger.info("从config模块直接导入配置")
        pg_config = getattr(config, 'PG_CONFIG', default_pg_config)
        upload_path = getattr(config, 'STRUCTURE_UPLOAD_BASE_PATH', default_upload_path)
        archive_path = getattr(config, 'STRUCTURE_UPLOAD_ARCHIVE_BASE_PATH', default_archive_path)
        return pg_config, upload_path, archive_path
    except ImportError:
        # 使用默认配置
        logger.warning("无法导入config模块，使用默认值")
        return default_pg_config, default_upload_path, default_archive_path

# 导入配置
PG_CONFIG, STRUCTURE_UPLOAD_BASE_PATH, STRUCTURE_UPLOAD_ARCHIVE_BASE_PATH = get_config()
logger.info(f"配置加载完成: 上传路径={STRUCTURE_UPLOAD_BASE_PATH}, 归档路径={STRUCTURE_UPLOAD_ARCHIVE_BASE_PATH}")

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
        for row in cursor.fetchall():
            col_name = row[0]
            col_comment = row[1] if row[1] else col_name  # 如果注释为空，使用列名
            columns[col_name] = col_comment
            
        if not columns:
             logger.warning(f"未能获取到表 '{table_name}' 的列信息，请检查表是否存在、schema是否正确以及权限。")

        return columns
    except Exception as e:
        logger.error(f"获取表 '{table_name}' 的列信息时出错: {str(e)}")
        return {}
    finally:
        cursor.close()
        conn.close()

def match_csv_columns(csv_headers, table_columns):
    """
    匹配CSV列名与表列名
    
    策略:
    1. 尝试通过表字段注释匹配CSV列名 (忽略大小写和空格)
    2. 尝试通过名称直接匹配 (忽略大小写和空格)
    
    参数:
        csv_headers (list): CSV文件的列名列表
        table_columns (dict): {数据库列名: 列注释} 的字典
    
    返回:
        dict: {CSV列名: 数据库列名} 的映射字典
    """
    mapping = {}
    matched_table_cols = set()

    # 数据库列名通常不区分大小写（除非加引号），注释可能区分
    # 为了匹配更健壮，我们将CSV和数据库列名/注释都转为小写处理
    processed_table_columns_lower = {col.lower(): col for col in table_columns.keys()}
    processed_comment_to_column_lower = {
        str(comment).lower(): col
        for col, comment in table_columns.items() if comment
    }

    # 预处理 CSV headers
    processed_csv_headers_lower = {str(header).lower(): header for header in csv_headers}

    # 1. 通过注释匹配 (忽略大小写)
    for processed_header, original_header in processed_csv_headers_lower.items():
        if processed_header in processed_comment_to_column_lower:
            table_col_original_case = processed_comment_to_column_lower[processed_header]
            if table_col_original_case not in matched_table_cols:
                mapping[original_header] = table_col_original_case
                matched_table_cols.add(table_col_original_case)
                logger.info(f"通过注释匹配: CSV 列 '{original_header}' -> 表列 '{table_col_original_case}'")

    # 2. 通过名称直接匹配 (忽略大小写)，仅匹配尚未映射的列
    for processed_header, original_header in processed_csv_headers_lower.items():
         if original_header not in mapping: # 仅当此 CSV 列尚未映射时才进行名称匹配
            if processed_header in processed_table_columns_lower:
                table_col_original_case = processed_table_columns_lower[processed_header]
                if table_col_original_case not in matched_table_cols:
                    mapping[original_header] = table_col_original_case
                    matched_table_cols.add(table_col_original_case)
                    logger.info(f"通过名称匹配: CSV 列 '{original_header}' -> 表列 '{table_col_original_case}'")

    unmapped_csv = [h for h in csv_headers if h not in mapping]
    if unmapped_csv:
         logger.warning(f"以下 CSV 列未能匹配到表列: {unmapped_csv}")

    unmapped_table = [col for col in table_columns if col not in matched_table_cols]
    if unmapped_table:
        logger.warning(f"以下表列未能匹配到 CSV 列: {unmapped_table}")

    return mapping

def load_csv_to_table(csv_file, table_name, execution_mode='append'):
    """
    将单个CSV文件数据加载到目标表
    
    参数:
        csv_file (str): CSV文件路径
        table_name (str): 目标表名 (大小写可能敏感，取决于数据库)
        execution_mode (str): 执行模式，'append'或'full_refresh'
    
    返回:
        bool: 成功返回True，失败返回False
    """
    conn = None
    cursor = None # 初始化 cursor
    logger.info(f"开始处理文件: {csv_file}")
    try:
        # 读取CSV文件，尝试自动检测编码
        try:
            # 使用 dtype=str 确保所有列按字符串读取，避免类型推断问题，特别是对于ID类字段
            df = pd.read_csv(csv_file, encoding='utf-8', keep_default_na=False, na_values=[r'\N', '', 'NULL', 'null'], dtype=str)
        except UnicodeDecodeError:
            try:
                logger.warning(f"UTF-8 读取失败，尝试 GBK: {csv_file}")
                df = pd.read_csv(csv_file, encoding='gbk', keep_default_na=False, na_values=[r'\N', '', 'NULL', 'null'], dtype=str)
            except UnicodeDecodeError:
                logger.warning(f"GBK 读取也失败，尝试 latin1: {csv_file}")
                df = pd.read_csv(csv_file, encoding='latin1', keep_default_na=False, na_values=[r'\N', '', 'NULL', 'null'], dtype=str)
        except Exception as read_err:
             logger.error(f"读取 CSV 文件 {csv_file} 时发生未知错误: {str(read_err)}")
             return False

        logger.info(f"成功读取CSV文件: {os.path.basename(csv_file)}, 共 {len(df)} 行")
        
        # 清理列名中的潜在空白符
        df.columns = df.columns.str.strip()

        # 如果CSV为空，则直接认为成功并返回
        if df.empty:
             logger.info(f"CSV 文件 {csv_file} 为空，无需加载数据。")
             return True

        # 获取CSV列名 (清理后)
        csv_headers = df.columns.tolist()
        logger.info(f"清理后的 CSV 列名: {csv_headers}")
        
        # 获取表结构
        table_columns = get_table_columns(table_name)
        if not table_columns:
            logger.error(f"无法获取表 '{table_name}' 的列信息，跳过文件 {csv_file}")
            return False
        
        logger.info(f"表 '{table_name}' 的列信息 (列名: 注释): {table_columns}")
        
        # 匹配CSV列与表列
        column_mapping = match_csv_columns(csv_headers, table_columns)
        logger.info(f"列映射关系 (CSV列名: 表列名): {column_mapping}")
        
        # 检查是否有任何列成功映射
        if not column_mapping:
            logger.error(f"文件 {csv_file} 的列无法与表 '{table_name}' 的列建立任何映射关系，跳过此文件。")
            return False # 如果一个都没匹配上，则认为失败
        
        # 仅选择成功映射的列进行加载
        mapped_csv_headers = list(column_mapping.keys())
        # 使用 .copy() 避免 SettingWithCopyWarning
        df_mapped = df[mapped_csv_headers].copy()
        df_mapped.rename(columns=column_mapping, inplace=True)
        logger.info(f"将加载以下映射后的列: {df_mapped.columns.tolist()}")

        # 将空字符串 '' 替换为 None，以便插入数据库时为 NULL
        # 使用 map 替代已废弃的 applymap 方法
        # 对每一列单独应用 map 函数
        for col in df_mapped.columns:
            df_mapped[col] = df_mapped[col].map(lambda x: None if isinstance(x, str) and x == '' else x)

        # 连接数据库
        conn = get_pg_conn()
        cursor = conn.cursor()
        
        # 根据执行模式确定操作 - 注意：full_refresh 在 run 函数层面控制，这里仅处理单个文件追加
        # if execution_mode == 'full_refresh':
        #     logger.warning(f"在 load_csv_to_table 中收到 full_refresh，但清空操作应在 run 函数完成。此处按 append 处理文件：{csv_file}")
            # # 如果是全量刷新，先清空表 - 这个逻辑移到 run 函数
            # logger.info(f"执行全量刷新，清空表 {table_name}")
            # cursor.execute(f"TRUNCATE TABLE {table_name}")
        
        # 构建INSERT语句
        # 使用原始大小写的数据库列名（从 column_mapping 的 value 获取）并加引号
        columns = ', '.join([f'"{col}"' for col in df_mapped.columns])
        placeholders = ', '.join(['%s'] * len(df_mapped.columns))
        # 假设表在 public schema，并为表名加引号以处理大小写或特殊字符
        insert_sql = f'INSERT INTO public."{table_name}" ({columns}) VALUES ({placeholders})'
        
        # 批量插入数据
        # df_mapped.values 会产生 numpy array，需要转换为 list of tuples
        # 确保 None 值正确传递
        rows = [tuple(row) for row in df_mapped.values]
        
        try:
            cursor.executemany(insert_sql, rows)
            conn.commit()
            logger.info(f"成功将文件 {os.path.basename(csv_file)} 的 {len(rows)} 行数据插入到表 '{table_name}'")
        except Exception as insert_err:
             logger.error(f"向表 '{table_name}' 插入数据时出错: {str(insert_err)}")
             logger.error(f"出错的 SQL 语句大致为: {insert_sql}")
             # 可以考虑记录前几行出错的数据 (注意隐私和日志大小)
             try:
                 logger.error(f"出错的前3行数据 (部分): {rows[:3]}")
             except: pass # 防御性编程
             conn.rollback() # 回滚事务
             return False # 插入失败则返回 False

        
        return True
    except pd.errors.EmptyDataError:
         logger.info(f"CSV 文件 {csv_file} 为空或只有表头，无需加载数据。")
         return True # 空文件视为成功处理
    except Exception as e:
        # 使用 exc_info=True 获取更详细的堆栈跟踪信息
        logger.error(f"处理文件 {csv_file} 加载到表 '{table_name}' 时发生意外错误", exc_info=True)
        if conn:
            conn.rollback()
        return False
    finally:
        if cursor:
             cursor.close()
        if conn:
             conn.close()

def run(table_name, execution_mode='append', exec_date=None, target_type=None, 
        storage_location=None, frequency=None, script_name=None, **kwargs):
    """
    统一入口函数，支持通配符路径，处理并归档文件
    """
    if script_name is None:
        script_name = os.path.basename(__file__)

    # 修正之前的日志记录格式错误
    exec_mode_str = '全量刷新' if execution_mode == 'full_refresh' else '增量追加'
    logger.info(f"===== 开始执行 {script_name} ({exec_mode_str}) =====")
    logger.info(f"表名: {table_name}")
    logger.info(f"执行模式: {execution_mode}")
    logger.info(f"执行日期: {exec_date}")
    logger.info(f"目标类型: {target_type}")
    logger.info(f"资源类型: {target_type}, 文件相对路径模式: {storage_location}")
    logger.info(f"基准上传路径: {STRUCTURE_UPLOAD_BASE_PATH}")
    logger.info(f"基准归档路径: {STRUCTURE_UPLOAD_ARCHIVE_BASE_PATH}")
    logger.info(f"更新频率: {frequency}")
    
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
    
    full_search_pattern = os.path.normpath(os.path.join(STRUCTURE_UPLOAD_BASE_PATH, storage_location))
    logger.info(f"完整文件搜索模式: {full_search_pattern}")
    
    # 检查路径是否存在（至少目录部分）
    search_dir = os.path.dirname(full_search_pattern)
    if not os.path.exists(search_dir):
        error_msg = f"错误: 搜索目录不存在: {search_dir}"
        logger.error(error_msg)
        raise FileNotFoundError(error_msg)  # 抛出异常而不是返回False
    
    # 查找匹配的文件
    try:
        # 增加 recursive=True 如果需要递归查找子目录中的文件 (例如 storage_location 是 a/b/**/*.csv)
        # 当前假设模式只在指定目录下匹配，例如 /data/subdir/*.csv
        found_files = glob.glob(full_search_pattern, recursive=False)
    except Exception as glob_err:
         logger.error(f"查找文件时发生错误 (模式: {full_search_pattern}): {str(glob_err)}")
         raise  # 重新抛出异常

    if not found_files:
        logger.warning(f"在目录 {search_dir} 下未找到匹配模式 '{os.path.basename(full_search_pattern)}' 的文件")
        return True  # 找不到文件视为正常情况，返回成功

    logger.info(f"找到 {len(found_files)} 个匹配文件: {found_files}")

    # 如果是全量刷新，在处理任何文件前清空表
    if execution_mode == 'full_refresh':
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

    # 处理并归档每个找到的文件
    processed_files_count = 0
    failed_files = []

    for file_path in found_files:
        file_start_time = datetime.now()
        # 使用 normpath 统一路径表示
        normalized_file_path = os.path.normpath(file_path)
        logger.info(f"--- 开始处理文件: {os.path.basename(normalized_file_path)} ---")
        try:
            # 加载CSV数据到表 (注意：full_refresh时也是append模式加载，因为表已清空)
            load_success = load_csv_to_table(normalized_file_path, table_name, 'append')
            
            if load_success:
                logger.info(f"文件 {os.path.basename(normalized_file_path)} 加载成功。")
                processed_files_count += 1
                # 归档文件
                try:
                    # 计算相对路径部分 (storage_location 可能包含子目录)
                    # 使用 os.path.dirname 获取 storage_location 的目录部分
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
                # 修改：任何一个文件加载失败就直接返回 False
                # 记录最终统计
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
            # 修改：任何一个文件处理异常就直接返回 False
            # 记录最终统计
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
    import argparse
    
    parser = argparse.ArgumentParser(description='从CSV文件加载数据到表（支持通配符）')
    parser.add_argument('--table', type=str, required=True, help='目标表名')
    parser.add_argument('--pattern', type=str, required=True, help='CSV文件查找模式 (相对于基准上传路径的相对路径，例如: data/*.csv 或 *.csv)')
    parser.add_argument('--mode', type=str, default='append', choices=['append', 'full_refresh'], help='执行模式: append 或 full_refresh')
    
    args = parser.parse_args()
    
    # 构造必要的 kwargs
    run_kwargs = {
        "table_name": args.table,
        "execution_mode": args.mode,
        "storage_location": args.pattern,
        "target_type": 'structure',
        "exec_date": datetime.now().strftime('%Y-%m-%d'),
        "frequency": "manual",
        "script_name": os.path.basename(__file__)
    }

    logger.info("命令行测试执行参数: " + str(run_kwargs))

    success = run(**run_kwargs)
    
    if success:
        print("CSV文件加载任务执行完毕，所有文件处理成功。")
        sys.exit(0)
    else:
        print("CSV文件加载任务执行完毕，但有部分或全部文件处理失败。")
        sys.exit(1)