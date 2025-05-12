# config.py

import os

# PostgreSQL 连接信息
PG_CONFIG = {
    "host": "192.168.67.10",
    "port": 5432,
    "user": "postgres",
    "password": "postgres",
    "database": "dataops",
}

# Neo4j 连接信息
NEO4J_CONFIG = {
    "uri": "bolt://192.168.67.1:7687",
    "user": "neo4j",
    "password": "Passw0rd",
}

# Airflow 自身配置（如果有需要，例如用 REST API 触发其他 DAG）
AIRFLOW_CONFIG = {
    "base_url": "http://localhost:8080",
    "username": "admin",
    "password": "admin",
}

# 任务重试配置
TASK_RETRY_CONFIG = {
    "retries": 2,  # 重试次数
    "retry_delay_minutes": 1  # 重试延迟（分钟）
}

# 脚本文件基础路径配置
# 部署到 Airflow 环境时使用此路径
AIRFLOW_BASE_PATH='/opt/airflow'
DATAOPS_DAGS_PATH = os.path.join(AIRFLOW_BASE_PATH, 'dags')
SCRIPTS_BASE_PATH = os.path.join(AIRFLOW_BASE_PATH, 'dataops_scripts')

# 上传的CSV/EXCEL文件的基准上传路径
STRUCTURE_UPLOAD_BASE_PATH ="/data/csv"
STRUCTURE_UPLOAD_ARCHIVE_BASE_PATH ="/data/archive"

# 本地开发环境脚本路径（如果需要区分环境）
# LOCAL_SCRIPTS_BASE_PATH = "/path/to/local/scripts"

# 执行计划保留的数量
EXECUTION_PLAN_KEEP_COUNT = 5

# ETL作业幂等性开关
ENABLE_ETL_IDEMPOTENCY = True

#dataops中的SCHEDULE相关的表 schema
SCHEDULE_TABLE_SCHEMA = "dags"