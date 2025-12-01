import numpy as np
import pandas as pd
import dolphindb as ddb
from ddbtools.log import logger
from typing import Dict
from pathlib import Path

# 创建数据库
def create_db(
    session: ddb.Session, dbname: str, partition_plan: str, engine: str = "TSDB"
):
    if not session.existsDatabase(dbname):
        CREATE_DATABASE_SCRIPT = f"""
            create database "{dbname}" 
            partitioned by {partition_plan}, 
            engine='{engine}'
            """
        session.run(CREATE_DATABASE_SCRIPT)
        logger.info(f"数据库 {dbname} 创建成功")
        return f"数据库 {dbname} 创建成功"
    else:
        logger.info(f"数据库 {dbname} 已存在, 跳过")
        return f"数据库 {dbname} 已存在, 跳过"


# 删除数据库
def delete_db(session: ddb.Session, dbname: str, force: bool = False) -> str:
    """删除数据库
    
    Args:
        session: DolphinDB 会话对象
        dbname: 数据库名称
        force: 是否强制删除，默认 False
        
    Returns:
        删除结果消息
    """
    if session.existsDatabase(dbname):
        DELETE_DATABASE_SCRIPT = f"drop database {dbname}{' force' if force else ''}"
        session.run(DELETE_DATABASE_SCRIPT)
        logger.info(f"数据库 {dbname} 删除成功")
        return f"数据库 {dbname} 删除成功"
    else:
        logger.info(f"数据库 {dbname} 不存在，无法删除")
        return f"数据库 {dbname} 不存在，无法删除"


# 获取数据库信息
def get_db_info(session: ddb.Session, dbname: str):
    session.database("db", dbPath=dbname)
    return session.run("schema(db)")

def backup_db(
    session: ddb.Session, 
    dbname: str, 
    backup_path: str, 
    backup_type: str = "full",  # full 或 incremental
    comment: str = None
) -> str:
    """备份数据库
    
    Args:
        session: DolphinDB 会话对象
        dbname: 数据库名称
        backup_path: 备份路径
        backup_type: 备份类型，full 或 incremental
        comment: 备份注释
        
    Returns:
        备份结果消息
    """
    if not session.existsDatabase(dbname):
        logger.info(f"数据库 {dbname} 不存在，无法备份")
        return f"数据库 {dbname} 不存在，无法备份"
    
    # 设置数据库句柄
    session.database("db", dbPath=dbname)
    
    # 构建备份脚本
    backup_type_upper = backup_type.upper()
    if backup_type_upper not in ["FULL", "INCREMENTAL"]:
        raise ValueError(f"不支持的备份类型: {backup_type}，只支持 full 或 incremental")
    
    comment_str = f", \"{comment}\"" if comment else ""
    BACKUP_DATABASE_SCRIPT = f"backupDatabase(db, \"{backup_path}\", \"{backup_type_upper}\"{comment_str})"
    
    session.run(BACKUP_DATABASE_SCRIPT)
    logger.info(f"数据库 {dbname} 备份成功，备份路径: {backup_path}，备份类型: {backup_type}")
    return f"数据库 {dbname} 备份成功，备份路径: {backup_path}，备份类型: {backup_type}"


def restore_db(
    session: ddb.Session, 
    dbname: str, 
    backup_path: str, 
    target_dbname: str = None,
    restore_type: str = "full"  # full 或 incremental
) -> str:
    """恢复数据库
    
    Args:
        session: DolphinDB 会话对象
        dbname: 原数据库名称
        backup_path: 备份路径
        target_dbname: 目标数据库名称，默认与原数据库名称相同
        restore_type: 恢复类型，full 或 incremental
        
    Returns:
        恢复结果消息
    """
    # 验证备份路径是否存在
    if not session.run(f"existsDirectory('{backup_path}')"):
        logger.info(f"备份路径 {backup_path} 不存在，无法恢复")
        return f"备份路径 {backup_path} 不存在，无法恢复"
    
    # 设置目标数据库名称
    target_db = target_dbname if target_dbname else dbname
    
    # 验证恢复类型
    restore_type_upper = restore_type.upper()
    if restore_type_upper not in ["FULL", "INCREMENTAL"]:
        raise ValueError(f"不支持的恢复类型: {restore_type}，只支持 full 或 incremental")
    
    # 构建恢复脚本
    RESTORE_DATABASE_SCRIPT = f"restoreDatabase('{dbname}', '{backup_path}', '{target_db}', '{restore_type_upper}')"
    
    session.run(RESTORE_DATABASE_SCRIPT)
    logger.info(f"数据库 {dbname} 从 {backup_path} 恢复成功，目标数据库: {target_db}，恢复类型: {restore_type}")
    return f"数据库 {dbname} 从 {backup_path} 恢复成功，目标数据库: {target_db}，恢复类型: {restore_type}"


def migrate_db(
    source_session: ddb.Session, 
    source_dbname: str, 
    target_session: ddb.Session, 
    target_dbname: str = None,
    overwrite: bool = False
) -> str:
    """迁移数据库
    
    Args:
        source_session: 源 DolphinDB 会话对象
        source_dbname: 源数据库名称
        target_session: 目标 DolphinDB 会话对象
        target_dbname: 目标数据库名称，默认与源数据库名称相同
        overwrite: 如果目标数据库已存在，是否覆盖
        
    Returns:
        迁移结果消息
    """
    # 验证源数据库是否存在
    if not source_session.existsDatabase(source_dbname):
        logger.info(f"源数据库 {source_dbname} 不存在，无法迁移")
        return f"源数据库 {source_dbname} 不存在，无法迁移"
    
    # 设置目标数据库名称
    target_db = target_dbname if target_dbname else source_dbname
    
    # 处理目标数据库已存在的情况
    if target_session.existsDatabase(target_db):
        if overwrite:
            # 删除目标数据库
            target_session.run(f"drop database {target_db} force")
            logger.info(f"已删除目标数据库 {target_db}，准备重新创建")
        else:
            logger.info(f"目标数据库 {target_db} 已存在，且未设置 overwrite=True，无法迁移")
            return f"目标数据库 {target_db} 已存在，且未设置 overwrite=True，无法迁移"
    
    # 获取源数据库结构
    source_session.database("source_db", dbPath=source_dbname)
    source_schema = source_session.run("schema(source_db)")
    
    # 提取数据库创建参数
    engine_type = source_schema["engineType"]
    partition_column_type = source_schema["partitionColumnType"]
    partition_type_name = source_schema["partitionTypeName"]
    partition_columns = source_schema["partitionColumns"]
    
    # 构建分区计划
    if partition_type_name == "RANGE":
        partition_plan = f"RANGE {partition_columns}"
    elif partition_type_name == "VALUE":
        partition_plan = f"VALUE {partition_columns}"
    elif partition_type_name == "LIST":
        partition_plan = f"LIST {partition_columns}"
    elif partition_type_name == "HASH":
        partition_plan = f"HASH {partition_columns}"
    else:
        partition_plan = partition_columns
    
    # 在目标服务器创建数据库
    CREATE_DATABASE_SCRIPT = f"create database \"{target_db}\" partitioned by {partition_plan}, engine='{engine_type}'"
    target_session.run(CREATE_DATABASE_SCRIPT)
    logger.info(f"已在目标服务器创建数据库 {target_db}")
    
    # 获取源数据库中的所有表
    source_tables = source_session.run("source_db.getTables()")
    
    # 迁移每个表
    for table_name in source_tables:
        # 获取表结构
        source_session.table(source_dbname, table_name, "source_table")
        table_schema = source_session.run("schema(source_table)")
        
        # 构建创建表的脚本
        col_defs = []
        for _, col in table_schema["colDefs"].iterrows():
            col_def = f"{col['name']} {col['typeString']}"
            # 添加注释和压缩方式
            options = []
            if col.get('comment'):
                options.append(f"comment=\"{col['comment']}\"")
            if col.get('compressMethod'):
                options.append(f"compress=\"{col['compressMethod']}\"")
            if options:
                col_def += f"[{', '.join(options)}]"
            col_defs.append(col_def)
        
        # 构建表创建脚本
        CREATE_TABLE_SCRIPT = f"create table \"{target_db}\"\".\"{table_name}\"(\n    {',\n    '.join(col_defs)}\n)"
        
        # 添加分区信息
        if table_schema.get('partitionColumnName'):
            CREATE_TABLE_SCRIPT += f"\npartitioned by {table_schema['partitionColumnName']},"
        
        # 添加排序信息
        if table_schema.get('sortColumns'):
            CREATE_TABLE_SCRIPT += f"\nsortColumns=[{table_schema['sortColumns']}],"
        
        # 添加重复值处理
        if table_schema.get('keepDuplicates'):
            CREATE_TABLE_SCRIPT += f"\nkeepDuplicates={table_schema['keepDuplicates']},"
        
        # 执行表创建
        target_session.run(CREATE_TABLE_SCRIPT)
        logger.info(f"已在目标数据库 {target_db} 中创建表 {table_name}")
        
        # 迁移数据
        # 注意：这里使用了简单的 select * 方式迁移数据，对于大数据集可能需要优化
        MIGRATE_DATA_SCRIPT = f"insert into \"{target_db}\"\".\"{table_name}\" select * from \"{source_dbname}\"\".\"{table_name}\""
        source_session.run(MIGRATE_DATA_SCRIPT)
        logger.info(f"已将表 {table_name} 的数据从 {source_dbname} 迁移到 {target_db}")
    
    logger.info(f"数据库 {source_dbname} 已成功迁移到目标服务器，目标数据库名称: {target_db}")
    return f"数据库 {source_dbname} 已成功迁移到目标服务器，目标数据库名称: {target_db}"


def get_all_dbs(session: ddb.Session):
    all_dbs = session.run("getAllDBs()").keys()
    db_schemas = []
    for db in all_dbs:
        session.database("db", dbPath=f"dfs:/{db}")
        db_schemas.append(session.run(f"schema(db)"))
    db_schemas = pd.DataFrame(
        [
            {
                "dbpath": schema["databaseDir"],
                "engineType": schema["engineType"],
                "partitionPlan": schema["partitionColumnType"],
                "partitiontype": schema["partitionTypeName"],
            }
            for schema in db_schemas
        ]
    )
    dtype_mapping = (
        pd.read_csv(
            f"{Path(__file__).parent}/dolphindb_dtype.csv",
        )
        .set_index("ID")["名称"]
        .to_dict()
    )

    def map_dtype(x, dtype_mapping: Dict):
        if isinstance(x, int):
            return dtype_mapping.get(str(x), x)
        if isinstance(x, np.ndarray):
            return [dtype_mapping.get(str(y), y) for y in x]

    db_schemas["partitionPlan"] = db_schemas["partitionPlan"].apply(
        map_dtype, dtype_mapping=dtype_mapping
    )

    return db_schemas
