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
def delete_db(session: ddb.Session, dbname: str): ...


# 获取数据库信息
def get_db_info(session: ddb.Session, dbname: str):
    session.database("db", dbPath=dbname)
    return session.run("schema(db)")

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
            encoding="GBK",
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
