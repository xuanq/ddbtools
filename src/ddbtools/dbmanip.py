import dolphindb as ddb
from ddbtools.log import get_logger
logger = get_logger(__name__)

# 创建数据库
def create_db(session: ddb.Session,dbname:str,partition_plan:str,engine:str="TSDB"):
    if not session.existsDatabase(dbname):
        CREATE_DATABASE_SCRIPT = f"""
            create database "{dbname}" 
            partitioned by {partition_plan}, 
            engine='{engine}'
            """
        session.run(CREATE_DATABASE_SCRIPT)
        logger.debug(f"数据库 {dbname} 创建成功")
    else:
        logger.debug(f"数据库 {dbname} 已存在, 跳过")

# 删除数据库
def delete_db(session: ddb.Session,dbname:str):
    ...

# 获取数据库信息
def get_db_info(session: ddb.Session):
    ...

