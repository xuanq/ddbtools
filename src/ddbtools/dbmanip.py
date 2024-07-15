import dolphindb as ddb
# 创建数据库
def create_db(session: ddb.Session,dbname:str,partition_plan:str,engine:str="TSDB"):
    if not session.existsDatabase(dbname):
        # log(f"数据库 {dbname} 不存在, 正在创建")
        CREATE_DATABASE_SCRIPT = f"""
            create database "{dbname}" 
            partitioned by {partition_plan}, 
            engine='{engine}'
            """
        session.run(CREATE_DATABASE_SCRIPT)
        # log(f"数据库 {dbname} 创建成功")

# 删除数据库
def delete_db(session: ddb.Session,dbname:str):
    ...

# 获取数据库信息
def get_db_info(session: ddb.Session):
    ...

