from ddbtools import get_all_dbs,get_db_info
import dolphindb as ddb
import pandas as pd
# 配置 DolphinDB 连接
DOLPHINDB_HOST = "192.168.200.1"  # 使用服务名称作为主机名
DOLPHINDB_PORT = 8902
USER = "admin"  # 替换为你的用户名
PASSWORD = "123456"  # 替换为你的密码

# 创建 DolphinDB 连接
def get_db() -> ddb.Session:
    return ddb.Session(DOLPHINDB_HOST, DOLPHINDB_PORT, USER, PASSWORD)

if __name__ == "__main__":
    session = get_db()
    print(get_all_dbs(session))
    print(get_db_info(session,"dfs://infos"))