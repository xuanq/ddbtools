import pytest
import dolphindb as ddb
from ddbtools import (
    create_db,
    create_table,
    DbColumn,
)

# 测试配置
TEST_DB_NAME = "dfs://test_ddbtools"
TEST_TABLE_NAME = "test_table"


@pytest.fixture(scope="session")
def session():
    """创建DolphinDB会话"""
    # 连接DolphinDB服务器
    sess = ddb.Session()
    sess.connect("localhost", 8848, "admin", "123456")
    yield sess
    # 关闭会话
    sess.close()


@pytest.fixture(scope="session")
def test_db(session):
    """创建测试数据库"""
    # 创建测试数据库
    create_db(session, TEST_DB_NAME, "RANGE(date(datetimeAdd(1990.01M,0..80*12,'M'))), VALUE(`f1`f2)", engine="TSDB")
    yield TEST_DB_NAME
    # 清理测试数据库
    session.run(f'drop database "{TEST_DB_NAME}"')


@pytest.fixture(scope="session")
def test_table(session, test_db):
    """创建测试表"""
    # 定义表结构
    columns = [
        DbColumn(name="date", dtype="DATE", comment="日期", compress="delta"),
        DbColumn(name="code", dtype="SYMBOL", comment="代码"),
        DbColumn(name="price", dtype="DOUBLE", comment="价格"),
        DbColumn(name="volume", dtype="LONG", comment="成交量")
    ]
    
    # 创建测试表
    create_table(
        session, 
        test_db, 
        TEST_TABLE_NAME, 
        columns, 
        partition_by="date, code",
        sortColumns="`code,`date",
        keepDuplicates="LAST"
    )
    yield TEST_TABLE_NAME
    # 清理测试表
    session.run(f'drop table "{test_db}"."{TEST_TABLE_NAME}"')
