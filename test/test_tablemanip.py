import pytest
import pandas as pd
from ddbtools import (
    create_table,
    get_table_info,
    get_table_columns,
    get_all_tables,
    DbColumn,
)


class TestTableManip:
    """测试表操作功能"""
    
    def test_create_table(self, session, test_db):
        """测试创建表"""
        test_table_name = "test_create_table"
        try:
            # 定义表结构
            columns = [
                DbColumn(name="date", dtype="DATE", comment="日期", compress="delta"),
                DbColumn(name="code", dtype="SYMBOL", comment="代码"),
                DbColumn(name="price", dtype="DOUBLE", comment="价格"),
            ]
            
            # 创建表
            result = create_table(
                session, 
                test_db, 
                test_table_name, 
                columns, 
                partition_by="date, code",
                sortColumns="`code,`date",
                keepDuplicates="LAST"
            )
            assert "成功" in result
            
            # 验证表存在
            assert session.run(f"existsTable('{test_db}',`{test_table_name});")
        finally:
            # 清理测试表
            session.run(f'drop table "{test_db}"."{test_table_name}"')
    
    def test_get_table_info(self, session, test_db, test_table):
        """测试获取表信息"""
        # 获取表信息
        table_info = get_table_info(session, test_db, test_table)
        
        # 验证返回结果是Series
        assert isinstance(table_info, type(pd.Series()))
        
        # 验证返回结果包含必要信息
        assert table_info["db_name"] == test_db
        assert table_info["table_name"] == test_table
        assert "partition_columns" in table_info
        assert "sort_columns" in table_info
    
    def test_get_table_columns(self, session, test_db, test_table):
        """测试获取表列信息"""
        # 获取表列信息
        table_columns = get_table_columns(session, test_db, test_table)
        
        # 验证返回结果是DataFrame
        assert isinstance(table_columns, type(pd.DataFrame()))
        
        # 验证表列包含必要信息
        assert "date" in table_columns.index
        assert "code" in table_columns.index
        assert "price" in table_columns.index
        assert "volume" in table_columns.index
    
    def test_get_all_tables(self, session, test_db, test_table):
        """测试获取所有表"""
        # 获取所有表
        all_tables = get_all_tables(session, test_db)
        
        # 验证测试表在结果中
        assert test_table in all_tables


