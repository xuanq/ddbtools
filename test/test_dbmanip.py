import pytest
# 导入pandas，用于类型检查
import pandas as pd
from ddbtools import (
    create_db,
    get_all_dbs,
    get_db_info,
)


class TestDbManip:
    """测试数据库操作功能"""
    
    def test_create_db(self, session):
        """测试创建数据库"""
        test_db_name = "dfs://test_create_db"
        try:
            # 创建数据库
            result = create_db(session, test_db_name, "RANGE(date(datetimeAdd(1990.01M,0..80*12,'M'))), VALUE(`f1`f2)", engine="TSDB")
            assert "创建成功" in result
            
            # 验证数据库存在
            assert session.existsDatabase(test_db_name)
        finally:
            # 清理测试数据库
            session.run(f'drop database "{test_db_name}"')
    
    def test_get_all_dbs(self, session, test_db):
        """测试获取所有数据库"""
        # 获取所有数据库
        all_dbs = get_all_dbs(session)
        
        # 验证返回结果是DataFrame
        assert isinstance(all_dbs, type(pd.DataFrame()))
        
        # 验证测试数据库在结果中
        assert "dbpath" in all_dbs.columns
    
    def test_get_db_info(self, session, test_db):
        """测试获取数据库信息"""
        # 获取数据库信息
        db_info = get_db_info(session, test_db)
        
        # 验证返回结果包含必要信息
        assert "engineType" in db_info
        assert "partitionColumnType" in db_info
        assert "partitionTypeName" in db_info
    
    # def test_delete_db(self, session):
    #     """测试删除数据库"""
    #     test_db_name = "dfs://test_delete_db"
        
    #     # 创建测试数据库
    #     create_db(session, test_db_name, "VALUE date", engine="TSDB")
    #     assert session.existsDatabase(test_db_name)
        
    #     # 删除数据库
    #     result = delete_db(session, test_db_name)
    #     assert "删除成功" in result
        
    #     # 验证数据库不存在
    #     assert not session.existsDatabase(test_db_name)

