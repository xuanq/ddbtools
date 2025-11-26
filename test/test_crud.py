import pytest
from ddbtools import (
    BaseCRUD,
    Filter,
    Comparator,
    DBDf,
)
import pandas as pd
from datetime import date


class TestComparator:
    """测试Comparator枚举"""
    
    def test_comparator_values(self):
        """测试比较运算符值"""
        assert Comparator.eq.value == "="
        assert Comparator.gt.value == ">="
        assert Comparator.lt.value == "<="
        assert Comparator.like.value == "like"
        assert Comparator.isin.value == "in"


class TestFilter:
    """测试Filter类"""
    
    def test_filter_eq(self):
        """测试等于过滤条件"""
        # 字符串值
        filter1 = Filter(column="code", value="AAPL")
        assert filter1.clause == "code = 'AAPL'"
        
        # 数值
        filter2 = Filter(column="price", value=100)
        assert filter2.clause == "price = 100"
    
    def test_filter_gt(self):
        """测试大于等于过滤条件"""
        filter1 = Filter(column="price", comparator=Comparator.gt, value=100)
        assert filter1.clause == "price >= 100"
    
    def test_filter_lt(self):
        """测试小于等于过滤条件"""
        filter1 = Filter(column="price", comparator=Comparator.lt, value=200)
        assert filter1.clause == "price <= 200"
    
    def test_filter_like(self):
        """测试模糊匹配过滤条件"""
        # 单个值
        filter1 = Filter(column="code", comparator=Comparator.like, value="A")
        assert filter1.clause == "code like '%A%'"
        
        # 多个值
        filter2 = Filter(column="code", comparator=Comparator.like, value=["A", "B"])
        assert filter2.clause == "code like '%A%' or code like '%B%'"
    
    def test_filter_isin(self):
        """测试包含过滤条件"""
        filter1 = Filter(column="code", comparator=Comparator.isin, value=["AAPL", "MSFT"])
        assert filter1.clause == "code in ['AAPL', 'MSFT']"


class TestBaseCRUD:
    """测试BaseCRUD类"""
    
    # 定义测试CRUD类
    class TestCRUD(BaseCRUD):
        key_cols = ["code","date"]
    
    def test_upsert(self, session, test_db, test_table):
        """测试插入或更新数据"""
        # 创建测试数据
        data = pd.DataFrame({
            "date": [pd.to_datetime("2023-01-01"), pd.to_datetime("2023-01-02")],
            "code": ["AAPL", "MSFT"],
            "price": [150.0, 200.0],
            "volume": [1000000, 2000000]
        })
        data = DBDf(session, test_db, test_table, data)
        # 创建CRUD实例
        crud = self.TestCRUD(test_db, test_table)
        
        # 插入数据
        crud.upsert(session, data)
        
        # 验证数据已插入
        table = session.loadTable(dbPath=test_db, tableName=test_table)
        assert len(table.toDF()) == 2
    
    def test_get(self, session, test_db, test_table):
        """测试查询数据"""
        # 先插入测试数据
        data = pd.DataFrame({
            "date": [pd.Timestamp("2023-01-01"), pd.Timestamp("2023-01-02")],
            "code": ["AAPL", "MSFT"],
            "price": [150.0, 200.0],
            "volume": [1000000, 2000000]
        })
        data = DBDf(session, test_db, test_table, data)
        crud = self.TestCRUD(test_db, test_table)
        crud.upsert(session, data)
        
        # 查询所有数据
        result = crud.get(session)
        assert len(result) == 2
        
        # 使用过滤条件查询
        filter1 = Filter(column="code", value="AAPL")
        result = crud.get(session, conds=filter1)
        assert len(result) == 1
        assert result.iloc[0]["code"] == "AAPL"
    
    def test_delete(self, session, test_db, test_table):
        """测试删除数据"""
        # 先插入测试数据
        data = pd.DataFrame({
            "date": [date(2023, 1, 1), date(2023, 1, 2)],
            "code": ["AAPL", "MSFT"],
            "price": [150.0, 200.0],
            "volume": [1000000, 2000000]
        })
        data = DBDf(session, test_db, test_table, data)
        crud = self.TestCRUD(test_db, test_table)
        crud.upsert(session, data)
        
        # 删除数据
        crud.delete(session, code="`AAPL")
        
        # 验证数据已删除
        result = crud.get(session)
        assert len(result) == 1
        assert result.iloc[0]["code"] == "MSFT"


class TestDBDf:
    """测试DBDf类"""
    
    def test_dbdf_init(self, session, test_db, test_table):
        """测试DBDf初始化"""
        # 创建DBDf实例
        dbdf = DBDf(session, test_db, test_table)
        
        # 验证列名正确
        expected_columns = ["date", "code", "price", "volume"]
        assert list(dbdf.columns) == expected_columns
        
        # 验证数据类型映射正确
        assert "column_names_types" in dbdf.attrs
        assert dbdf.attrs["column_names_types"]["date"] == "datetime64"
        assert dbdf.attrs["column_names_types"]["code"] == "object"
        assert dbdf.attrs["column_names_types"]["price"] == "float64"
        assert dbdf.attrs["column_names_types"]["volume"] == "Int64"
    
    def test_dbdf_with_data(self, session, test_db, test_table):
        """测试带数据的DBDf"""
        # 准备测试数据
        data = {
            "date": ["2023-01-01", "2023-01-02"],
            "code": ["AAPL", "MSFT"],
            "price": [150.0, 200.0],
            "volume": [1000000, 2000000]
        }
        
        # 创建DBDf实例
        dbdf = DBDf(session, test_db, test_table, data)
        
        # 验证数据已正确加载
        assert len(dbdf) == 2
        
        # 验证数据类型已正确转换
        assert pd.api.types.is_datetime64_dtype(dbdf["date"])
        assert dbdf["code"].dtype == "object"
        assert dbdf["price"].dtype == "float64"
        assert dbdf["volume"].dtype == "Int64"
