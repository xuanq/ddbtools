# ddbtools

## 项目简介

`ddbtools` 是一个基于 DolphinDB 库的二次开发工具库，提供了便捷的数据库操作功能，简化了 DolphinDB 数据库的建库、建表、增删改查等操作。

## 核心功能

- **数据库操作**：创建数据库、获取数据库信息、获取所有数据库
- **表操作**：创建表、获取表信息、获取表列信息、获取所有表、创建维度表、创建属性表
- **CRUD 操作**：基于 `BaseCRUD` 类的增删改查功能，支持灵活的过滤条件
- **数据类型转换**：自动处理 DolphinDB 与 pandas 之间的数据类型转换
- **日志支持**：基于 loguru 库的日志记录

## 安装

### 依赖

- Python 3.10+
- DolphinDB 3.0.1.0+
- loguru 0.7.2+

### 安装方式

使用 pip 安装：

```bash
pip install ddbtools
```

或使用 poetry 安装：

```bash
poetry add ddbtools
```

## 快速开始

### 1. 连接 DolphinDB 服务器

```python
import dolphindb as ddb

# 创建 DolphinDB 会话
session = ddb.Session()
session.connect("localhost", 8848)
```

### 2. 数据库操作

```python
from ddbtools import create_db, get_all_dbs, get_db_info

# 创建数据库
create_db(session, "dfs://test_db", "VALUE date", engine="TSDB")

# 获取所有数据库
all_dbs = get_all_dbs(session)
print(all_dbs)

# 获取数据库信息
db_info = get_db_info(session, "dfs://test_db")
print(db_info)
```

### 3. 表操作

```python
from ddbtools import create_table, get_table_info, get_all_tables, DbColumn

# 定义表结构
columns = [
    DbColumn(name="date", dtype="DATE", comment="日期", compress="delta"),
    DbColumn(name="code", dtype="SYMBOL", comment="代码"),
    DbColumn(name="price", dtype="DOUBLE", comment="价格"),
    DbColumn(name="volume", dtype="LONG", comment="成交量")
]

# 创建表
create_table(
    session, 
    "dfs://test_db", 
    "stock", 
    columns, 
    partition_by="date",
    sortColumns="`code`date",
    keepDuplicates="LAST"
)

# 获取所有表
all_tables = get_all_tables(session, "dfs://test_db")
print(all_tables)

# 获取表信息
table_info = get_table_info(session, "dfs://test_db", "stock")
print(table_info)
```

### 4. CRUD 操作

```python
from ddbtools import BaseCRUD, Filter, Comparator
import pandas as pd
from datetime import date

# 创建测试数据
data = pd.DataFrame({
    "date": [date(2023, 1, 1), date(2023, 1, 2)],
    "code": ["AAPL", "AAPL"],
    "price": [150.0, 155.0],
    "volume": [1000000, 2000000]
})

# 定义 CRUD 类
class StockCRUD(BaseCRUD):
    key_cols = ["date", "code"]

# 创建 CRUD 实例
stock_crud = StockCRUD("dfs://test_db", "stock")

# 插入数据
stock_crud.upsert(session, data)

# 查询数据
# 查询所有数据
all_data = stock_crud.get(session)
print(all_data)

# 使用过滤条件查询
filter1 = Filter(column="date", comparator=Comparator.gt, value=date(2023, 1, 1))
filter2 = Filter(column="code", value="AAPL")
filtered_data = stock_crud.get(session, conds=[filter1, filter2])
print(filtered_data)

# 删除数据
stock_crud.delete(session, code="AAPL")
```

## API 文档

### 数据库操作

#### `create_db(session, dbname, partition_plan, engine="TSDB")`

创建数据库。

- **参数**：
  - `session`：DolphinDB 会话对象
  - `dbname`：数据库名称
  - `partition_plan`：分区方案
  - `engine`：数据库引擎，默认 "TSDB"

- **返回值**：创建结果消息

#### `get_all_dbs(session)`

获取所有数据库信息。

- **参数**：
  - `session`：DolphinDB 会话对象

- **返回值**：包含数据库信息的 DataFrame

#### `get_db_info(session, dbname)`

获取指定数据库的详细信息。

- **参数**：
  - `session`：DolphinDB 会话对象
  - `dbname`：数据库名称

- **返回值**：数据库信息

### 表操作

#### `DbColumn` 数据类

定义表列信息。

- **属性**：
  - `name`：列名
  - `dtype`：数据类型
  - `comment`：列注释（可选）
  - `compress`：压缩方式（可选）

#### `create_table(session, db_name, table_name, columns, partition_by=None, sortColumns=None, keepDuplicates=None, sortKeyMappingFunction=None)`

创建表。

- **参数**：
  - `session`：DolphinDB 会话对象
  - `db_name`：数据库名称
  - `table_name`：表名
  - `columns`：`DbColumn` 对象或列表
  - `partition_by`：分区列（可选）
  - `sortColumns`：排序列（可选）
  - `keepDuplicates`：重复值处理方式（可选，取值："ALL", "LAST", "FIRST"）
  - `sortKeyMappingFunction`：排序键映射函数（可选）

- **返回值**：创建结果消息

#### `get_table_info(session, db_name, table_name)`

获取表信息。

- **参数**：
  - `session`：DolphinDB 会话对象
  - `db_name`：数据库名称
  - `table_name`：表名

- **返回值**：包含表信息的 Series

#### `get_table_columns(session, db_name, table_name)`

获取表列信息。

- **参数**：
  - `session`：DolphinDB 会话对象
  - `db_name`：数据库名称
  - `table_name`：表名

- **返回值**：包含列信息的 DataFrame

#### `get_all_tables(session, db_name)`

获取数据库中所有表名。

- **参数**：
  - `session`：DolphinDB 会话对象
  - `db_name`：数据库名称

- **返回值**：表名列表

#### `create_dimensional_table(session, db_name, table_name, columns, partition_by=None)`

创建维度表。

- **参数**：
  - `session`：DolphinDB 会话对象
  - `db_name`：数据库名称
  - `table_name`：表名
  - `columns`：`DbColumn` 对象或列表
  - `partition_by`：分区列（可选）

- **返回值**：创建结果消息

#### `create_attribute_table(session, db_name, table_name, code_dtype="SYMBOL", attr_dtype="DOUBLE", dt_dtype="DATE")`

创建属性表。

- **参数**：
  - `session`：DolphinDB 会话对象
  - `db_name`：数据库名称
  - `table_name`：表名
  - `code_dtype`：代码列数据类型，默认 "SYMBOL"
  - `attr_dtype`：值列数据类型，默认 "DOUBLE"
  - `dt_dtype`：时间列数据类型，默认 "DATE"

- **返回值**：创建结果消息

### CRUD 操作

#### `Comparator` 枚举

比较运算符枚举：

- `eq`：等于（=）
- `gt`：大于等于（>=）
- `lt`：小于等于（<=）
- `like`：模糊匹配（like）
- `isin`：包含（in）

#### `Filter` 数据类

定义查询过滤条件。

- **属性**：
  - `column`：列名
  - `comparator`：比较运算符，默认 `Comparator.eq`
  - `value`：比较值

#### `BaseCRUD` 类

基础 CRUD 操作类，需要继承使用。

- **属性**：
  - `key_cols`：主键列列表（必须在子类中定义）

- **方法**：
  - `__init__(self, db_path: str, table_name: str)`：初始化方法
  - `upsert(self, session: ddb.Session, data: DataFrame)`：插入或更新数据
  - `delete(self, session: ddb.Session, **kwargs)`：删除数据
  - `get(self, session: ddb.Session, conds: Filter | List[Filter] = None, panel=True)`：查询数据

#### `DBDf` 类

继承自 pandas.DataFrame，自动处理 DolphinDB 数据类型转换。

- **方法**：
  - `__init__(self, session: ddb.Session, db_path: str, table_name: str, data: pd.DataFrame = None)`：初始化方法

## 日志配置

默认情况下，`ddbtools` 的日志是禁用的。如需启用日志，可以通过以下方式：

```python
from loguru import logger

# 启用日志
logger.enable("ddbtools")

# 配置日志级别和格式
logger.add(
    "ddbtools.log",
    level="INFO",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
    rotation="10 MB"
)
```

## 贡献

1. Fork 本仓库
2. 新建 Feat_xxx 分支
3. 提交代码
4. 新建 Pull Request

## 许可证

MIT License
