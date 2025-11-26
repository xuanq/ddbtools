# ddbtools

## Project Introduction

`ddbtools` is a secondary development tool library based on DolphinDB, providing convenient database operation functions to simplify the creation, modification, and query operations of DolphinDB databases.

## Core Features

- **Database Operations**: Create databases, get database information, get all databases
- **Table Operations**: Create tables, get table information, get table columns, get all tables, create dimensional tables, create attribute tables
- **CRUD Operations**: Based on `BaseCRUD` class, supporting flexible filtering conditions
- **Data Type Conversion**: Automatically handle data type conversion between DolphinDB and pandas
- **Logging Support**: Logging based on loguru library

## Installation

### Dependencies

- Python 3.10+
- DolphinDB 3.0.1.0+
- loguru 0.7.2+

### Installation Methods

Install basic version using uv:

```bash
uv add ddbtools
```

Install with logging support:

```bash
uv add ddbtools --optional log
```

Install using pip:

```bash
pip install ddbtools
```

Install with logging support:

```bash
pip install ddbtools[log]
```

## Quick Start

### 1. Connect to DolphinDB Server

```python
import dolphindb as ddb

# Create DolphinDB session
session = ddb.Session()
session.connect("localhost", 8848)
```

### 2. Database Operations

```python
from ddbtools import create_db, get_all_dbs, get_db_info

# Create database
create_db(session, "dfs://test_db", "VALUE date", engine="TSDB")

# Get all databases
all_dbs = get_all_dbs(session)
print(all_dbs)

# Get database information
db_info = get_db_info(session, "dfs://test_db")
print(db_info)
```

### 3. Table Operations

```python
from ddbtools import create_table, get_table_info, get_all_tables, DbColumn

# Define table structure
columns = [
    DbColumn(name="date", dtype="DATE", comment="Date", compress="delta"),
    DbColumn(name="code", dtype="SYMBOL", comment="Code"),
    DbColumn(name="price", dtype="DOUBLE", comment="Price"),
    DbColumn(name="volume", dtype="LONG", comment="Volume")
]

# Create table
create_table(
    session, 
    "dfs://test_db", 
    "stock", 
    columns, 
    partition_by="date",
    sortColumns="`code`date",
    keepDuplicates="LAST"
)

# Get all tables
all_tables = get_all_tables(session, "dfs://test_db")
print(all_tables)

# Get table information
table_info = get_table_info(session, "dfs://test_db", "stock")
print(table_info)
```

### 4. CRUD Operations

```python
from ddbtools import BaseCRUD, Filter, Comparator
import pandas as pd
from datetime import date

# Create test data
data = pd.DataFrame({
    "date": [date(2023, 1, 1), date(2023, 1, 2)],
    "code": ["AAPL", "AAPL"],
    "price": [150.0, 155.0],
    "volume": [1000000, 2000000]
})

# Define CRUD class
class StockCRUD(BaseCRUD):
    key_cols = ["date", "code"]

# Create CRUD instance
stock_crud = StockCRUD("dfs://test_db", "stock")

# Insert data
stock_crud.upsert(session, data)

# Query data
# Query all data
all_data = stock_crud.get(session)
print(all_data)

# Query with filter conditions
filter1 = Filter(column="date", comparator=Comparator.gt, value=date(2023, 1, 1))
filter2 = Filter(column="code", value="AAPL")
filtered_data = stock_crud.get(session, conds=[filter1, filter2])
print(filtered_data)

# Delete data
stock_crud.delete(session, code="AAPL")
```

## API Documentation

### Database Operations

#### `create_db(session, dbname, partition_plan, engine="TSDB")`

Create a database.

- **Parameters**:
  - `session`: DolphinDB session object
  - `dbname`: Database name
  - `partition_plan`: Partition plan
  - `engine`: Database engine, default "TSDB"

- **Return Value**: Creation result message

#### `get_all_dbs(session)`

Get information about all databases.

- **Parameters**:
  - `session`: DolphinDB session object

- **Return Value**: DataFrame containing database information

#### `get_db_info(session, dbname)`

Get detailed information about a specified database.

- **Parameters**:
  - `session`: DolphinDB session object
  - `dbname`: Database name

- **Return Value**: Database information

### Table Operations

#### `DbColumn` Data Class

Define table column information.

- **Attributes**:
  - `name`: Column name
  - `dtype`: Data type
  - `comment`: Column comment (optional)
  - `compress`: Compression method (optional)

#### `create_table(session, db_name, table_name, columns, partition_by=None, sortColumns=None, keepDuplicates=None, sortKeyMappingFunction=None)`

Create a table.

- **Parameters**:
  - `session`: DolphinDB session object
  - `db_name`: Database name
  - `table_name`: Table name
  - `columns`: `DbColumn` object or list
  - `partition_by`: Partition column (optional)
  - `sortColumns`: Sort columns (optional)
  - `keepDuplicates`: Duplicate handling method (optional, values: "ALL", "LAST", "FIRST")
  - `sortKeyMappingFunction`: Sort key mapping function (optional)

- **Return Value**: Creation result message

#### `get_table_info(session, db_name, table_name)`

Get table information.

- **Parameters**:
  - `session`: DolphinDB session object
  - `db_name`: Database name
  - `table_name`: Table name

- **Return Value**: Series containing table information

#### `get_table_columns(session, db_name, table_name)`

Get table column information.

- **Parameters**:
  - `session`: DolphinDB session object
  - `db_name`: Database name
  - `table_name`: Table name

- **Return Value**: DataFrame containing column information

#### `get_all_tables(session, db_name)`

Get all table names in the database.

- **Parameters**:
  - `session`: DolphinDB session object
  - `db_name`: Database name

- **Return Value**: List of table names

#### `create_dimensional_table(session, db_name, table_name, columns, partition_by=None)`

Create a dimensional table.

- **Parameters**:
  - `session`: DolphinDB session object
  - `db_name`: Database name
  - `table_name`: Table name
  - `columns`: `DbColumn` object or list
  - `partition_by`: Partition column (optional)

- **Return Value**: Creation result message

#### `create_attribute_table(session, db_name, table_name, code_dtype="SYMBOL", attr_dtype="DOUBLE", dt_dtype="DATE")`

Create an attribute table.

- **Parameters**:
  - `session`: DolphinDB session object
  - `db_name`: Database name
  - `table_name`: Table name
  - `code_dtype`: Code column data type, default "SYMBOL"
  - `attr_dtype`: Value column data type, default "DOUBLE"
  - `dt_dtype`: Time column data type, default "DATE"

- **Return Value**: Creation result message

### CRUD Operations

#### `Comparator` Enum

Comparison operator enum:

- `eq`: Equal to (=)
- `gt`: Greater than or equal to (>=)
- `lt`: Less than or equal to (<=)
- `like`: Fuzzy matching (like)
- `isin`: Contains (in)

#### `Filter` Data Class

Define query filter conditions.

- **Attributes**:
  - `column`: Column name
  - `comparator`: Comparison operator, default `Comparator.eq`
  - `value`: Comparison value

#### `BaseCRUD` Class

Base CRUD operation class, needs to be inherited.

- **Attributes**:
  - `key_cols`: Primary key column list (must be defined in subclass)

- **Methods**:
  - `__init__(self, db_path: str, table_name: str)`: Initialization method
  - `upsert(self, session: ddb.Session, data: DataFrame)`: Insert or update data
  - `delete(self, session: ddb.Session, **kwargs)`: Delete data
  - `get(self, session: ddb.Session, conds: Filter | List[Filter] = None, panel=True)`: Query data

#### `DBDf` Class

Inherited from pandas.DataFrame, automatically handles DolphinDB data type conversion.

- **Methods**:
  - `__init__(self, session: ddb.Session, db_path: str, table_name: str, data: pd.DataFrame = None)`: Initialization method

## Log Configuration

`ddbtools` supports optional logging functionality based on the loguru library. By default, logging is disabled and loguru is not a required dependency.

### Enabling Logging

1. First, make sure you have installed the version with logging support:

```bash
uv add ddbtools --optional log
# Or using pip
pip install ddbtools[log]
```

2. Then, enable logging in your code:

```python
from ddbtools import logger

# Enable logging
logger.enable("ddbtools")

# Configure log level and format (only available if loguru is installed)
try:
    from loguru import logger as loguru_logger
    loguru_logger.add(
        "ddbtools.log",
        level="INFO",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
        rotation="10 MB"
    )
except ImportError:
    pass
```

### Log Levels

Supported log levels:
- `debug`: Debug information
- `info`: General information
- `warning`: Warning messages
- `error`: Error messages

### Disabling Logging

If you need to disable logging, you can use the following code:

```python
from ddbtools import logger

# Disable logging
logger.disable("ddbtools")
```

## Contribution

1. Fork this repository
2. Create a Feat_xxx branch
3. Commit your code
4. Create a Pull Request

## License

MIT License
