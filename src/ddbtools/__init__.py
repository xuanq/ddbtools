import importlib.metadata

# 从pyproject.toml中获取版本号，实现版本号的单一管理
try:
    __version__ = importlib.metadata.version('ddbtools')
except ImportError:
    # 如果无法导入，使用默认版本号（开发环境可能需要）
    __version__ = '0.0.dev0'

from ddbtools.dbmanip import create_db,get_all_dbs,get_db_info
from ddbtools.tablemanip import create_table,get_table_info,DbColumn,get_all_tables,get_table_columns
from ddbtools.crud import BaseCRUD,Filter,Comparator,DBDf