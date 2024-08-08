from dataclasses import dataclass
from typing import List, Literal
import dolphindb as ddb
from loguru import logger


@dataclass
class DbColumn:
    name: str
    dtype: str
    comment: str = None
    compress: str = None


def get_table_info(session: ddb.Session, db_name: str, table_name: str):
    session.table(db_name, table_name, "db_table")
    return session.run("schema(db_table).colDefs")


def create_table(
    session: ddb.Session,
    db_name: str,
    table_name: str,
    columns: DbColumn | List[DbColumn],
    partition_by: str = None,
    sortColumns: str = None,
    keepDuplicates:Literal["ALL","LAST","FIRST"]=None,
    sortKeyMappingFunction:str=None,
):
    if session.run(f"existsTable('{db_name}',`{table_name});"):
        logger.info(
            f"在数据库 {db_name} 下表 {table_name} 已存在，请删除重建或修改原表"
        )
        return f"在数据库 {db_name} 下表 {table_name} 已存在，请删除重建或修改原表"

    if isinstance(columns, DbColumn):
        columns = [columns]

    col_strs = []
    for col in columns:
        col_str = f"{col.name} {col.dtype}"
        patten = (isinstance(col.comment, str), isinstance(col.compress, str))
        match patten:
            case (True, True):
                col_str = (
                    col_str + f'[comment="{col.comment}",compress="{col.compress}"]'
                )
            case (True, False):
                col_str = col_str + f'[comment="{col.comment}"]'
            case (False, True):
                col_str = col_str + f'[compress="{col.compress}"]'
        col_strs.append(col_str)
    cols_str = "\n".join(col_strs)

    script = f"""
        create table "{db_name}"."{table_name}"(
            {cols_str}
        )
    """
    if partition_by:
        script = script + f"\npartitioned by {partition_by},"
    if sortColumns:
        script = script + f"\nsortColumns=[{sortColumns}],"
    if keepDuplicates:
        script = script + f"\nkeepDuplicates={keepDuplicates},"
    if sortKeyMappingFunction:
        script = script + f"\nsortKeyMappingFunction=[{sortKeyMappingFunction}]"
    session.run(script)
    logger.info(f"在数据库 {db_name} 下创建表 {table_name} 成功")
    return f"在数据库 {db_name} 下创建表 {table_name} 成功"


def create_dimensional_table(
    session: ddb.Session,
    db_name: str,
    table_name: str,
    columns: DbColumn | List[DbColumn],
    partition_by: str = None,
):
    if session.run(f"existsTable('{db_name}',`{table_name});"):
        logger.info(
            f"在数据库 {db_name} 下表 {table_name} 已存在，请删除重建或修改原表"
        )
        return f"在数据库 {db_name} 下表 {table_name} 已存在，请删除重建或修改原表"

    if isinstance(columns, DbColumn):
        columns = [columns]

    col_strs = []
    for col in columns:
        col_str = f"{col.name} {col.dtype}"
        patten = (isinstance(col.comment, str), isinstance(col.compress, str))
        match patten:
            case (True, True):
                col_str = (
                    col_str + f'[comment="{col.comment}",compress="{col.compress}"]'
                )
            case (True, False):
                col_str = col_str + f'[comment="{col.comment}"]'
            case (False, True):
                col_str = col_str + f'[compress="{col.compress}"]'
        col_strs.append(col_str)
    cols_str = "\n".join(col_strs)

    script = f"""
        create table "{db_name}"."{table_name}"(
            {cols_str}
        )
    """
    if partition_by:
        script = script + f"\npartitioned by {partition_by},"
    session.run(script)
    logger.info(f"在数据库 {db_name} 下创建表 {table_name} 成功")
    return f"在数据库 {db_name} 下创建表 {table_name} 成功"


def create_attribute_table(
    session: ddb.Session,
    db_name: str,
    table_name: str,
    code_dtype: str = "SYMBOL",
    attr_dtype: str = "DOUBLE",
    dt_dtype: str = "DATE",
):
    if not session.run(f"existsTable('{db_name}',`{table_name});"):
        script = f"""
        create table "{db_name}"."{table_name}"(
            datetime {dt_dtype}[comment="时间", compress="delta"]
            code {code_dtype}
            attribute SYMBOL
            value {attr_dtype}
        )
        partitioned by datetime, attribute,
        sortColumns=[`code, `datetime],
        keepDuplicates=ALL, 
        sortKeyMappingFunction=[hashBucket{{, 500}}]
        """
        session.run(script)
        logger.info(f"在数据库 {db_name} 下创建表 {table_name} 成功")
        return f"在数据库 {db_name} 下创建表 {table_name} 成功"
    else:
        logger.info(f"在数据库 {db_name} 下表 {table_name} 已存在,跳过")
        return f"在数据库 {db_name} 下表 {table_name} 已存在,跳过"


def delete_table(session: ddb.Session, db_name: str, table_name: str): ...


def add_columns(
    session: ddb.Session,
    db_name: str,
    table_name: str,
    columns: DbColumn | List[DbColumn],
): ...
