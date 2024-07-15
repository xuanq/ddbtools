from dataclasses import dataclass
from typing import List
import dolphindb as ddb


@dataclass
class DbColumn:
    name: str
    dtype: str
    comment: str = None
    compress: str = None


def get_table_info(session: ddb.Session, db_name: str, table_name: str):
    session.table(db_name, table_name, "db_table")
    return session.run("schema(db_table).colDefs")


def create_dimensional_table(
    session: ddb.Session,
    db_name: str,
    table_name: str,
    columns: DbColumn | List[DbColumn],
):
    if session.run(f"existsTable('{db_name}',`{table_name});"):
        # log(f"在数据库 {db_name} 下表 {table_name} 已存在，请删除重建或修改原表)
        return

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
    session.run(script)
    # log(f"在数据库 {db_name} 下创建表 {table_name} 成功")


def create_attribute_table(
    session: ddb.Session,
    db_name: str,
    table_name: str,
    code_dtype: str = "SYMBOL",
    attr_dtype: str = "DOUBLE",
    dt_dtype: str = "DATE",
):
    if not session.run(f"existsTable('{db_name}',`{table_name});"):
        # log(f"在数据库 {db_name} 下表 {table_name} 不存在, 正在创建")
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
        # log(f"在数据库 {db_name} 下创建表 {table_name} 成功")


def delete_table(session: ddb.Session, db_name: str, table_name: str): ...


def add_columns(
    session: ddb.Session,
    db_name: str,
    table_name: str,
    columns: DbColumn | List[DbColumn],
): ...
