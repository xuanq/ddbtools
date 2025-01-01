from typing import Dict, List
import dolphindb as ddb
from pandas import DataFrame
from datetime import datetime
from dataclasses import dataclass, field
from enum import Enum
from ddbtools import get_table_columns
import pandas as pd


class Comparator(Enum):
    eq = "="
    gt = ">="
    lt = "<="
    like = "like"
    isin = "in"


@dataclass
class Filter:
    column: str
    comparator: Comparator = Comparator.eq
    value: str | int | float | list | datetime = None
    clause: str = field(init=False)

    def __post_init__(self):
        if isinstance(self.value, str) and self.comparator != Comparator.like:
            self.value = "'" + self.value + "'"

        if issubclass(self.value.__class__, datetime):
            self.value = self.value.strftime("%Y.%m.%d %H:%M:%S.%f")[:-3]

        match self.comparator:
            case Comparator.eq | Comparator.gt | Comparator.lt | Comparator.isin:
                self.clause = f"{self.column} {self.comparator.value} {self.value}"
            case Comparator.like:
                if not isinstance(self.value, list):
                    self.value = [self.value]
                conditions = [
                    f"{self.column} {self.comparator.value} '%{v}%'" for v in self.value
                ]
                self.clause = " or ".join(conditions)


class BaseCRUD:
    key_cols: List[str]

    def __init__(self, db_path: str, table_name: str) -> None:
        self.db_path = db_path
        self.table_name = table_name

    def upsert(self, session: ddb.Session, data: DataFrame):
        upserter = ddb.TableUpserter(
            dbPath=self.db_path,
            tableName=self.table_name,
            ddbSession=session,
            ignoreNull=True,
            keyColNames=self.key_cols,
        )
        upserter.upsert(data)

    def delete(self, session: ddb.Session, **kwargs):
        table_delete = session.table(self.db_path, self.table_name).delete()
        for kw, param in kwargs.items():
            table_delete = table_delete.where(f"{kw}={param}")
        table_delete.execute()

    def get(
        self, session: ddb.Session, conds: Filter | List[Filter] = None, panel=True
    ):
        table = session.table(self.db_path, self.table_name)
        if conds:
            if isinstance(conds, Filter):
                conds = [conds]
            for cond in conds:
                table = table.where(cond.clause)

        if "attr_" in self.table_name and panel:
            value = (
                table.select("value")
                .pivotby(index="datetime,code", column="attribute")
                .toDF()
            )
            if value.empty:
                return value
            else:
                return value.set_index(["datetime", "code"]).sort_index()
        else:
            return table.toDF()


DTYPE_DDB2PD = {
    "BOOL": "boolean",
    "CHAR": "Int8",
    "SHORT": "Int16",
    "INT": "Int32",
    "LONG": "Int64",
    "DATE": "datetime64",
    "MONTH": "datetime64",
    "TIME": "datetime64",
    "DATETIME": "datetime64",
    "MINUTE": "datetime64",
    "SECOND": "datetime64",
    "NANOTIME": "datetime64",
    "TIMESTAMP": "datetime64",
    "NANOTIMESTAMP": "datetime64",
    "FLOAT": "Float32",
    "DOUBLE": "float64",
    "SYMBOL": "object",
    "STRING": "object",
}


class DBDf(pd.DataFrame):
    def __init__(
        self,
        session: ddb.Session,
        db_path: str,
        table_name: str,
        data: pd.DataFrame = None,
    ):
        db_cols: pd.DataFrame = get_table_columns(session, db_path, table_name)
        db_cols["pd_dtype"] = db_cols["typeString"].map(DTYPE_DDB2PD)
        super().__init__(columns=db_cols.index)
        self.attrs["column_names_types"] = db_cols["pd_dtype"].to_dict()

        if data is not None:
            data = pd.DataFrame(data).reset_index()
            commom_columns = data.columns.intersection(db_cols.index)
            self[commom_columns] = data[commom_columns]

        self._apply_column_types()

    def _apply_column_types(self):
        for name, dtype in self.attrs["column_names_types"].items():
            # 需要做时间格式转化
            if pd.api.types.is_datetime64_dtype(dtype):
                self[name] = pd.to_datetime(self[name])

                if pd.api.types.is_datetime64tz_dtype(self[name]):
                    # 已有时区，将时区去除(默认转化为东八区时间)
                    self[name] = self[name].dt.tz_convert("PRC").dt.tz_localize(None)
            elif self[name].dtype != dtype:
                if dtype == "boolean":
                    mapping = {
                        "TRUE": True,
                        "True": True,
                        "true": True,
                        "是": True,
                        "1": True,
                        "FALSE": False,
                        "False": False,
                        "false": False,
                        "否": False,
                        "0": False,
                    }
                    self[name] = self[name].map(mapping)
                self[name] = self[name].astype(dtype, errors="ignore")
