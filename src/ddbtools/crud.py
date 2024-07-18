from typing import List
import dolphindb as ddb
from pandas import DataFrame
from datetime import datetime
from dataclasses import dataclass,field
from enum import Enum

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


class BaseCRUD():
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