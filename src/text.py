from engine import Engine
from sqlalchemy import inspect
from sqlalchemy import MetaData
from sqlalchemy import Table
from state_machine import SolrSQLState


class Text:

    def __init__(self) -> None:

        self.insp = inspect(Engine.get_engine())

        self.state_keyword = {
            SolrSQLState.SELECT: ["select"],
            SolrSQLState.FROM: ["from"],
            SolrSQLState.WHERE: ["where"],
            SolrSQLState.GROUP_BY: ["group by"],
            SolrSQLState.ORDER_BY: ["order by"],
            SolrSQLState.LIMIT: ["limit"],
            SolrSQLState.DIRECTION: ["asc", "desc"],
        }

        self.state_functions = ["count", "sum", "avg", "min", "max"]
        self.logical_ops = ["not", "and", "or"]

        columns = []
        table_names = self.insp.get_table_names()
        for table_name in table_names:
            meta = MetaData()
            table = Table(table_name, meta)
            self.insp.reflect_table(table, None)
            columns.extend(table.columns.keys())
        self.columns = columns

    def textual(self, state: SolrSQLState):
        if state == SolrSQLState.INITIAL:
            raise ValueError
        elif state in self.state_keyword:
            return self.state_keyword[state]
        elif state == SolrSQLState.EXPRESSION_LIST:
            return self.columns + self.state_functions
        elif state == SolrSQLState.COLLECTION_NAME:
            table_names = self.insp.get_table_names()
            return table_names
        elif state in [SolrSQLState.COLUMN_NAME, SolrSQLState.COLUMN_NAME_LIST]:
            return self.columns  # + self.column_aliases
        elif state == SolrSQLState.CONDITION:
            return self.columns + self.state_functions + self.logical_ops
        elif state == SolrSQLState.OPERATOR:
            return self.columns + self.state_functions
