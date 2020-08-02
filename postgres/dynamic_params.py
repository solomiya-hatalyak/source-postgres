from typing import List, Iterator
from .utils import format_table_name, connect
from .consts import SQL_GET_ALL_TABLES


def get_databases(source) -> List:
    return ["a", "b", "c"]


def get_tables(source) -> Iterator:
    """get the list of tables from the source"""

    source.conn, source.cursor = connect(source.source)
    source.execute(SQL_GET_ALL_TABLES)
    result = map(format_table_name, source.cursor.fetchall())

    source.close()

    return result
