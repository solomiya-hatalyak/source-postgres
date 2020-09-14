from typing import Iterator
from .utils import format_table_name, connect, close_connection
from .dal.queries.consts import SQL_GET_ALL_TABLES


def get_tables(source) -> Iterator:
    """get the list of tables from the source"""

    connector = connect(source)
    connector.cursor.execute(SQL_GET_ALL_TABLES)
    result = list(map(format_table_name, connector.cursor.fetchall()))

    close_connection(connector)

    return result
