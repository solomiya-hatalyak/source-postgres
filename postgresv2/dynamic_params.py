from typing import Iterator

from .dal.queries.consts import SQL_GET_ALL_TABLES
from .utils import format_table_name, connect, close_connection,\
    validate_host_and_port


def get_tables(source: dict) -> Iterator:
    """get the list of tables from the source"""

    validate_host_and_port(source)
    connector = connect(source)
    connector.cursor.execute(SQL_GET_ALL_TABLES)
    result = list(map(format_table_name, connector.cursor.fetchall()))

    close_connection(connector)

    return result
