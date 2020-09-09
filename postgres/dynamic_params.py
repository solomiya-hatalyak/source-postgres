from typing import Iterator
from .utils import format_table_name, connect
from dal.queries.consts import SQL_GET_ALL_TABLES


def get_tables(source) -> Iterator:
    """get the list of tables from the source"""

    source.connector = connect(source.source)
    source.execute(SQL_GET_ALL_TABLES)
    result = map(format_table_name, source.connector.cursor.fetchall())

    source.close()

    return result
