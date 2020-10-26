from typing import Iterator

from .dal.queries.consts import SQL_GET_ALL_TABLES
from .exceptions import PostgresNoDataError
from .utils import format_table_name, connect, close_connection, \
    validate_host_and_port


def get_tables(source: dict) -> Iterator:
    """get the list of tables from the source"""

    validate_host_and_port(source)
    connector = connect(source)
    connector.cursor.execute(SQL_GET_ALL_TABLES)
    migrated_source = '__sourceMigrationDate' in source
    result = [format_table_name(row, migrated_source)
              for row in connector.cursor.fetchall()]

    close_connection(connector)

    if not result:
        raise PostgresNoDataError('Database is empty')

    return result
