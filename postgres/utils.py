from typing import Dict

import panoply
import psycopg2
from psycopg2.extras import RealDictRow

from .dal.connector import Connector
from .dal.queries.consts import CONNECT_TIMEOUT


def format_table_name(row: RealDictRow) -> Dict:
    """format the table name with schema (and type if applicable)"""
    table_types = {
        "VIEW": "view",
        "BASE TABLE": "table",
    }
    # value should include the schema of the tables as there might be tables
    # with the same name in different schemas
    value = f"{row['table_schema']}.{row['table_name']}"

    # For display purposes name will indicate if this is a view or table
    name = value
    name += f" ({table_types[row['table_type']]})"

    return {'name': name, 'value': value}


def connect(source: Dict) -> Connector:
    """connect to the DB using properties from the source"""

    # create partial DSN, user & pass still supplied as kwargs
    # as they're input separately from addr and will take precendence
    # over any user/pass from addr
    if 'addr' in source:
        # kept for backward compatibility
        dsn = 'postgres://{}'.format(source['addr'])
    else:
        dsn = 'postgres://{}:{}/{}'.format(source['host'],
                                           source['port'],
                                           source['db_name'])

    try:
        conn = psycopg2.connect(
            dsn=dsn,
            user=source['username'],
            password=source['password'],
            connect_timeout=CONNECT_TIMEOUT
        )
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    except psycopg2.OperationalError as e:
        if 'authentication failed' in str(e):
            e = panoply.PanoplyException(
                "Login failed for user: {}".format(source['username']),
                retryable=False
            )
        raise e

    return Connector(connection=conn, cursor=cur)


def close_connection(connector: Connector):
    """close the connection, and clear everything"""
    try:
        if connector.cursor:
            connector.cursor.close()
        if connector.connection:
            # psycopg2 uses transactions for everything, hence we use rollback
            # to cleanly exit the transaction although conn.close should do it
            # implicitly
            connector.connection.rollback()
            connector.connection.close()
    finally:
        reset(connector)


def reset(connector: Connector):
    connector.loaded = 0
    connector.conn = None
    connector.cursor = None
