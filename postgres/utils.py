import psycopg2
import panoply
from typing import Dict, Tuple, Any
from dal.queries.consts import CONNECT_TIMEOUT
from .dal.connector import Connector


def format_table_name(row) -> Dict:
    """format the table name with schema (and type if applicable)"""

    # value should include the schema of the tables as there might be tables
    # with the same name in different schemas
    value = "%s.%s" % (row['table_schema'], row['table_name'])

    # For display purposes name will indicate if this is a view or not,
    name = value
    if row['table_type'] == 'VIEW':
        name += ' (VIEW)'

    return {'name': name, 'value': value}


def connect(source) -> Connector:
    """connect to the DB using properties from the source"""

    # create partial DSN, user & pass still supplied as kwargs
    # as they're input separately from addr and will take precendence
    # over any user/pass from addr
    dsn = 'postgres://%s' % source['addr']  # kept for backward compatibility

    if dsn is None or dsn == '':
        dsn = 'postgres://{}:{}/{}'.format(source['host'], source['port'], source['db_name'])

    try:
        conn = psycopg2.connect(
            dsn=dsn,
            user=source['user'],
            password=source['password'],
            connect_timeout=CONNECT_TIMEOUT
        )
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    except psycopg2.OperationalError as e:
        if 'authentication failed' in e.message:
            e = panoply.PanoplyException(
                "Login failed for user: {}".format(source['user']),
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
