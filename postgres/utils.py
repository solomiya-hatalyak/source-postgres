import psycopg2


def format_table_name(row):
    """format the table name with schema (and type if applicable)"""

    # value should include the schema of the tables as there might be tables
    # with the same name in different schemas
    value = "%s.%s" % (row['table_schema'], row['table_name'])

    # For display purposes name will indicate if this is a view or not,
    name = value
    if row['table_type'] == 'VIEW':
        name += ' (VIEW)'

    return {'name': name, 'value': value}


def connect(source):
    """connect to the DB using properties from the source"""

    # create partial DSN, user & pass still supplied as kwargs
    # as they're input separately from addr and will take precendence
    # over any user/pass from addr
    dsn = 'postgres://%s' % source['addr']
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

    return conn, cur