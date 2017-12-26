import panoply
import psycopg2
import psycopg2.extras


DEST = '{__tablename}'
BATCH_SIZE = 5000


class Postgres(panoply.DataSource):
    def __init__(self, *args, **kwargs):
        super(Postgres, self).__init__(*args, **kwargs)

        self.source['destination'] = self.source.get('destination') or DEST

        tables = self.source.get('tables', [])
        self.tables = tables[:]
        self.index = 0
        self.conn = None
        self.cursor = None
        self.batch_size = self.source.get('__batchSize', None)

    def read(self, batch_size=None):
        batch_size = self.batch_size or BATCH_SIZE
        total = len(self.tables)
        if self.index >= total:
            return None  # no tables left, we're done

        schema, table = self.tables[self.index]['value'].split('.', 1)

        msg = 'Reading table {} ({}) out of {}'\
              .format(self.index + 1, table, total)
        self.progress(self.index + 1, total, msg)

        # if there is no cursor (starting a new table read), create it
        if not self.cursor:
            self.conn, self.cursor = connect(self.source, self.log)
            q = get_query(schema, table, self.source)
            self.execute('DECLARE cur CURSOR FOR {}'.format(q))

        # read n(=BATCH_SIZE) records from the table
        self.execute('FETCH FORWARD {} FROM cur'.format(batch_size))
        result = self.cursor.fetchall()

        # add __schemaname and __tablename to each row so it would be available
        # as `destination` parameter if needed and also in case multiple tables
        # are pulled into the same destination table
        result = [dict(r, __tablename=table, __schemaname=schema)
                  for r in result]

        # no more rows for this table, clear and proceed to next table
        if not result:
            self.close()
            self.index += 1

        return result

    def execute(self, query):
        self.log(query)
        self.cursor.execute(query)

    def close(self):
        '''close the connection, and clear everything'''
        if self.cursor:
            self.cursor.close()
        if self.conn:
            # psycopg2 uses transactions for everything, hence we use rollback
            # to cleanly exit the transaction although conn.close should do it
            # implicitly
            self.conn.rollback()
            self.conn.close()

        self.conn = None
        self.cursor = None

    def get_tables(self):
        '''get the list of tables from the source'''
        query = '''
            SELECT * FROM information_schema.tables
            WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
        '''

        self.conn, self.cursor = connect(self.source, self.log)
        self.execute(query)
        result = map(format_table_name, self.cursor.fetchall())

        self.close()

        return result


def connect(source, log):
    '''connect to the DB using properties from the source'''
    host, dbname = source['addr'].rsplit('/', 1)
    port = 5432
    if ':' in host:
        host, port = host.rsplit(':', 1)
        port = int(port)  # pyscopg expects port to be numeric

    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            user=source['user'],
            password=source['password'],
            dbname=dbname
        )
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    except psycopg2.OperationalError, e:
        log(e)
        raise panoply.PanoplyException(
            "Login failed for user: {}".format(source['user']),
            retryable=False
        )

    return conn, cur


def get_query(schema, table, src):
    '''return a SELECT query using properties from the source'''
    where = ''
    if src.get('inckey') and src.get('incval'):
        where = " WHERE {} > '{}'".format(src['inckey'], src['incval'])

    return 'SELECT * FROM "{}"."{}"{}'.format(schema, table, where)


def format_table_name(row):
    '''format the table name with schema (and type if applicable)'''

    # value should include the schema of the tables as there might be tables
    # with the same name in different schemas
    value = "%s.%s" % (row['table_schema'], row['table_name'])

    # For display purposes name will indicate if this is a view or not,
    name = value
    if row['table_type'] == 'VIEW':
        name += ' (VIEW)'

    return {'name': name, 'value': value}
