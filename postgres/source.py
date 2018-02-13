import sys
import panoply
import uuid
import psycopg2
import psycopg2.extras
import backoff

DEST = '{__tablename}'
BATCH_SIZE = 5000
CONNECT_TIMEOUT = 15  # seconds
MAX_RETRIES = 5
RETRY_TIMEOUT = 2


def _log_backoff(details):
    err = sys.exc_info()[1]
    print 'Retrying (attempt %s) in %d seconds, after error %s: %s' % (
        details['tries'],
        details['wait'],
        err.pgcode or '',
        err.message
    )


# Used for testing - this constant is overriden durring tests so that we don't
# actually have to wait for the retry
def _get_connect_timeout():
    return CONNECT_TIMEOUT


class Postgres(panoply.DataSource):

    def __init__(self, source, options):
        super(Postgres, self).__init__(source, options)

        self.source['destination'] = self.source.get('destination', DEST)

        self.batch_size = self.source.get('__batchSize', BATCH_SIZE)
        tables = self.source.get('tables', [])
        self.tables = tables[:]
        self.index = 0
        self.conn = None
        self.cursor = None
        self.state_id = None
        self.loaded = 0
        self.saved_state = self.source.get('state', {})

        # Remove the state object from the source definition
        # since it does not need to be saved on the source.
        self.source.pop('state', None)

    @backoff.on_exception(backoff.expo,
                          psycopg2.DatabaseError,
                          max_tries=MAX_RETRIES,
                          on_backoff=_log_backoff,
                          base=_get_connect_timeout)
    def read(self, batch_size=None):
        batch_size = batch_size or self.batch_size
        total = len(self.tables)
        if self.index >= total:
            return None  # no tables left, we're done

        schema, table = self.tables[self.index]['value'].split('.', 1)

        msg = 'Reading table {} ({}) out of {}'\
              .format(self.index + 1, table, total)
        self.progress(self.index + 1, total, msg)

        if not self.cursor:
            self.conn, self.cursor = connect(self.source)
            state = self.saved_state.get("%s.%s" % (schema, table))
            self.loaded = state if state is not None else 0
            q = get_query(schema, table, self.source, state)
            self.execute('DECLARE cur CURSOR FOR {}'.format(q))

        # read n(=BATCH_SIZE) records from the table
        self.execute('FETCH FORWARD {} FROM cur'.format(batch_size))
        result = self.cursor.fetchall()

        self.state_id = str(uuid.uuid4())
        # Add __schemaname and __tablename to each row so it would be available
        # as `destination` parameter if needed and also in case multiple tables
        # are pulled into the same destination table.
        # state_id is also added in order to support checkpoints
        internals = dict(
            __tablename=table,
            __schemaname=schema,
            __state=self.state_id
        )
        result = [dict(r, **internals) for r in result]
        self.loaded += len(result)

        # no more rows for this table, clear and proceed to next table
        if not result:
            self.close()
            self.index += 1
            self.loaded = 0
        else:
            self._report_state(internals, self.loaded)

        return result

    def execute(self, query):
        self.log(query, "Loaded: %s" % self.loaded)
        try:
            self.cursor.execute(query)
        except psycopg2.DatabaseError, e:
            # We're ensuring that there is no connection or cursor objects
            # after an exception so that when we retry,
            # a new connection will be created.
            self.reset()
            raise
        self.log("DONE", query)

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

        self.reset()

    def reset(self):
        self.loaded = 0
        self.conn = None
        self.cursor = None

    def get_tables(self):
        '''get the list of tables from the source'''
        query = '''
            SELECT * FROM information_schema.tables
            WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
        '''

        self.conn, self.cursor = connect(self.source)
        self.execute(query)
        result = map(format_table_name, self.cursor.fetchall())

        self.close()

        return result

    def _report_state(self, params, loaded):
        table_name = '%(__schemaname)s.%(__tablename)s' % params
        self.state(self.state_id, {table_name: loaded})


def connect(source):
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
            dbname=dbname,
            connect_timeout=CONNECT_TIMEOUT
        )
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    except psycopg2.OperationalError, e:
        if 'authentication failed' in e.message:
            e = panoply.PanoplyException(
                "Login failed for user: {}".format(source['user']),
                retryable=False
            )
        raise e

    return conn, cur


def get_query(schema, table, src, state=None):
    '''return a SELECT query using properties from the source'''
    offset = ''
    where = ''
    if src.get('inckey') and src.get('incval'):
        where = " WHERE {} > '{}'".format(src['inckey'], src['incval'])

    if state:
        offset = " OFFSET %s" % state

    return 'SELECT * FROM "{}"."{}"{}{}'.format(schema, table, where, offset)


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
