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

    def read(self, n=None):
        total = len(self.tables)
        if self.index >= total:
            return None  # no tables left, we're done

        table = self.tables[self.index]
        if table.endswith('(VIEW)'):
            table = table[:-7]

        msg = 'Reading table {} ({}) out of {}'\
              .format(self.index + 1, table, total)
        self.progress(self.index + 1, total, msg)

        # if there is no cursor (starting a new table read), create it
        if not self.cursor:
            self.conn, self.cursor = connect(self.source)
            q = get_query(table, self.source)
            self.cursor.execute('DECLARE cur CURSOR FOR {}'.format(q))

        # read BATCH_SIZE records from the table
        self.cursor.execute('FETCH FORWARD {} FROM cur'.format(BATCH_SIZE))
        result = self.cursor.fetchall()

        # add __tablename to each row, so it would be available as `destination`
        tablename = table.lower().replace(".", "_")
        result = [dict(r, __tablename=tablename) for r in result]

        # no more rows for this table, clear and proceed to next table
        if not result:
            self.close()
            self.index += 1

        return result

    def close(self):
        '''close the connection, and clear everything'''
        self.conn.rollback()
        self.cursor.close()
        self.conn.close()
        self.cursor = None
        self.conn = None

    def get_tables(self):
        '''get the list of tables from the source'''
        query = '''
            SELECT * FROM information_schema.tables
            WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
        '''

        self.conn, self.cursor = connect(self.source)
        self.cursor.execute(query)
        result = map(format_table_name, self.cursor.fetchall())

        self.close()

        return result


def connect(source):
    '''connect to the DB using properties from the source'''
    host, dbname = source['address'].rsplit('/', 1)
    if ':' in host:
        host, port = host.rsplit(':', 1)

    try:
        conn = psycopg2.connect(
            host=host,
            user=source['username'],
            password=source['password'],
            dbname=dbname
        )
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    except psycopg2.OperationalError:
        raise panoply.PanoplyException(
            "Login failed for user: {}".format(source['username']),
            retryable=False
        )

    return conn, cur

def get_query(table, src):
    '''return a SELECT query using properties from the source'''
    where = ''
    if src.get('inckey') and src.get('incval'):
        where = ' WHERE {} > {}'.format(src['inckey'], src['incval'])

    return 'SELECT * FROM {}{}'.format(table, where)

def format_table_name(row):
    '''format the table name (add schema and type if applicable)'''
    name = row['table_name']
    if row['table_schema'] != 'public':
        name = row['table_schema'] + '.' + name

    if row['table_type'] == 'VIEW':
        name += ' (VIEW)'

    return name
