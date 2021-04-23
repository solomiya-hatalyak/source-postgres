import sys
import time
import uuid

import backoff
import psycopg2.extras

from .dal.key_strategy import choose_index
from .dal.queries.consts import *
from .dal.queries.query_builder import get_query
from .exceptions import PostgresInckeyError, PostgresUndefinedTableError
from .progressor import background_progress
from .utils import *


def _log_backoff(details: dict):
    err = sys.exc_info()[1]
    print('Retrying (attempt {}) in {:.2f} seconds, after error {}: {}'.format(
        details['tries'],
        details['wait'],
        err.pgcode or '',
        err.pgerror
    ))


# Used for testing - this constant is overriden durring tests so that we don't
# actually have to wait for the retry
def _get_connect_timeout() -> int:
    return CONNECT_TIMEOUT


class Postgres(panoply.DataSource):

    def __init__(self, source: dict, options: dict):
        super(Postgres, self).__init__(source, options)
# TODO: break everything to 2 objects: conf and state
        if "destination" not in self.source or not self.source["destination"]:
            self.source["destination"] = DESTINATION
        validate_host_and_port(source)
        self.batch_size = source.get('__batchSize', BATCH_SIZE)
        tables = source.get('data_available', [])
        self.tables = tables[:]
        self.index = 0
        self.connector = None
        self.state_id = None
        self.current_keys = None
        self.inckey = source.get('inckey', '')
        self.incval = source.get('incval', '')
        self.keys = None
        self.last_values = None
        state = source.get('state', {})
        self.index = state.get('last_index', 0)
        # Remove the state object from the source definition
        # since it does not need to be saved on the source.
        self.source.pop('state', None)
        self.start_time = None

    @backoff.on_exception(backoff.expo,
                          psycopg2.DatabaseError,
                          max_tries=MAX_RETRIES,
                          on_backoff=_log_backoff,
                          base=_get_connect_timeout)
    def read(self, batch_size: int = None) -> (list, None):
        batch_size = batch_size or self.batch_size
        total = len(self.tables)

        if self.start_time is None:
            self.start_time = time.time()

        if self.index >= total:
            end_time = time.time()
            elapsed_time = time.strftime('%H:%M:%S',
                                         time.gmtime(
                                             end_time - self.start_time))
            self.log('Collection duration: {}'.format(elapsed_time))
            return None  # no tables left, we're done

        schema, table = self.tables[self.index]['value'].split('.', 1)

        msg = 'Reading table {} ({}) out of {}'\
              .format(self.index + 1, table, total)
        self.log(msg)
        self.progress(self.index + 1, total, msg)

        if self.connector is None or self.connector.cursor is None:
            self.connector = connect(self.source)

            register_adapters(self.connector.cursor)

            if self.keys is None:
                if self.inckey:
                    self.keys = [self.inckey]
                    if self.incval and self.last_values is None:
                        self.last_values = [self.incval]
                else:
                    try:
                        keys = self.get_table_metadata(
                            SQL_GET_INDEXES,
                            schema,
                            table
                        )
                        self.keys = choose_index(keys)
                    except psycopg2.errors.UndefinedTable:
                        raise PostgresUndefinedTableError(
                            'Table "{}"."{}" does not exist'.format(schema,
                                                                    table))

            q = get_query(schema, table, self.keys, self.last_values)
            self.execute('DECLARE cur CURSOR FOR {}'.format(q))

        # read n(=BATCH_SIZE) records from the table
        self.execute('FETCH FORWARD {} FROM cur'.format(batch_size))
        result = self.connector.cursor.fetchall()

        self.state_id = str(uuid.uuid4())
        # Add __schemaname and __tablename to each row so it would be available
        # as `destination` parameter if needed and also in case multiple tables
        # are pulled into the same destination table.
        # state_id is also added in order to support checkpoints
        internals = dict(
            __tablename=table,
            __schemaname=schema,
            __databasename=self.source.get('db_name'),
            __state=self.state_id
        )
        result = [dict(r, **internals) for r in result]
        self.connector.loaded += len(result)

        # no more rows for this table, clear and proceed to next table
        if not result:
            self.log('Finished collection of table: {}'.format(table))
            close_connection(self.connector)
            self.index += 1
            self.keys = None
            self.last_values = None
        else:
            last_row = result[-1]
            self.last_values = [last_row[column] for column in self.keys]
            self._report_state(self.index)

        return result

    def get_table_metadata(self, sql, schema, table):
        search_path = '"{}"."{}"'.format(schema, table)
        sql = sql.format(search_path)
        self.execute(sql)

        return self.connector.cursor.fetchall()

    @background_progress(message='Waiting for query to execute', waiting_interval=3*60)
    def execute(self, query: str):
        self.log(query, "Loaded: {}".format(self.connector.loaded))
        try:
            self.connector.cursor.execute(query)
        except psycopg2.errors.UndefinedColumn as e:
            if self.inckey:
                raise PostgresInckeyError(
                    'Incremental key "{}" does not exist in the '
                    'table "{}"'.format(self.inckey,
                                        self.tables[self.index]['value']))
            raise e
        except psycopg2.DatabaseError as e:
            # We're ensuring that there is no connection or cursor objects
            # after an exception so that when we retry,
            # a new connection will be created.

            # Since we got an error, it will trigger backoff expo
            # We want the source to continue where it left off
            close_connection(self.connector)
            print('Raise error {}'.format(e))
            raise e
        self.log("DONE", query)

    def get_query_opts(self, schema: str, table: str) -> dict:
        query_opts = {
            'schema': schema,
            'table': table,
            'last_index_value': self.last_values,
            'index': self.index
        }

        return query_opts

    def _report_state(self, current_index: int):
        state = {
            'last_index': current_index
        }
        self.state(self.state_id, state)
