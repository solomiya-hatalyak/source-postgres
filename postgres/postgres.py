import sys
import time
import uuid
from collections import OrderedDict
from copy import copy

import backoff
import psycopg2.extras

from .dal.queries.consts import *
from .dal.queries.query_builder import get_query, get_max_value_query
from .keystrategy import KEY_STRATEGY
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
def _get_connect_timeout():
    return CONNECT_TIMEOUT


class Postgres(panoply.DataSource):

    def __init__(self, source: dict, options: dict):
        super(Postgres, self).__init__(source, options)
# TODO: break everything to 2 objects: conf and state
        self.source['destination'] = source.get('destination', DESTINATION)
        validate_host_and_port(source)
        self.batch_size = source.get('__batchSize', BATCH_SIZE)
        tables = source.get('data_available', [])
        self.tables = tables[:]
        self.index = 0
        self.connector = None
        self.state_id = None
        self.saved_state = {}
        self.current_keys = None
        self.inckey = source.get('inckey', '')
        self.incval = source.get('incval', '')

        state = source.get('state', {})
        self.index = state.get('last_index', 0)
        self.max_value = None

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
            state = self.saved_state.get('last_value', None)

            if not self.current_keys:
                self.current_keys = self.get_table_metadata(
                    SQL_GET_KEYS,
                    schema,
                    table
                )

            if not self.current_keys:
                # Select first column if no pk, indexes found
                self.current_keys = self.get_table_metadata(
                    SQL_GET_COLUMNS,
                    schema,
                    table
                )[:1]

            self.current_keys = key_strategy(self.current_keys)

            if not self.max_value:
                self.max_value = self.get_max_value(schema, table, self.inckey)
            query_opts = self.get_query_opts(schema, table, state,
                                             self.max_value)

            q = get_query(**query_opts)
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
            self.current_keys = None
            self.saved_state = {}
            self.max_value = None
        else:
            last_row = result[-1]
            self._save_last_values(last_row)
            self._report_state(self.index)

        return result

    def execute(self, query: str):
        self.log(query, "Loaded: {}".format(self.connector.loaded))
        try:
            self.connector.cursor.execute(query)
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

    def get_query_opts(self, schema: str, table: str,
                       state: dict, max_value=None) -> dict:
        query_opts = {
            'schema': schema,
            'table': table,
            'inckey': self.inckey,
            'incval': self.incval,
            'keys': self.current_keys,
            'state': state,
            'max_value': max_value
        }

        return query_opts

    def get_max_value(self, schema: str, table: str, column: str):
        if not column:
            return None

        query = get_max_value_query(column, schema, table)
        self.execute(query)

        return self.connector.cursor.fetchall()[0]['max']

    def get_table_metadata(self, sql: str, schema: str, table: str) -> list:
        search_path = '"{}"."{}"'.format(schema, table)
        sql = sql.format(search_path)
        self.log(sql)
        self.execute(sql)

        return self.connector.cursor.fetchall()

    def _save_last_values(self, last_row: dict):
        keys = map(lambda x: x.get('attname'), self.current_keys)
        last_value = [(key, last_row.get(key)) for key in keys]
        last_value = OrderedDict(last_value)

        self.saved_state = {
            'last_value': last_value
        }

    def _report_state(self, current_index: int):
        state = {
            'last_index': current_index
        }
        self.state(self.state_id, state)


def key_strategy(keys: list) -> list:
    keys_copy = copy(keys)

    for strategy in KEY_STRATEGY:
        results = strategy(keys_copy)

        if results:
            return results

    return keys
