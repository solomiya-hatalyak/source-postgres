import unittest

import mock
import psycopg2
from panoply import PanoplyException

from postgresv2.dal.queries.consts import MAX_RETRIES, CONNECT_TIMEOUT
from postgresv2.dal.queries.query_builder import get_query
from postgresv2.dal.key_strategy import choose_index
from postgresv2.dynamic_params import get_tables
from postgresv2.exceptions import PostgresValidationError, PostgresInckeyError
from postgresv2.postgresv2 import Postgres
from postgresv2.utils import connect, validate_host_and_port

OPTIONS = {
    "logger": lambda *msgs: None,  # no-op logger
}


class TestPostgres(unittest.TestCase):
    def setUp(self):
        self.source = {
            "host": "test.database.name",
            "port": "5432",
            "db_name": "foobar",
            "username": "test",
            "password": "testpassword",
            "inckey": "id",
            "incval": "1"
        }
        self.mock_recs = [
            {'id': 1, 'col1': 'foo1', 'col2': 'bar1'},
            {'id': 2, 'col1': 'foo2', 'col2': 'bar2'},
            {'id': 3, 'col1': 'foo3', 'col2': 'bar3'}
        ]

    def tearDown(self):
        self.source = None

    # fetches list of tables from database
    @mock.patch("psycopg2.connect")
    def test_get_tables(self, m):
        """gets the list of tables from the database"""

        # Notice 'name' here is only for validation of expected result.
        # It is not a field that returns in the actual query results
        mock_tables = [
            {'table_schema': 'dbo', 'table_name': 'testNoUnique',
             'table_type': 'BASE TABLE', 'name': 'dbo.testNoUnique (table)'},
            {'table_schema': 'dbo', 'table_name': 'testNoIndex',
             'table_type': 'BASE TABLE', 'name': 'dbo.testNoIndex (table)'},
            {'table_schema': 'SalesLT', 'table_name': 'Customer',
             'table_type': 'BASE TABLE', 'name': 'SalesLT.Customer (table)'},
            {'table_schema': 'SalesLT', 'table_name': 'ProductModel',
             'table_type': 'BASE TABLE',
             'name': 'SalesLT.ProductModel (table)'},
            {'table_schema': 'mySchema', 'table_name': 'someTable',
             'table_type': 'VIEW', 'name': 'mySchema.someTable (view)'}
        ]

        m.return_value.cursor.return_value.fetchall.return_value = mock_tables
        tables = get_tables(self.source)
        self.assertEqual(len(tables), len(mock_tables))
        for x in range(0, len(tables)):
            mtable = mock_tables[x]
            v = '{}.{}'.format(mtable["table_schema"], mtable["table_name"])

            self.assertEqual(tables[x]['name'], mtable['name'])
            self.assertEqual(tables[x]['value'], v)

    @mock.patch("psycopg2.connect")
    def test_read(self, mock_connect):
        """reads a table from the database and validates that each row
        has a __tablename, __schemaname and __databasename column"""

        inst = Postgres(self.source, OPTIONS)
        inst.tables = [{'value': 'my_schema.foo_bar'}]
        cursor_return_value = mock_connect.return_value.cursor.return_value
        cursor_return_value.fetchall.return_value = self.mock_recs

        rows = inst.read()
        self.assertEqual(len(rows), len(self.mock_recs))
        for x in range(0, len(rows)):
            self.assertEqual(rows[x]['__tablename'], 'foo_bar')
            self.assertEqual(rows[x]['__schemaname'], 'my_schema')
            self.assertEqual(rows[x]['__databasename'], self.source['db_name'])

    @mock.patch("psycopg2.connect")
    def test_read_from_other_schema(self, mock_connect):

        inst = Postgres(self.source, OPTIONS)
        inst.tables = [
            {'value': 'my_schema.foo_bar'},
            {'value': 'your_schema.bar_foo'}
        ]
        cursor_return_value = mock_connect.return_value.cursor.return_value
        mock_data = [self.mock_recs[:1], []] * 2
        cursor_return_value.fetchall.side_effect = mock_data

        expected = [
            ('my_schema', 'foo_bar'),
            (None, None),
            ('your_schema', 'bar_foo')
        ]

        for expected_schema, expected_table in expected:
            result = inst.read()
            if result:
                schema = result[0]['__schemaname']
                table = result[0]['__tablename']

                self.assertEqual(schema, expected_schema)
                self.assertEqual(table, expected_table)

    @mock.patch("psycopg2.connect")
    def test_incremental(self, mock_connect):
        inst = Postgres(self.source, OPTIONS)
        inst.tables = [{'value': 'schema.foo'}]
        inst.read()

        q = ('DECLARE cur CURSOR FOR '
             'SELECT * FROM "schema"."foo" '
             'WHERE "id" >= \'1\' ORDER BY "id"')
        execute_mock = mock_connect.return_value.cursor.return_value.execute
        execute_mock.assert_has_calls([mock.call(q)], True)

    @mock.patch("psycopg2.connect")
    def test_schema_name(self, mock_connect):
        """Test schema name is used when queries and that both schema and table
        names are wrapped in enclosing quotes"""

        source = {
            "host": "test.database.name",
            "port": "5432",
            "db_name": "foobar",
            "username": "test",
            "password": "testpassword",
            "data_available": [
                {'value': 'schema.foo'}
            ]
        }
        inst = Postgres(source, OPTIONS)
        inst.read()

        q = 'DECLARE cur CURSOR FOR SELECT * FROM "schema"."foo"'
        execute_mock = mock_connect.return_value.cursor.return_value.execute
        execute_mock.assert_has_calls([mock.call(q)], True)

    @mock.patch("psycopg2.connect")
    def test_connect_auth_error(self, mock_connect):
        msg = 'authentication failed'
        mock_connect.side_effect = psycopg2.OperationalError(msg)
        with self.assertRaises(PanoplyException):
            get_tables(self.source)

    @mock.patch("psycopg2.connect")
    def test_connect_other_error(self, mock_connect):
        msg = 'something unexpected'
        mock_connect.side_effect = psycopg2.OperationalError(msg)
        with self.assertRaises(psycopg2.OperationalError):
            get_tables(self.source)

    @mock.patch("psycopg2.connect")
    def test_invalid_inckey(self, mock_connect):
        tables = [
            {'value': 'public.table1'}
        ]
        inst = Postgres(self.source, OPTIONS)
        inst.tables = tables
        mock_connector = mock.Mock()
        mock_connector.cursor.execute.side_effect = \
            psycopg2.errors.UndefinedColumn('column does not exist')
        inst.connector = mock_connector
        with self.assertRaises(PostgresInckeyError):
            inst.read()

    @mock.patch("psycopg2.connect")
    def test_connect_with_addr(self, mock_connect):
        source = {
            "addr": "test.database.name:5439/foobar",
            "username": "test",
            "password": "testpassword",
            "data_available": [{'value': 'schema.foo'}]
        }
        connect(source)

        mock_connect.assert_called_with(
            dsn="postgres://test.database.name:5439/foobar",
            user=source['username'],
            password=source['password'],
            connect_timeout=CONNECT_TIMEOUT
        )

    @mock.patch("psycopg2.connect")
    def test_connection_parameters(self, mock_connect):
        source = {
            "host": "test.database",
            "port": "5432",
            "username": "test",
            "password": "testpassword",
            "db_name": "foobar",
            "data_available": [{'value': 'schema.foo'}]
        }
        connect(source)

        mock_connect.assert_called_with(
            dsn="postgres://test.database:5432/foobar",
            user=source['username'],
            password=source['password'],
            connect_timeout=CONNECT_TIMEOUT
        )

    @mock.patch("postgresv2.postgresv2.Postgres.execute")
    @mock.patch("psycopg2.connect")
    def test_read_end_stream(self, mock_connect, mock_execute):
        """reads the entire table from the database and validates that the
        stream returns None to indicate the end"""
        tables = [
            {'value': 'public.table1'},
            {'value': 'public.table2'},
            {'value': 'public.table3'},
        ]

        inst = Postgres(self.source, OPTIONS)
        inst.tables = tables
        result_order = [
            self.mock_recs,
            [],
            self.mock_recs,
            [],
            self.mock_recs,
            []
        ]

        cursor_return_value = mock_connect.return_value.cursor.return_value
        cursor_return_value.fetchall.side_effect = result_order

        # First call to read
        result = inst.read()
        self.assertEqual(len(result), len(self.mock_recs))
        query = mock_execute.call_args_list[0][0][0]
        expected_query = 'FROM "public"."table1" ' \
                         'WHERE "id" >= \'1\' ' \
                         'ORDER BY "id"'
        self.assertTrue(expected_query in query)
        query = mock_execute.call_args_list[1][0][0]
        expected_query = 'FETCH FORWARD'
        self.assertTrue(expected_query in query)

        # Second call to read
        result = inst.read()
        self.assertEqual(result, [])
        query = mock_execute.call_args_list[2][0][0]
        expected_query = 'FETCH FORWARD'
        self.assertTrue(expected_query in query)

        # Third call to read
        result = inst.read()
        self.assertEqual(len(result), len(self.mock_recs))
        query = mock_execute.call_args_list[3][0][0]
        expected_query = 'FROM "public"."table2" ' \
                         'WHERE "id" >= \'1\' ' \
                         'ORDER BY "id"'
        self.assertTrue(expected_query in query)
        query = mock_execute.call_args_list[4][0][0]
        expected_query = 'FETCH FORWARD'
        self.assertTrue(expected_query in query)

        # Fourth call to read
        result = inst.read()
        self.assertEqual(result, [])
        query = mock_execute.call_args_list[5][0][0]
        expected_query = 'FETCH FORWARD'
        self.assertTrue(expected_query in query)

        # Fifth call to read
        result = inst.read()
        self.assertEqual(len(result), len(self.mock_recs))
        query = mock_execute.call_args_list[6][0][0]
        expected_query = 'FROM "public"."table3" ' \
                         'WHERE "id" >= \'1\' ' \
                         'ORDER BY "id"'
        self.assertTrue(expected_query in query)
        query = mock_execute.call_args_list[7][0][0]
        expected_query = 'FETCH FORWARD'
        self.assertTrue(expected_query in query)

        # Sixth call to read
        result = inst.read()
        self.assertEqual(result, [])
        query = mock_execute.call_args_list[8][0][0]
        expected_query = 'FETCH FORWARD'
        self.assertTrue(expected_query in query)

        end = inst.read()
        self.assertEqual(end, None)

    # Make sure that the state is reported and that the
    # output data contains a key __state
    @mock.patch("postgresv2.postgresv2.Postgres.state")
    @mock.patch("psycopg2.connect")
    def test_reports_state(self, mock_connect, mock_state):
        """before returning a batch of data, the sources state should be
        reported as well as having the state ID appended to each data object"""

        inst = Postgres(self.source, OPTIONS)
        table_name = 'my_schema.foo_bar'
        inst.tables = [{'value': table_name}]
        result_order = [self.mock_recs, []]
        cursor_return_value = mock_connect.return_value.cursor.return_value
        cursor_return_value.fetchall.side_effect = result_order

        rows = inst.read()
        state_id = rows[0]['__state']
        state_obj = dict([
            ('last_index', 0),
        ])

        msg = 'State ID is not the same in all rows!'
        for row in rows:
            self.assertEqual(row['__state'], state_id, msg)

        # State function was called with relevant table name and row count
        mock_state.assert_called_with(state_id, state_obj)

    @mock.patch("postgresv2.postgresv2.Postgres.state")
    @mock.patch("psycopg2.connect")
    def test_no_state_for_empty_results(self, mock_connect, mock_state):
        """before returning a batch of data, the sources state should be
        reported as well as having the state ID appended to each data object"""

        inst = Postgres(self.source, OPTIONS)
        table_name = 'my_schema.foo_bar'
        inst.tables = [{'value': table_name}]
        result_order = [[], []]
        cursor_return_value = mock_connect.return_value.cursor.return_value
        cursor_return_value.fetchall.side_effect = result_order

        inst.read()

        # State function was called with relevant table name and row count
        mock_state.assert_not_called()

    @mock.patch("postgresv2.postgresv2.Postgres.execute")
    @mock.patch("psycopg2.connect")
    def test_recover_from_state(self, mock_connect, mock_execute):
        """continues to read a table from the saved state"""

        tables = [
            {'value': 'public.test1'},
            {'value': 'public.test2'},
            {'value': 'public.test3'},
        ]
        last_index = 1

        self.source['state'] = {
            'last_index': last_index
        }
        inst = Postgres(self.source, OPTIONS)
        inst.tables = tables
        cursor_return_value = mock_connect.return_value.cursor.return_value
        cursor_return_value.fetchall.return_value = [
            {'id': 101},
            {'id': 102},
            {'id': 103}
        ]

        inst.read()
        first_query = mock_execute.call_args_list[0][0][0]
        self.assertTrue("\"id\" >= '1'" in first_query)
        self.assertTrue('FROM "public"."test2"' in first_query)

    def test_remove_state_from_source(self):
        """ once extracted, the state object is removed from the source """
        last_index = 3
        state = {
            'last_index': last_index,
        }
        self.source['state'] = state
        inst = Postgres(self.source, OPTIONS)

        self.assertEqual(inst.index, last_index)
        # No state key should be inside the source definition
        self.assertIsNone(inst.source.get('state', None))

    @mock.patch("postgresv2.postgresv2.Postgres.execute")
    @mock.patch("psycopg2.connect")
    def test_batch_size(self, mock_connect, mock_execute):
        customBatchSize = 42
        self.source['__batchSize'] = customBatchSize
        inst = Postgres(self.source, OPTIONS)
        inst.tables = [{'value': 'my_schema.foo_bar'}]

        cursor_return_value = mock_connect.return_value.cursor.return_value
        cursor_return_value.fetchall.return_value = self.mock_recs

        inst.read()
        second_query = mock_execute.call_args_list[1][0][0]
        txt = 'FETCH FORWARD {}'.format(customBatchSize)
        self.assertTrue(second_query.startswith(txt))

    def test_reset_query_on_error(self):
        inst = Postgres(self.source, OPTIONS)
        mock_connector = mock.Mock()
        mock_connector.cursor.execute.side_effect = \
            psycopg2.DatabaseError('oh noes!')
        inst.connector = mock_connector
        with self.assertRaises(psycopg2.DatabaseError):
            inst.execute('SELECT 1')

        # The connector.loaded variable should have been reset to 0 in order to
        # reset the query and start from the begining.
        self.assertEqual(inst.connector.loaded, 0)
        self.assertEqual(inst.connector.cursor, None)

    @mock.patch("psycopg2.connect")
    def test_retry_cursor_from_last_value(self, mock_connect):
        inst = Postgres(self.source, OPTIONS)
        inst.tables = [{'value': 'my_schema.foo_bar'}]
        inst.inckey = None
        inst.incval = None
        inst.keys = ['col1', 'col2']
        records = [
            {'id': 1, 'col1': 'foo1', 'col2': 'bar1'},
            {'id': 2, 'col1': 'foo2', 'col2': 'bar2'},
            {'id': 3, 'col1': 'foo3', 'col2': 'bar3'}
        ]

        cursor_execute = mock_connect.return_value.cursor.return_value.execute
        cursor_execute.side_effect = [
            lambda *args: None,
            lambda *args: None,
            psycopg2.DatabaseError('TestRetriesError'),
            lambda *args: None,
            lambda *args: None
        ]
        cursor_return_value = mock_connect.return_value.cursor.return_value
        cursor_return_value.fetchall.return_value = records
        # First read no error
        inst.read()

        init_query = cursor_execute.call_args_list[0][0][0]
        expected_init_query = 'DECLARE cur CURSOR FOR SELECT * FROM ' \
                              '"my_schema"."foo_bar" ORDER BY "col1", "col2"'

        # Raise retry error and restart cursor
        inst.read()

        retried_query = cursor_execute.call_args_list[-2][0][0]
        expected_retried_query = 'DECLARE cur CURSOR FOR SELECT * FROM ' \
                                 '"my_schema"."foo_bar" WHERE ' \
                                 '"col1" >= \'foo3\' AND "col2" >= \'bar3\' ' \
                                 'ORDER BY "col1", "col2"'

        self.assertEqual(init_query, expected_init_query)
        self.assertEqual(retried_query, expected_retried_query)

    @mock.patch("postgresv2.postgresv2.CONNECT_TIMEOUT", 0)
    @mock.patch("psycopg2.connect")
    def test_read_retries(self, mock_connect):
        inst = Postgres(self.source, OPTIONS)
        inst.tables = [{'value': 'my_schema.foo_bar'}]
        mock_connect.side_effect = psycopg2.DatabaseError('TestRetriesError')
        with self.assertRaises(psycopg2.DatabaseError):
            inst.read()

        self.assertEqual(mock_connect.call_count, MAX_RETRIES)

    @mock.patch("psycopg2.connect")
    def test_use_inckey_as_index(self, mock_connect):
        inst = Postgres(self.source, OPTIONS)
        inst.tables = [{'value': 'my_schema.foo_bar'}]

        cursor_return_value = mock_connect.return_value.cursor.return_value
        cursor_return_value.fetchall.return_value = [{'id': 1, 'col1': 1}]

        inst.read()

        expected = [self.source['inckey']]
        result = inst.keys
        self.assertEqual(result, expected)

    def test_get_query_with_multicolumn_index(self):
        keys = ['col1', 'col2']
        last_values = ['1', '2']
        schema = 'public'
        table = 'test'

        result = get_query(
            schema,
            table,
            keys,
            last_values
        )
        expected = 'SELECT * FROM "public"."test" ' \
                   'WHERE "col1" >= \'1\' AND "col2" >= \'2\' ' \
                   'ORDER BY "col1", "col2"'

        self.assertEqual(result, expected)

    def test_get_query_without_last_values(self):
        keys = ['col1', 'col2']
        last_values = None
        schema = 'public'
        table = 'test'

        result = get_query(
            schema,
            table,
            keys,
            last_values
        )
        expected = 'SELECT * FROM "public"."test" ' \
                   'ORDER BY "col1", "col2"'

        self.assertEqual(result, expected)

    def test_get_query_without_index(self):
        keys = []
        last_values = None
        schema = 'public'
        table = 'test'

        result = get_query(
            schema,
            table,
            keys,
            last_values
        )
        expected = 'SELECT * FROM "public"."test"'

        self.assertEqual(result, expected)

    def test_choose_index_primary_key(self):
        keys = [
            {
                'attname': 'col1',
                'indnatts': 1,
                'indisunique': True,
                'indisprimary': False,
                'column_order': 1
            },
            {
                'attname': 'id',
                'indnatts': 1,
                'indisunique': True,
                'indisprimary': True,
                'column_order': 1
            },
            {
                'attname': 'col2',
                'indnatts': 1,
                'indisunique': True,
                'indisprimary': False,
                'column_order': 1
            }
        ]

        result = choose_index(keys)
        expected = ['id']

        self.assertEqual(result, expected)

    def test_choose_index_unique_key(self):
        keys = [
            {
                'attname': 'col1',
                'indnatts': 1,
                'indisunique': True,
                'indisprimary': False,
                'column_order': 1
            },
            {
                'attname': 'col2',
                'indnatts': 1,
                'indisunique': False,
                'indisprimary': False,
                'column_order': 1
            }
        ]

        result = choose_index(keys)
        expected = ['col1']

        self.assertEqual(result, expected)

    def test_choose_index_non_unique_key(self):
        keys = [
            {
                'attname': 'col1',
                'indnatts': 1,
                'indisunique': False,
                'indisprimary': False,
                'column_order': 1
            },
            {
                'attname': 'col2',
                'indnatts': 1,
                'indisunique': False,
                'indisprimary': False,
                'column_order': 1
            }
        ]

        result = choose_index(keys)
        expected = ['col1']

        self.assertEqual(result, expected)

    def test_choose_index_columns_order(self):
        keys = [
            {
                'attname': 'id',
                'indexrelid': 1,
                'indnatts': 1,
                'indisunique': False,
                'indisprimary': False,
                'column_order': 1
            },
            {
                'attname': 'col1',
                'indexrelid': 2,
                'indnatts': 2,
                'indisunique': True,
                'indisprimary': False,
                'column_order': 2
            },

            {
                'attname': 'col2',
                'indexrelid': 2,
                'indnatts': 2,
                'indisunique': True,
                'indisprimary': False,
                'column_order': 1
            }
        ]

        result = choose_index(keys)
        expected = ['col2', 'col1']

        self.assertEqual(result, expected)

    def test_validate_host_and_port(self):
        valid_sources = [
            {"addr": "test.database.name:5439/postgres"},
            {"host": "test.database.name", "port": "1234"},
            {"host": "123.45.67.89", "port": "1234"}
        ]

        for source in valid_sources:
            validate_host_and_port(source)

        invalid_sources = [
            {"host": "test.database.name", "port": "12 34"},
            {"host": "https://test.database.name", "port": "1234"},
            {"host": "test.database. name", "port": "1234"}
        ]

        for source in invalid_sources:
            with self.assertRaises(PostgresValidationError):
                validate_host_and_port(source)


if __name__ == "__main__":
    unittest.main()
