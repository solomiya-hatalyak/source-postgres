import mock
import unittest
import psycopg2
import postgres
from collections import OrderedDict
from postgres.source import (
    Postgres,
    connect,
    get_incremental,
    get_query,
    key_strategy,
    SQL_GET_KEYS,
    SQL_GET_COLUMNS
)
from panoply import PanoplyException

OPTIONS = {
    "logger": lambda *msgs: None,  # no-op logger
}

MOCK_MAX_VALUE = 100


def mock_max_value(*args):
    column = args[2]
    if column:
        return MOCK_MAX_VALUE
    return None


def mock_table_metadata(*args):
    if args[0] == SQL_GET_COLUMNS:
        return [{'attname': 'id'}]
    return []


class TestPostgres(unittest.TestCase):
    def setUp(self):
        self.source = {
            "addr": "test.database.name/foobar",
            "user": "test",
            "password": "testpassword",
            "inckey": "inckey",
            "incval": "incval"
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
             'table_type': 'BASE TABLE', 'name': 'dbo.testNoUnique'},
            {'table_schema': 'dbo', 'table_name': 'testNoIndex',
             'table_type': 'BASE TABLE', 'name': 'dbo.testNoIndex'},
            {'table_schema': 'SalesLT', 'table_name': 'Customer',
             'table_type': 'BASE TABLE', 'name': 'SalesLT.Customer'},
            {'table_schema': 'SalesLT', 'table_name': 'ProductModel',
             'table_type': 'BASE TABLE', 'name': 'SalesLT.ProductModel'},
            {'table_schema': 'mySchema', 'table_name': 'someTable',
             'table_type': 'VIEW', 'name': 'mySchema.someTable (VIEW)'}
        ]

        inst = Postgres(self.source, OPTIONS)
        m.return_value.cursor.return_value.fetchall.return_value = mock_tables

        tables = inst.get_tables()
        self.assertEqual(len(tables), len(mock_tables))
        for x in range(0, len(tables)):
            mtable = mock_tables[x]
            v = '{}.{}'.format(mtable["table_schema"], mtable["table_name"])

            self.assertEqual(tables[x]['name'], mtable['name'])
            self.assertEqual(tables[x]['value'], v)

    @mock.patch.object(Postgres, 'get_max_value', side_effect=mock_max_value)
    @mock.patch.object(Postgres, 'get_table_metadata',
                       side_effect=mock_table_metadata)
    @mock.patch("psycopg2.connect")
    def test_read(self, mock_connect, _, __):
        """reads a table from the database and validates that each row
        has a __tablename and __schemaname column"""

        inst = Postgres(self.source, OPTIONS)
        inst.tables = [{'value': 'my_schema.foo_bar'}]
        cursor_return_value = mock_connect.return_value.cursor.return_value
        cursor_return_value.fetchall.return_value = self.mock_recs

        rows = inst.read()
        self.assertEqual(len(rows), len(self.mock_recs))
        for x in range(0, len(rows)):
            self.assertEqual(rows[x]['__tablename'], 'foo_bar')
            self.assertEqual(rows[x]['__schemaname'], 'my_schema')

    @mock.patch.object(Postgres, 'get_max_value', side_effect=mock_max_value)
    @mock.patch.object(Postgres, 'get_table_metadata',
                       side_effect=mock_table_metadata)
    @mock.patch("psycopg2.connect")
    def test_read_from_other_schema(self, mock_connect, mock_metadata, __):

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
            ('my_schema', 'foo_bar'),
            ('your_schema', 'bar_foo'),
            ('your_schema', 'bar_foo')
        ]

        for expected_schema, expected_table in expected:
            inst.read()
            _, schema, table = mock_metadata.call_args[0]

            self.assertEqual(schema, expected_schema)
            self.assertEqual(table, expected_table)

    @mock.patch.object(Postgres, 'get_max_value', side_effect=mock_max_value)
    @mock.patch.object(Postgres, 'get_table_metadata',
                       side_effect=mock_table_metadata)
    @mock.patch("psycopg2.connect")
    def test_incremental(self, mock_connect, _, __):
        inst = Postgres(self.source, OPTIONS)
        inst.tables = [{'value': 'schema.foo'}]
        inst.read()

        q = ('DECLARE cur CURSOR FOR '
             'SELECT * FROM "schema"."foo" WHERE ("inckey" >= \'incval\' '
             'AND "inckey" <= \'100\') '
             'ORDER BY "id","inckey"')
        execute_mock = mock_connect.return_value.cursor.return_value.execute
        execute_mock.assert_has_calls([mock.call(q)], True)

    @mock.patch.object(Postgres, 'get_table_metadata', return_value=[])
    @mock.patch("psycopg2.connect")
    def test_schema_name(self, mock_connect, _):
        """Test schema name is used when queries and that both schema and table
        names are wrapped in enclosing quotes"""

        source = {
            "addr": "test.database.name/foobar",
            "user": "test",
            "password": "testpassword",
            "tables": [
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
        inst = Postgres(self.source, OPTIONS)
        inst.tables = [{'value': 'schema.foo'}]
        msg = 'authentication failed'
        mock_connect.side_effect = psycopg2.OperationalError(msg)
        with self.assertRaises(PanoplyException):
            inst.get_tables()

    @mock.patch("psycopg2.connect")
    def test_connect_other_error(self, mock_connect):
        inst = Postgres(self.source, OPTIONS)
        inst.tables = [{'value': 'schema.foo'}]
        msg = 'something unexpected'
        mock_connect.side_effect = psycopg2.OperationalError(msg)
        with self.assertRaises(psycopg2.OperationalError):
            inst.get_tables()

    @mock.patch("psycopg2.connect")
    def test_default_port(self, mock_connect):
        source = {
            "addr": "test.database.name/foobar",
            "user": "test",
            "password": "testpassword",
            "tables": [{'value': 'schema.foo'}]
        }
        inst = Postgres(source, OPTIONS)
        inst.read()

        mock_connect.assert_called_with(
            host="test.database.name",
            port=5432,
            user=source['user'],
            password=source['password'],
            dbname="foobar",
            connect_timeout=postgres.source.CONNECT_TIMEOUT
        )

    @mock.patch("psycopg2.connect")
    def test_custom_port(self, mock_connect):
        source = {
            "addr": "test.database.name:5439/foobar",
            "user": "test",
            "password": "testpassword",
            "tables": [{'value': 'schema.foo'}]
        }
        inst = Postgres(source, OPTIONS)
        inst.read()

        mock_connect.assert_called_with(
            host="test.database.name",
            port=5439,
            user=source['user'],
            password=source['password'],
            dbname="foobar",
            connect_timeout=postgres.source.CONNECT_TIMEOUT
        )

    @mock.patch.object(Postgres, 'get_max_value', side_effect=mock_max_value)
    @mock.patch.object(Postgres, 'get_table_metadata')
    @mock.patch("postgres.source.Postgres.execute")
    @mock.patch("psycopg2.connect")
    def test_read_end_stream(self, mock_connect, mock_execute, mock_metadata,
                             _):
        """reads the entire table from the database and validates that the
        stream returns None to indicate the end"""
        tables = [
            {'value': 'public.table1'},
            {'value': 'public.table2'},
            {'value': 'public.table3'},
        ]

        mock_metadata.side_effect = [
            [{'attname': 'col1'}],
            [{'attname': 'col2'}],
            [{'attname': 'col3'}],
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
                         'WHERE ("inckey" >= \'incval\' ' \
                         'AND "inckey" <= \'100\') '\
                         'ORDER BY "col1","inckey"'
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
                         'WHERE ("inckey" >= \'incval\' ' \
                         'AND "inckey" <= \'100\') '\
                         'ORDER BY "col2","inckey"'
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
                         'WHERE ("inckey" >= \'incval\' ' \
                         'AND "inckey" <= \'100\') '\
                         'ORDER BY "col3","inckey"'
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
    @mock.patch.object(Postgres, 'get_max_value',
                       side_effect=mock_max_value)
    @mock.patch.object(Postgres, 'get_table_metadata',
                       side_effect=mock_table_metadata)
    @mock.patch("postgres.source.Postgres.state")
    @mock.patch("psycopg2.connect")
    def test_reports_state(self, mock_connect, mock_state, _, __):
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

    @mock.patch.object(Postgres, 'get_max_value', side_effect=mock_max_value)
    @mock.patch.object(Postgres, 'get_table_metadata', return_value=[])
    @mock.patch("postgres.source.Postgres.state")
    @mock.patch("psycopg2.connect")
    def test_no_state_for_empty_results(self, mock_connect, mock_state, _, __):
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

    @mock.patch.object(Postgres, 'get_max_value', side_effect=mock_max_value)
    @mock.patch.object(Postgres, 'get_table_metadata',
                       side_effect=mock_table_metadata)
    @mock.patch("postgres.source.Postgres.execute")
    @mock.patch("psycopg2.connect")
    def test_recover_from_state(self, mock_connect, mock_execute, _, __):
        """continues to read a table from the saved state"""

        tables = [
            {'value': 'public.test1'},
            {'value': 'public.test2'},
            {'value': 'public.test3'},
        ]
        last_index = 1

        self.source['state'] = {
            'last_index': last_index,
            'max_value': 100
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
        self.assertTrue("\"inckey\" >= 'incval' AND \"inckey\" <= '100'" in
                        first_query)
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

    @mock.patch.object(Postgres, 'get_max_value', side_effect=mock_max_value)
    @mock.patch.object(Postgres, 'get_table_metadata', return_value=[])
    @mock.patch("postgres.source.Postgres.execute")
    @mock.patch("psycopg2.connect")
    def test_batch_size(self, mock_connect, mock_execute, _, __):
        customBatchSize = 42
        self.source['__batchSize'] = customBatchSize
        inst = Postgres(self.source, OPTIONS)
        inst.tables = [{'value': 'my_schema.foo_bar'}]

        cursor_return_value = mock_connect.return_value.cursor.return_value
        cursor_return_value.fetchall.return_value = self.mock_recs

        inst.read()
        second_query = mock_execute.call_args_list[1][0][0]
        txt = 'FETCH FORWARD %s' % customBatchSize
        self.assertTrue(second_query.startswith(txt))

    def test_reset_query_on_error(self):
        inst = Postgres(self.source, OPTIONS)
        mock_cursor = mock.Mock()
        mock_cursor.execute.side_effect = psycopg2.DatabaseError('oh noes!')
        inst.cursor = mock_cursor
        with self.assertRaises(psycopg2.DatabaseError):
            inst.execute('SELECT 1')

        # The self.loaded variable should have been reset to 0 in order to
        # reset the query and start from the begining.
        self.assertEqual(inst.loaded, 0)
        self.assertEqual(inst.cursor, None)

    @mock.patch("postgres.source.CONNECT_TIMEOUT", 0)
    @mock.patch("psycopg2.connect")
    def test_read_retries(self, mock_connect):
        inst = Postgres(self.source, OPTIONS)
        inst.tables = [{'value': 'my_schema.foo_bar'}]
        mock_connect.side_effect = psycopg2.DatabaseError('TestRetriesError')
        with self.assertRaises(psycopg2.DatabaseError):
            inst.read()

        self.assertEqual(mock_connect.call_count, postgres.source.MAX_RETRIES)

    def test_get_query_without_state_and_incremental(self):
        inckey = ''
        incval = ''
        schema = 'public'
        table = 'test'
        keys = [
            {
                'attname': 'pk1',
                'indisunique': True,
                'indisprimary': True
            }
        ]
        max_value = None
        state = {}

        result = get_query(
            schema,
            table,
            inckey,
            incval,
            keys,
            max_value,
            state
        )
        expected = 'SELECT * FROM "public"."test" ORDER BY "pk1"'

        self.assertEqual(result, expected)

    def test_orderby_without_incremental(self):
        schema = 'public'
        table = 'test'
        inckey = ''
        incval = ''
        max_value = ''
        keys = [
            {
                'attname': 'pk1',
                'indisunique': True,
                'indisprimary': True
            }
        ]
        state = {}

        result = get_query(
            schema,
            table,
            inckey,
            incval,
            keys,
            max_value,
            state
        )
        expected = 'SELECT * FROM "public"."test" ORDER BY "pk1"'

        self.assertEqual(result, expected)

    def test_orderby_with_incremental(self):
        inckey = 'pk3'
        incval = ''
        max_value = 10
        schema = 'public'
        table = 'test'
        keys = [
            {
                'attname': 'pk1',
                'indisunique': True,
                'indisprimary': True
            },
            {
                'attname': 'pk2',
                'indisunique': True,
                'indisprimary': True
            }
        ]
        state = {}

        result = get_query(
            schema,
            table,
            inckey,
            incval,
            keys,
            max_value,
            state
        )
        expected = 'SELECT * FROM "public"."test" ORDER BY "pk1","pk2","pk3"'

        self.assertEqual(result, expected)

    def test_orderby_with_incremental_in_keys(self):
        inckey = 'pk2'
        incval = ''
        max_value = ''
        schema = 'public'
        table = 'test'
        keys = [
            {
                'attname': 'pk1',
                'indisunique': True,
                'indisprimary': True
            },
            {
                'attname': 'pk2',
                'indisunique': True,
                'indisprimary': True
            }
        ]
        state = {}

        result = get_query(
            schema,
            table,
            inckey,
            incval,
            keys,
            max_value,
            state
        )
        expected = 'SELECT * FROM "public"."test" ORDER BY "pk1","pk2"'

        self.assertEqual(result, expected)

    def test_where_without_state_and_incremental(self):
        inckey = ''
        incval = ''
        max_value = ''
        schema = 'public'
        table = 'test'
        keys = [
            {
                'attname': 'pk1',
                'indisunique': True,
                'indisprimary': True
            },
            {
                'attname': 'pk2',
                'indisunique': True,
                'indisprimary': True
            }
        ]
        state = {}

        result = get_query(
            schema,
            table,
            inckey,
            incval,
            keys,
            max_value,
            state
        )
        expected = 'SELECT * FROM "public"."test" ORDER BY "pk1","pk2"'

        self.assertEqual(result, expected)

    def test_where_without_state_and_with_incremental(self):
        inckey = 'id'
        incval = 1
        max_value = 100
        schema = 'public'
        table = 'test'
        keys = [
            {
                'attname': 'pk1',
                'indisunique': True,
                'indisprimary': True
            },
            {
                'attname': 'pk2',
                'indisunique': True,
                'indisprimary': True
            }
        ]
        state = {}

        result = get_query(
            schema,
            table,
            inckey,
            incval,
            keys,
            max_value,
            state)
        expected = 'SELECT * FROM "public"."test" ' \
                   'WHERE ("id" >= \'1\' AND "id" <= \'100\') ' \
                   'ORDER BY "pk1","pk2","id"'

        self.assertEqual(result, expected)

    def test_where_with_state_and_without_incremental(self):
        inckey = ''
        incval = ''
        max_value = ''
        schema = 'public'
        table = 'test'
        keys = [
            {
                'attname': 'pk1',
                'indisunique': True,
                'indisprimary': True
            },
            {
                'attname': 'pk2',
                'indisunique': True,
                'indisprimary': True
            }
        ]
        state = OrderedDict([
            ('pk1', '1'),
            ('pk2', '1994-09-16')
        ])

        result = get_query(
            schema,
            table,
            inckey,
            incval,
            keys,
            max_value,
            state
        )
        expected = 'SELECT * FROM "public"."test" ' \
                   'WHERE ("pk1","pk2") >= (\'1\',\'1994-09-16\') '\
                   'ORDER BY "pk1","pk2"'

        self.assertEqual(result, expected)

    def test_where_with_single_column(self):
        inckey = ''
        incval = ''
        max_value = ''
        schema = 'public'
        table = 'test'
        keys = [
            {
                'attname': 'pk1',
                'indisunique': True,
                'indisprimary': True
            },
        ]
        state = OrderedDict([
            ('pk1', '1'),
        ])

        result = get_query(
            schema,
            table,
            inckey,
            incval,
            keys,
            max_value,
            state
        )
        expected = 'SELECT * FROM "public"."test" ' \
                   'WHERE "pk1" >= \'1\' ' \
                   'ORDER BY "pk1"'

        self.assertEqual(result, expected)

    def test_where_with_state_and_incremental(self):
        inckey = 'id'
        incval = 2
        max_value = 100
        schema = 'public'
        table = 'test'
        keys = [
            {
                'attname': 'pk1',
                'indisunique': True,
                'indisprimary': True
            },
        ]
        state = OrderedDict([
            ('pk1', '1'),
        ])

        result = get_query(
            schema,
            table,
            inckey,
            incval,
            keys,
            max_value,
            state
        )
        expected = 'SELECT * FROM "public"."test" ' \
                   'WHERE "pk1" >= \'1\' ' \
                   'AND ("id" >= \'2\' AND "id" <= \'100\') ' \
                   'ORDER BY "pk1","id"'

        self.assertEqual(result, expected)

    @mock.patch.object(Postgres, 'get_max_value', side_effect=mock_max_value)
    @mock.patch.object(Postgres, 'get_table_metadata')
    @mock.patch("postgres.source.CONNECT_TIMEOUT", 0)
    @mock.patch("psycopg2.connect")
    def test_retry_with_last_values(self, mock_connect, mock_metadata, _):

        mock_metadata.side_effect = lambda *args: [
            {'attname': 'col1', 'indisunique': True, 'indisprimary': True},
            {'attname': 'col2', 'indisunique': True, 'indisprimary': True}
        ]

        inst = Postgres(self.source, OPTIONS)
        inst.tables = [{'value': 'my_schema.foo_bar'}]
        inst.batch_size = 1

        cursor_execute = mock_connect.return_value.cursor.return_value.execute
        cursor_execute.side_effect = [
            lambda *args: None,
            lambda *args: None,
            psycopg2.DatabaseError('TestRetriesError'),
            lambda *args: None,
            lambda *args: None,
        ]

        cursor_return_value = mock_connect.return_value.cursor.return_value
        cursor_return_value.fetchall.return_value = self.mock_recs

        # First read no error
        inst.read()
        # Raise retry error
        inst.read()

        # Extract mock call arguments
        args = mock_connect.return_value.cursor.return_value\
            .execute.call_args_list
        args = [r[0] for r, _ in args]
        args = filter(lambda x: 'DECLARE' in x, args)

        # Second DECLARATION of cursor should start from last row fetched
        self.assertTrue('WHERE ("col1","col2") >= (\'foo3\',\'bar3\')' in
                        args[1])

    @mock.patch("postgres.source.CONNECT_TIMEOUT", 0)
    @mock.patch("psycopg2.connect")
    def test_query_with_primary_keys(self, mock_connect):
        inst = Postgres(self.source, OPTIONS)
        inst.conn, inst.cursor = connect(self.source)

        cursor_return_value = mock_connect.return_value.cursor.return_value
        cursor_return_value.fetchall.return_value = [
            {'attname': 'pk1', 'indisunique': True, 'indisprimary': True},
            {'attname': 'pk2', 'indisunique': True, 'indisprimary': True},
            {'attname': 'pk2', 'indisunique': True, 'indisprimary': False},
        ]

        schema = 'public'
        table = 'test'
        inckey = ''
        incval = ''
        max_value = 100
        keys = inst.get_table_metadata(SQL_GET_KEYS, schema, table)
        keys = key_strategy(keys)
        state = None

        result = get_query(
            schema,
            table,
            inckey,
            incval,
            keys,
            max_value,
            state
        )
        expected = 'SELECT * FROM "public"."test" ORDER BY "pk1","pk2"'

        self.assertEqual(result, expected)

    @mock.patch("postgres.source.CONNECT_TIMEOUT", 0)
    @mock.patch("psycopg2.connect")
    def test_query_with_unique_keys(self, mock_connect):
        inst = Postgres(self.source, OPTIONS)
        inst.conn, inst.cursor = connect(self.source)

        cursor_return_value = mock_connect.return_value.cursor.return_value
        cursor_return_value.fetchall.return_value = [
            {
                'attname': 'idx1',
                'indisunique': True,
                'indisprimary': False,
                'indnatts': 2,
                'indexrelid': 'idx12'
            },
            {
                'attname': 'idx2',
                'indisunique': True,
                'indisprimary': False,
                'indnatts': 2,
                'indexrelid': 'idx12'
            },
            {
                'attname': 'idx3',
                'indisunique': True,
                'indisprimary': False,
                'indnatts': 1,
                'indexrelid': 'idx3123'
            },
        ]

        schema = 'public'
        table = 'test'
        inckey = ''
        incval = ''
        max_value = ''
        keys = inst.get_table_metadata(SQL_GET_KEYS, schema, table)
        keys = key_strategy(keys)
        state = None

        result = get_query(
            schema,
            table,
            inckey,
            incval,
            keys,
            max_value,
            state
        )
        expected = 'SELECT * FROM "public"."test" ORDER BY "idx1","idx2"'

        self.assertEqual(result, expected)

    @mock.patch("postgres.source.CONNECT_TIMEOUT", 0)
    @mock.patch("psycopg2.connect")
    def test_query_with_non_unique_keys(self, mock_connect):
        inst = Postgres(self.source, OPTIONS)
        inst.conn, inst.cursor = connect(self.source)

        cursor_return_value = mock_connect.return_value.cursor.return_value
        cursor_return_value.fetchall.return_value = [
            {
                'attname': 'idx3',
                'indisunique': False,
                'indisprimary': False,
                'indnatts': 1,
                'indexrelid': 'idx3123'
            },
        ]

        schema = 'public'
        table = 'test'
        inckey = ''
        incval = ''
        max_value = 100
        keys = inst.get_table_metadata(SQL_GET_KEYS, schema, table)
        keys = key_strategy(keys)
        state = None

        result = get_query(
            schema,
            table,
            inckey,
            incval,
            keys,
            max_value,
            state
        )
        expected = 'SELECT * FROM "public"."test" ORDER BY "idx3"'

        self.assertEqual(result, expected)

    @mock.patch.object(Postgres, 'get_max_value', side_effect=mock_max_value)
    @mock.patch("postgres.source.CONNECT_TIMEOUT", 0)
    @mock.patch("psycopg2.connect")
    def test_query_with_no_keys(self, mock_connect, _):
        inst = Postgres(self.source, OPTIONS)
        inst.tables = [{'value': 'my_schema.foo_bar'}]

        cursor_return_value = mock_connect.return_value.cursor.return_value
        cursor_return_value.fetchall.side_effect = [
            [],
            [
                {'attname': 'id', 'data_type': 'integer'},
                {'attname': 'name', 'data_type': 'text'},
            ],
            [],
        ]

        cursor_execute = mock_connect.return_value.cursor.return_value.execute
        cursor_execute.return_value = lambda *args: None

        inst.read()

        # Extract mock call arguments
        args = mock_connect.return_value.cursor.return_value \
            .execute.call_args_list
        args = [r[0] for r, _ in args]
        args = filter(lambda x: 'DECLARE' in x, args)

        expected = 'DECLARE cur ' \
                   'CURSOR FOR SELECT * FROM "my_schema"."foo_bar" ' \
                   "WHERE (\"inckey\" >= 'incval' AND \"inckey\" <= '100') " \
                   'ORDER BY "id","inckey"'

        self.assertEqual(args[0], expected)

    def test_get_incremental_key_in_where(self):
        where = ''
        inckey = 'inckey'
        incval = 1
        max_value = 100

        result = get_incremental(where, inckey, incval, max_value)
        expected = "(\"inckey\" >= '1' AND \"inckey\" <= '100')"

        self.assertEqual(result, expected)

    def test_get_incremental_key_not_in_where(self):
        where = "(inckey, id) >= ('1', '2)"
        inckey = 'inckey'
        incval = 1
        max_value = 100

        result = get_incremental(where, inckey, incval, max_value)
        expected = "inckey <= '100'"

        self.assertEqual(result, expected)

    @mock.patch("postgres.source.Postgres.execute")
    @mock.patch("psycopg2.connect")
    def test_get_table_metadata(self, mock_connect, mock_execute):
        testcases = [
            {'schema': 'public', 'table': 'Errors'},
            {'schema': 'public', 'table': 'Notifications'},
        ]

        stream = Postgres(self.source, OPTIONS)
        stream.conn, stream.cursor = connect(self.source)

        for i, tcase in enumerate(testcases):
            schema, table = tcase['schema'], tcase['table']
            stream.get_table_metadata(SQL_GET_KEYS, schema, table)

            # make sure tables and schema are properly escaped when
            # casting to regclass (as it implicitly converts to lowercase)
            query = mock_execute.call_args_list[i][0][0]
            self.assertIn('\'"%s"."%s"\'::regclass' % (schema, table), query)


if __name__ == "__main__":
    unittest.main()
