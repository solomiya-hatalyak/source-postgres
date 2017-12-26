import mock
import unittest
import psycopg2
from postgres.source import Postgres
from panoply import PanoplyException

OPTIONS = {
    "logger": lambda *msgs: None,  # no-op logger
}


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
        '''gets the list of tables from the database'''

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

    # read a table from the database
    @mock.patch("psycopg2.connect")
    def test_read(self, m):
        '''reads a table from the database and validates that each row
        has a __tablename and __schemaname column'''

        inst = Postgres(self.source, OPTIONS)
        inst.tables = [{'value': 'my_schema.foo_bar'}]
        m.return_value.cursor.return_value.fetchall.return_value = self.mock_recs

        rows = inst.read()
        self.assertEqual(len(rows), len(self.mock_recs))
        for x in range(0, len(rows)):
            self.assertEqual(rows[x]['__tablename'], 'foo_bar')
            self.assertEqual(rows[x]['__schemaname'], 'my_schema')

    @mock.patch("psycopg2.connect")
    def test_incremental(self, m):
        inst = Postgres(self.source, OPTIONS)
        inst.tables = [{'value': 'schema.foo'}]
        inst.read()

        q = ('DECLARE cur CURSOR FOR '
             'SELECT * FROM "schema"."foo" WHERE inckey > \'incval\'')
        execute_mock = m.return_value.cursor.return_value.execute
        execute_mock.assert_has_calls([mock.call(q)], True)

    @mock.patch("psycopg2.connect")
    def test_schema_name(self, m):
        '''Test schema name is used when queries and that both schema and table
        names are wrapped in enclosing quotes'''

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
        execute_mock = m.return_value.cursor.return_value.execute
        execute_mock.assert_has_calls([mock.call(q)], True)

    @mock.patch("psycopg2.connect")
    def test_connect_error(self, m):
        inst = Postgres(self.source, OPTIONS)
        inst.tables = [{'value': 'schema.foo'}]
        m.side_effect = psycopg2.OperationalError('Mock Error')
        with self.assertRaises(PanoplyException):
            inst.read()

    @mock.patch("psycopg2.connect")
    def test_default_port(self, m):
        source = {
            "addr": "test.database.name/foobar",
            "user": "test",
            "password": "testpassword",
            "tables": [{'value': 'schema.foo'}]
        }
        inst = Postgres(source, OPTIONS)
        inst.read()

        m.assert_called_with(
            host="test.database.name",
            port=5432,
            user=source['user'],
            password=source['password'],
            dbname="foobar"
        )

    @mock.patch("psycopg2.connect")
    def test_custom_port(self, m):
        source = {
            "addr": "test.database.name:5439/foobar",
            "user": "test",
            "password": "testpassword",
            "tables": [{'value': 'schema.foo'}]
        }
        inst = Postgres(source, OPTIONS)
        inst.read()

        m.assert_called_with(
            host="test.database.name",
            port=5439,
            user=source['user'],
            password=source['password'],
            dbname="foobar"
        )

    # Make sure the stream ends properly
    @mock.patch("psycopg2.connect")
    def test_read_end_stream(self, m):
        '''reads the entire table from the database and validates that the
        stream returns None to indicate the end'''

        inst = Postgres(self.source, OPTIONS)
        inst.tables = [{'value': 'my_schema.foo_bar'}]
        result_order = [self.mock_recs, []]
        m.return_value.cursor.return_value.fetchall.side_effect = result_order

        rows = inst.read()
        self.assertEqual(len(rows), len(self.mock_recs))

        empty = inst.read()
        self.assertEqual(empty, [])
        end = inst.read()
        self.assertEqual(end, None)

    # Make sure that the state is reorted and that the
    # output data contains a key __state
    @mock.patch("postgres.source.Postgres.state")
    @mock.patch("psycopg2.connect")
    def test_reports_state(self, mock_connect, mock_state):
        '''before returning a batch of data, the sources state should be
        reported as well as having the state ID appended to each data object'''

        inst = Postgres(self.source, OPTIONS)
        table_name = 'my_schema.foo_bar'
        inst.tables = [{'value': table_name}]
        result_order = [self.mock_recs, []]
        mock_connect.return_value.cursor.return_value.fetchall.side_effect = result_order

        rows = inst.read()
        state_id = rows[0]['__state']
        state_obj = dict([(table_name, len(self.mock_recs))])

        msg = 'State ID is not the same in all rows!'
        for row in rows:
            self.assertEqual(row['__state'], state_id, msg)

        # State function was called with relevant table name and row count
        mock_state.assert_called_with(state_id, state_obj)


if __name__ == "__main__":
    unittest.main()
