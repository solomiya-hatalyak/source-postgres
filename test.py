import mock
import unittest
from postgres.source import Postgres

OPTIONS = {
    "logger": lambda *msgs: None,  # no-op logger
}


class TestPostgres(unittest.TestCase):
    def setUp(self):
        self.source = {
            "address": "test.database.name/foobar",
            "username": "test",
            "password": "testpassword"
        }

    def tearDown(self):
        self.source = None

    # fetches list of tables from database
    @mock.patch("psycopg2.connect")
    def test_get_tables(self, m):
        '''gets the list of tables from the database'''
        mock_tables = [
            {'table_schema': 'dbo', 'table_name': 'testnoUnique',
             'table_type': 'BASE TABLE', 'name': 'dbo.testnoUnique'},
            {'table_schema': 'dbo', 'table_name': 'testNoIndex',
             'table_type': 'BASE TABLE', 'name': 'dbo.testNoIndex'},
            {'table_schema': 'SalesLT', 'table_name': 'Customer',
             'table_type': 'BASE TABLE', 'name': 'SalesLT.Customer'},
            {'table_schema': 'SalesLT', 'table_name': 'ProductModel',
             'table_type': 'BASE TABLE', 'name': 'SalesLT.ProductModel'}
        ]

        inst = Postgres(self.source, OPTIONS)
        m.return_value.cursor.return_value.fetchall.return_value = mock_tables

        tables = inst.get_tables()
        self.assertEqual(len(tables), len(mock_tables))
        for x in range(0, len(tables)):
            self.assertEqual(tables[x], mock_tables[x]["name"])


    # read a table from the database
    @mock.patch("psycopg2.connect")
    def test_read(self, m):
        '''reads a table from the database and validates that each row
        has a __tablename column'''

        mock_recs = [
            {'id': 1, 'col1': 'foo1', 'col2': 'bar1'},
            {'id': 2, 'col1': 'foo2', 'col2': 'bar2'},
            {'id': 3, 'col1': 'foo3', 'col2': 'bar3'}
        ]

        inst = Postgres(self.source, OPTIONS)
        inst.tables = ['foo.bar']
        m.return_value.cursor.return_value.fetchall.return_value = mock_recs

        rows = inst.read()
        self.assertEqual(len(rows), len(mock_recs))
        for x in range(0, len(rows)):
            self.assertEqual(rows[x]['__tablename'], 'foo_bar')


if __name__ == "__main__":
    unittest.main()
