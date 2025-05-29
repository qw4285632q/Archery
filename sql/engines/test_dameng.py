# -*- coding: UTF-8 -*-
import unittest
from unittest.mock import patch, MagicMock

# Assuming dmPython might not be available in the test environment,
# we might need to mock it at the module level if DamengEngine imports it directly.
# If dmPython is imported inside methods, that's easier to handle.
# For now, let's assume we might need to handle its absence.
try:
    import dmPython  # Try to import it for type hinting or Error classes
except ImportError:
    # If dmPython is not available, create a placeholder for its Error class if referenced in exceptions
    class dmPython:  # Placeholder class
        class Error(Exception):  # Placeholder Error
            pass

from sql.engines.models import ResultSet, ReviewSet, ReviewEntry
from sql.engines.dameng import DamengEngine
from sql.models import Instance  # For creating a mock instance object


class TestDamengEngine(unittest.TestCase):

    def setUp(self):
        # Create a mock Instance object that the engine expects
        self.mock_instance = Instance(
            instance_name='test_dameng_instance',
            host='localhost',
            port=5236,
            user='testuser',
            # Provide a way to access password, assuming raw_password or similar
            # If PasswordMixin is active, this needs care.
            # For testing, we can bypass complex password retrieval.
        )
        # Mock the get_username_password method directly on the instance for simplicity
        self.mock_instance.get_username_password = MagicMock(return_value=('testuser', 'testpassword'))

        self.engine = DamengEngine(instance=self.mock_instance)

    @patch('sql.engines.dameng.dmPython', create=True)  # create=True if dmPython might not be importable
    def test_get_connection_success(self, mock_dmPython_module):
        mock_connect_instance = MagicMock()
        mock_dmPython_module.connect.return_value = mock_connect_instance

        conn = self.engine.get_connection(db_name='SCHEMA1')

        mock_dmPython_module.connect.assert_called_once_with(
            user='testuser',
            password='testpassword',
            server='localhost',
            port=5236
        )
        self.assertEqual(conn, mock_connect_instance)
        self.assertEqual(self.engine.conn, mock_connect_instance)

        # Test if connection is reused
        conn2 = self.engine.get_connection(db_name='SCHEMA1')
        self.assertEqual(conn2, mock_connect_instance)
        mock_dmPython_module.connect.assert_called_once()

        self.engine.conn = None

    @patch('sql.engines.dameng.dmPython', create=True)
    def test_get_connection_failure(self, mock_dmPython_module):
        # Ensure the mock dmPython module has the Error class for exception handling
        mock_dmPython_module.Error = type('dmPythonError', (Exception,), {})  # Create a mock Error class
        mock_dmPython_module.connect.side_effect = mock_dmPython_module.Error("Connection failed")

        with self.assertRaisesRegex(Exception, "Dameng connection failed: Connection failed"):
            self.engine.get_connection(db_name='SCHEMA1')
        self.assertIsNone(self.engine.conn)

    @patch.object(DamengEngine, 'get_connection')
    def test_query_success(self, mock_get_connection):
        mock_conn_instance = MagicMock()
        mock_cursor_instance = MagicMock()
        mock_get_connection.return_value = mock_conn_instance
        mock_conn_instance.cursor.return_value = mock_cursor_instance

        mock_cursor_instance.description = [('col1',), ('col2',)]
        mock_cursor_instance.fetchall.return_value = [('val1', 'val2'), ('val3', 'val4')]
        mock_cursor_instance.rowcount = 2  # Or use len(fetchall_return_value) if rowcount is for DML

        sql = "SELECT * FROM test_table"
        result = self.engine.query(db_name='SCHEMA1', sql=sql, parameters=['param1'])

        mock_get_connection.assert_called_once_with(db_name='SCHEMA1')
        mock_conn_instance.cursor.assert_called_once()
        mock_cursor_instance.execute.assert_called_once_with(sql, ['param1'])
        mock_cursor_instance.fetchall.assert_called_once()

        self.assertIsNone(result.error)
        self.assertEqual(result.column_list, ['col1', 'col2'])
        self.assertEqual(result.rows, [('val1', 'val2'), ('val3', 'val4')])
        self.assertEqual(result.affected_rows, 2)  # Based on len(rows) in the code
        mock_cursor_instance.close.assert_called_once()
        mock_conn_instance.close.assert_called_once()  # Assuming close_conn=True default

    @patch('sql.engines.dameng.dmPython', create=True)  # Mock dmPython at the module level
    @patch.object(DamengEngine, 'get_connection')
    def test_query_failure_db_error(self, mock_get_connection, mock_dmPython_module):
        mock_conn_instance = MagicMock()
        mock_cursor_instance = MagicMock()
        mock_get_connection.return_value = mock_conn_instance
        mock_conn_instance.cursor.return_value = mock_cursor_instance

        mock_dmPython_module.Error = type('dmPythonError', (Exception,), {})  # Mock the error class
        mock_cursor_instance.execute.side_effect = mock_dmPython_module.Error("DB query error")

        sql = "SELECT * FROM test_table"
        result = self.engine.query(db_name='SCHEMA1', sql=sql)

        self.assertIsNotNone(result.error)
        self.assertIn("Dameng query failed: DB query error", result.error)
        self.assertEqual(len(result.rows), 0)

    def test_filter_sql_limit(self):
        sql = "select * from test"
        limit_num = 10
        expected_sql = "select * from test LIMIT 10"
        filtered_sql = self.engine.filter_sql(sql=sql, limit_num=limit_num)
        self.assertEqual(filtered_sql, expected_sql)

    def test_filter_sql_no_limit_for_non_select(self):
        sql = "update test set col1 = 1"
        limit_num = 10
        filtered_sql = self.engine.filter_sql(sql=sql, limit_num=limit_num)
        self.assertEqual(filtered_sql, sql)

    @patch.object(DamengEngine, 'query')
    def test_get_all_databases(self, mock_query_method):
        mock_result_set = ResultSet()
        mock_result_set.rows = [('DB1',), ('DB2',)]
        mock_query_method.return_value = mock_result_set

        result = self.engine.get_all_databases()

        expected_sql = "SELECT NAME FROM SYSOBJECTS WHERE TYPE$ = 'SCH';"
        mock_query_method.assert_called_once_with(sql=expected_sql)
        self.assertEqual(result.rows, [('DB1',), ('DB2',)])

    @patch.object(DamengEngine, 'query')
    def test_get_all_tables(self, mock_query_method):
        mock_result_set = ResultSet()
        mock_result_set.rows = [('TABLE1',), ('TABLE2',)]
        mock_query_method.return_value = mock_result_set
        db_name = "MYSCHEMA"

        result = self.engine.get_all_tables(db_name=db_name)

        expected_sql = "SELECT TABLE_NAME FROM ALL_TABLES WHERE OWNER = %s;"
        # Note: .upper() is used in the actual method, so the mocked parameter should reflect that if checking strictly
        mock_query_method.assert_called_once_with(db_name=db_name, sql=expected_sql, parameters=[db_name.upper()])
        self.assertEqual(result.rows, [('TABLE1',), ('TABLE2',)])

    # TODO: Add tests for get_all_columns_by_tb, describe_table
    # TODO: Add tests for execute_check, execute (these are more complex due to ReviewSet and transaction mocking)


if __name__ == '__main__':
    unittest.main()
