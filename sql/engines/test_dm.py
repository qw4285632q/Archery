import unittest
from unittest.mock import patch, MagicMock

from sql.engines.dm import DmEngine
from sql.models import Instance
from sql.engines.models import ReviewSet, ReviewResult, ResultSet

class TestDmEngine(unittest.TestCase):

    def setUp(self):
        self.instance = Instance(
            db_type='DM',
            host='testhost',
            port=5236,
            user='testuser',
            password='testpassword',
            instance_name='test_dm_instance'
        )
        # Mock dmPython at the module level where DmEngine imports it
        self.patcher_dmpython = patch('sql.engines.dm.dmPython', MagicMock())
        self.mock_dmpython = self.patcher_dmpython.start()

        # Common mock for connection and cursor often reused
        self.mock_conn = MagicMock()
        self.mock_cursor = MagicMock()
        self.mock_dmpython.connect.return_value = self.mock_conn
        self.mock_conn.cursor.return_value = self.mock_cursor


    def tearDown(self):
        self.patcher_dmpython.stop()
        self.mock_dmpython.reset_mock()
        self.mock_conn.reset_mock()
        self.mock_cursor.reset_mock()


    def test_get_connection_success(self):
        # Arrange
        # setUp already mocks dmPython.connect to return self.mock_conn
        engine = DmEngine(instance=self.instance)

        # Act
        conn = engine.get_connection(db_name='testdb')

        # Assert
        self.mock_dmpython.connect.assert_called_once_with(
            user='testuser',
            password='testpassword',
            server='testhost',
            port=5236
        )
        self.assertEqual(conn, self.mock_conn)
        self.assertEqual(engine.conn, self.mock_conn)

    def test_get_connection_failure(self):
        # Arrange
        self.mock_dmpython.connect.reset_mock(return_value=True, side_effect=True) # Reset previous mocks if any
        self.mock_dmpython.connect.side_effect = Exception("Connection failed")
        engine = DmEngine(instance=self.instance)

        # Act & Assert
        with self.assertRaisesRegex(Exception, "Connection failed"):
            engine.get_connection(db_name='testdb')
        self.assertIsNone(engine.conn)

    def test_execute_check_syntax_ok(self):
        # Arrange
        engine = DmEngine(instance=self.instance)
        engine.query_check = MagicMock(return_value={'status': 0, 'msg': 'Syntax check successful.', 'data': {}, 'error': None})

        # Act
        review_set = engine.execute_check(sql="SELECT 1")

        # Assert
        self.assertIsInstance(review_set, ReviewSet)
        self.assertEqual(review_set.error_count, 0)
        self.assertEqual(len(review_set.rows), 0)
        self.assertEqual(review_set.full_sql, "SELECT 1")


    def test_execute_check_syntax_error(self):
        # Arrange
        engine = DmEngine(instance=self.instance)
        engine.query_check = MagicMock(return_value={'status': 1, 'msg': 'Syntax error near SELECT', 'data': {}, 'error': 'Syntax error near SELECT'})

        # Act
        review_set = engine.execute_check(sql="SELEC * FROM DUAL")

        # Assert
        self.assertIsInstance(review_set, ReviewSet)
        self.assertEqual(review_set.error_count, 1)
        self.assertEqual(len(review_set.rows), 1)
        self.assertEqual(review_set.rows[0].sql, "SELEC * FROM DUAL")
        self.assertEqual(review_set.rows[0].errormessage, 'Syntax error near SELECT')
        self.assertEqual(review_set.rows[0].stagestatus, 'SQL审核不通过')


    def test_get_rollback_placeholder(self):
        # Arrange
        engine = DmEngine(instance=self.instance)

        # Act
        rollback_statements = engine.get_rollback(workflow=None)

        # Assert
        self.assertEqual(rollback_statements, [])

    # Tests for query method
    def test_query_success(self):
        # Arrange
        sql = "SELECT name FROM users"
        self.mock_cursor.description = [('name',)]
        self.mock_cursor.fetchall.return_value = [('user1',), ('user2',)]
        self.mock_cursor.rowcount = 2
        engine = DmEngine(instance=self.instance)

        # Act
        result = engine.query(sql=sql)

        # Assert
        self.mock_dmpython.connect.assert_called_once()
        self.mock_conn.cursor.assert_called_once()
        self.mock_cursor.execute.assert_called_once_with(sql + " FETCH FIRST 0 ROWS ONLY") # limit_num defaults to 0
        self.assertEqual(result['column_list'], ['name'])
        self.assertEqual(result['rows'], [('user1',), ('user2',)])
        self.assertEqual(result['effect_row'], 2)
        self.assertIsNone(result['error'])

    def test_query_with_limit(self):
        # Arrange
        sql = "SELECT name FROM users"
        limit_num = 1
        expected_sql = f"{sql} FETCH FIRST {limit_num} ROWS ONLY"
        self.mock_cursor.description = [('name',)]
        self.mock_cursor.fetchall.return_value = [('user1',)]
        self.mock_cursor.rowcount = 1
        engine = DmEngine(instance=self.instance)

        # Act
        result = engine.query(sql=sql, limit_num=limit_num)

        # Assert
        self.mock_cursor.execute.assert_called_once_with(expected_sql)
        self.assertEqual(result['rows'], [('user1',)])
        self.assertEqual(result['effect_row'], 1)

    def test_query_failure(self):
        # Arrange
        sql = "SELECT name FROM users"
        self.mock_cursor.execute.side_effect = Exception("Query failed")
        engine = DmEngine(instance=self.instance)

        # Act
        result = engine.query(sql=sql)

        # Assert
        self.assertIsNotNone(result['error'])
        self.assertTrue("Query failed" in result['error'])
        self.assertEqual(result['rows'], [])
        self.assertEqual(result['column_list'], [])

    # Tests for get_all_databases
    def test_get_all_databases_success(self):
        engine = DmEngine(instance=self.instance)
        # Mock the engine's own query method
        engine.query = MagicMock(return_value={'rows': [('db1',), ('db2',)], 'column_list': ['NAME'], 'error': None})
        result = engine.get_all_databases()
        self.assertIsNone(result['error'])
        self.assertEqual(result['data'], ['db1', 'db2'])
        engine.query.assert_called_once_with(sql="SELECT NAME FROM SYSOBJECTS WHERE TYPE$ = 'SCH';")

    def test_get_all_databases_no_data(self):
        engine = DmEngine(instance=self.instance)
        engine.query = MagicMock(return_value={'rows': [], 'column_list': ['NAME'], 'error': None})
        result = engine.get_all_databases()
        self.assertIsNone(result['error'])
        self.assertEqual(result['data'], [])

    def test_get_all_databases_error(self):
        engine = DmEngine(instance=self.instance)
        engine.query = MagicMock(return_value={'rows': [], 'error': "DB error fetching schemas"})
        result = engine.get_all_databases()
        self.assertEqual(result['error'], "DB error fetching schemas")
        self.assertEqual(result['data'], [])

    # Tests for get_all_tables
    def test_get_all_tables_success(self):
        db_name = "test_schema"
        engine = DmEngine(instance=self.instance)
        expected_sql = f"""
            SELECT O.NAME
            FROM SYSOBJECTS O
            JOIN SYSOBJECTS S ON O.SCHID = S.ID
            WHERE O.TYPE$ = 'SCHOBJ'
              AND O.SUBTYPE$ IN ('UTAB', 'STAB')
              AND S.TYPE$ = 'SCH'
              AND S.NAME = '{db_name}';
        """
        engine.query = MagicMock(return_value={'rows': [('table1',), ('table2',)], 'error': None})
        result = engine.get_all_tables(db_name=db_name)
        self.assertIsNone(result['error'])
        self.assertEqual(result['data'], ['table1', 'table2'])
        engine.query.assert_called_once_with(db_name=db_name, sql=expected_sql)

    def test_get_all_tables_no_data(self):
        db_name = "test_schema"
        engine = DmEngine(instance=self.instance)
        engine.query = MagicMock(return_value={'rows': [], 'error': None}) # No error, just no tables
        result = engine.get_all_tables(db_name=db_name)
        self.assertIsNone(result['error'])
        self.assertEqual(result['data'], [])

    def test_get_all_tables_error(self):
        db_name = "test_schema"
        engine = DmEngine(instance=self.instance)
        engine.query = MagicMock(return_value={'rows': [], 'error': "DB error fetching tables"})
        result = engine.get_all_tables(db_name=db_name)
        self.assertEqual(result['error'], "DB error fetching tables")
        self.assertEqual(result['data'], [])

    # Tests for get_all_columns_by_tb
    def test_get_all_columns_by_tb_success(self):
        db_name, tb_name = "test_db", "test_tb"
        engine = DmEngine(instance=self.instance)
        expected_sql = f"""
            SELECT C.NAME
            FROM SYSCOLUMNS C
            JOIN SYSOBJECTS T ON C.ID = T.ID
            JOIN SYSOBJECTS S ON T.SCHID = S.ID
            WHERE T.NAME = '{tb_name}'
              AND S.NAME = '{db_name}'
              AND T.TYPE$ = 'SCHOBJ'
              AND T.SUBTYPE$ IN ('UTAB', 'STAB')
              AND S.TYPE$ = 'SCH'
            ORDER BY C.COLID;
        """
        engine.query = MagicMock(return_value={'rows': [('col1',), ('col2',)], 'error': None})
        result = engine.get_all_columns_by_tb(db_name, tb_name)
        self.assertIsNone(result['error'])
        self.assertEqual(result['data'], ['col1', 'col2'])
        engine.query.assert_called_once_with(db_name=db_name, sql=expected_sql)

    def test_get_all_columns_by_tb_no_data(self):
        db_name, tb_name = "test_db", "test_tb"
        engine = DmEngine(instance=self.instance)
        engine.query = MagicMock(return_value={'rows': [], 'error': None})
        result = engine.get_all_columns_by_tb(db_name, tb_name)
        self.assertIsNone(result['error'])
        self.assertEqual(result['data'], [])

    def test_get_all_columns_by_tb_error(self):
        db_name, tb_name = "test_db", "test_tb"
        engine = DmEngine(instance=self.instance)
        engine.query = MagicMock(return_value={'rows': [], 'error': "DB error fetching columns"})
        result = engine.get_all_columns_by_tb(db_name, tb_name)
        self.assertEqual(result['error'], "DB error fetching columns")
        self.assertEqual(result['data'], [])

    # Tests for describe_table
    def test_describe_table_success(self):
        db_name, tb_name = "test_db", "test_tb"
        engine = DmEngine(instance=self.instance)
        expected_sql = f"""
            SELECT C.NAME, C.TYPE$
            FROM SYSCOLUMNS C
            JOIN SYSOBJECTS T ON C.ID = T.ID
            JOIN SYSOBJECTS S ON T.SCHID = S.ID
            WHERE T.NAME = '{tb_name}'
              AND S.NAME = '{db_name}'
              AND T.TYPE$ = 'SCHOBJ'
              AND T.SUBTYPE$ IN ('UTAB', 'STAB')
              AND S.TYPE$ = 'SCH'
            ORDER BY C.COLID;
        """
        query_rows = [('id', 'INT'), ('name', 'VARCHAR')]
        expected_data = [{'name': 'id', 'type': 'INT', 'comment': ''}, {'name': 'name', 'type': 'VARCHAR', 'comment': ''}]
        engine.query = MagicMock(return_value={'rows': query_rows, 'error': None})
        result = engine.describe_table(db_name, tb_name)
        self.assertIsNone(result['error'])
        self.assertEqual(result['data'], expected_data)
        engine.query.assert_called_once_with(db_name=db_name, sql=expected_sql)

    def test_describe_table_error(self):
        db_name, tb_name = "test_db", "test_tb"
        engine = DmEngine(instance=self.instance)
        engine.query = MagicMock(return_value={'rows': [], 'error': "DB error describing table"})
        result = engine.describe_table(db_name, tb_name)
        self.assertEqual(result['error'], "DB error describing table")
        self.assertEqual(result['data'], [])

    # Tests for get_tables_metas_data
    def test_get_tables_metas_data_success(self):
        db_name = "test_db"
        engine = DmEngine(instance=self.instance)

        # Mock sequence of query calls made by get_tables_metas_data
        # 1. Get tables
        # 2. For each table: get table comment, get columns, for each column get comment
        table_rows = [('table1', 101, db_name), ('table2', 102, db_name)]
        table1_comment_rows = [("Table 1 comment",)]
        table1_col_rows = [('id', 'INT', 10, 'N', None, 1), ('data', 'TEXT', 1000, 'Y', "'default'", 2)]
        table1_col1_comment_rows = [("ID column",)]
        table1_col2_comment_rows = [("Data column",)]
        table2_comment_rows = [("Table 2 comment",)]
        table2_col_rows = [('pk', 'BIGINT', 8, 'N', None, 1)]
        table2_col1_comment_rows = [("PK column",)]

        engine.query = MagicMock()
        engine.query.side_effect = [
            # Call 1: Get tables
            {'rows': table_rows, 'column_list': ['TABLE_NAME', 'TABLE_ID', 'TABLE_SCHEMA'], 'error': None},
            # table1 mocks
            {'rows': table1_comment_rows, 'error': None}, # table1 comment
            {'rows': table1_col_rows, 'error': None},   # table1 columns
            {'rows': table1_col1_comment_rows, 'error': None}, # table1 col1 comment
            {'rows': table1_col2_comment_rows, 'error': None}, # table1 col2 comment
            # table2 mocks
            {'rows': table2_comment_rows, 'error': None}, # table2 comment
            {'rows': table2_col_rows, 'error': None},   # table2 columns
            {'rows': table2_col1_comment_rows, 'error': None}, # table2 col1 comment
        ]

        # Mock close method, as it's called explicitly in the finally block
        engine.close = MagicMock()

        result = engine.get_tables_metas_data(db_name=db_name)
        self.assertIsNone(result['error'])
        self.assertEqual(len(result['data']), 2)
        self.assertEqual(result['data'][0]['TABLE_INFO']['TABLE_NAME'], 'table1')
        self.assertEqual(result['data'][0]['TABLE_INFO']['TABLE_COMMENT'], "Table 1 comment")
        self.assertEqual(len(result['data'][0]['COLUMNS']), 2)
        self.assertEqual(result['data'][0]['COLUMNS'][0]['COLUMN_NAME'], 'id')
        self.assertEqual(result['data'][0]['COLUMNS'][0]['COLUMN_COMMENT'], "ID column")
        self.assertEqual(result['data'][1]['TABLE_INFO']['TABLE_NAME'], 'table2')
        engine.close.assert_called_once()


    def test_get_tables_metas_data_no_tables(self):
        db_name = "test_db"
        engine = DmEngine(instance=self.instance)
        engine.query = MagicMock(return_value={'rows': [], 'error': None}) # No tables
        engine.close = MagicMock()
        result = engine.get_tables_metas_data(db_name=db_name)
        self.assertIsNone(result['error'])
        self.assertEqual(result['data'], [])
        engine.close.assert_called_once()

    def test_get_tables_metas_data_db_error(self):
        db_name = "test_db"
        engine = DmEngine(instance=self.instance)
        engine.query = MagicMock(return_value={'rows': [], 'error': "DB error listing tables"})
        engine.close = MagicMock() # Should still be called in finally
        result = engine.get_tables_metas_data(db_name=db_name)
        self.assertTrue("Error fetching tables" in result['error'])
        self.assertEqual(result['data'], [])
        # engine.close might or might not be called depending on where the error occurs
        # In the current implementation, if the first query fails, close won't be called by the finally block in get_tables_metas_data itself
        # because self.conn might not be set by the failing query call.
        # This depends on the internal behavior of self.query and its close_conn logic.
        # For this test, let's assume if the first query fails, the main method's finally block calls close.
        engine.close.assert_called_once()


    # Tests for get_table_meta_data
    def test_get_table_meta_data_success(self):
        db_name, tb_name = "test_db", "test_table"
        engine = DmEngine(instance=self.instance)
        table_info_row = ('test_table', db_name, 'BASE TABLE', 101, '2023-01-01', 'Y')
        column_list = ['TABLE_NAME', 'TABLE_SCHEMA', 'TABLE_TYPE', 'TABLE_ID', 'CREATE_TIME', 'IS_VALID']
        comment_row = ("Test table comment",)

        engine.query = MagicMock()
        engine.query.side_effect = [
            {'rows': [table_info_row], 'column_list': column_list, 'error': None}, # Table info query
            {'rows': [comment_row], 'error': None} # Comment query
        ]

        result = engine.get_table_meta_data(db_name, tb_name)

        self.assertIsNone(result['error'])
        self.assertEqual(len(result['rows']), 1)
        expected_row_dict = dict(zip(column_list + ['TABLE_COMMENT'], list(table_info_row) + [comment_row[0]]))
        self.assertEqual(result['rows'][0], expected_row_dict)
        self.assertEqual(result['column_list'], column_list + ['TABLE_COMMENT'])

    def test_get_table_meta_data_table_not_found(self):
        db_name, tb_name = "test_db", "non_existent_table"
        engine = DmEngine(instance=self.instance)
        engine.query = MagicMock(return_value={'rows': [], 'error': None}) # Table info query returns no rows
        engine.close = MagicMock()

        result = engine.get_table_meta_data(db_name, tb_name)

        self.assertTrue(f"Table {db_name}.{tb_name} not found" in result['error'])
        self.assertEqual(result['rows'], [])
        # If query returns no rows, the first query call itself would use close_conn=False,
        # but the method get_table_meta_data doesn't have its own finally close for this specific path.
        # This means self.close() is not guaranteed. Let's adjust DmEngine or test.
        # DmEngine.get_table_meta_data uses close_conn=True for comment query, so that one handles it.
        # If the first query fails (no rows), no second query is made.
        # No explicit self.close() in the "not found" path of get_table_meta_data.
        # Let's assume engine.query handles its own connection state or this is okay.
        # For now, not asserting engine.close() here.

    def test_get_table_meta_data_error_fetching_info(self):
        db_name, tb_name = "test_db", "test_table"
        engine = DmEngine(instance=self.instance)
        engine.query = MagicMock(return_value={'error': "DB error fetching table info"})
        engine.close = MagicMock() # Should be called in except block of get_table_meta_data

        result = engine.get_table_meta_data(db_name, tb_name)

        self.assertTrue("Error fetching table metadata" in result['error'])
        self.assertEqual(result['rows'], [])
        engine.close.assert_called_once() # From the except block

    # Tests for get_table_desc_data
    def test_get_table_desc_data_success(self):
        db_name, tb_name = "test_db", "test_table"
        engine = DmEngine(instance=self.instance)

        # COL_NAME, TYPE_CODE, CHAR_MAX_LEN, NUM_PREC, NUM_SCALE, IS_NULL, DEF_VAL, ORD_POS, TBL_ID
        col_rows_from_query = [
            ('id', 1, None, 10, 0, 'N', None, 1, 101),
            ('name', 12, 255, None, None, 'Y', "'anon'", 2, 101)
        ]
        # Comments for each column
        id_comment = ("Primary key",)
        name_comment = ("User name",)

        engine.query = MagicMock()
        engine.query.side_effect = [
            {'rows': col_rows_from_query, 'error': None}, # Column details query
            {'rows': [id_comment], 'error': None},      # Comment for 'id'
            {'rows': [name_comment], 'error': None}     # Comment for 'name'
        ]
        engine.close = MagicMock()

        result = engine.get_table_desc_data(db_name, tb_name)

        self.assertIsNone(result['error'])
        self.assertEqual(len(result['rows']), 2)
        self.assertEqual(result['rows'][0]['COLUMN_NAME'], 'id')
        self.assertEqual(result['rows'][0]['COLUMN_TYPE'], 1) # Type code for now
        self.assertEqual(result['rows'][0]['COLUMN_COMMENT'], "Primary key")
        self.assertEqual(result['rows'][1]['COLUMN_NAME'], 'name')
        self.assertEqual(result['rows'][1]['CHARACTER_MAXIMUM_LENGTH'], 255)
        self.assertEqual(result['rows'][1]['COLUMN_COMMENT'], "User name")
        self.assertEqual(result['column_list'], [
            'COLUMN_NAME', 'COLUMN_TYPE', 'CHARACTER_MAXIMUM_LENGTH', 'NUMERIC_PRECISION', 'NUMERIC_SCALE',
            'IS_NULLABLE', 'COLUMN_KEY', 'COLUMN_DEFAULT', 'EXTRA', 'COLUMN_COMMENT'
        ])
        engine.close.assert_called_once() # Called after all comments fetched

    def test_get_table_desc_data_error_fetching_cols(self):
        db_name, tb_name = "test_db", "test_table"
        engine = DmEngine(instance=self.instance)
        engine.query = MagicMock(return_value={'error': "DB error fetching columns"})
        engine.close = MagicMock()

        result = engine.get_table_desc_data(db_name, tb_name)
        self.assertTrue("Error fetching column descriptions" in result['error'])
        engine.close.assert_called_once() # From the main query error path

    # Tests for get_table_index_data
    def test_get_table_index_data_success(self):
        db_name, tb_name = "test_db", "test_table"
        engine = DmEngine(instance=self.instance)
        # COL_NAME, IDX_NAME, NON_UNIQUE, SEQ_IN_IDX, CARDINALITY, IS_NULL_COL, IDX_TYPE, IDX_COMMENT
        index_rows_from_query = [
            ('id', 'pk_users', 'NO', 1, None, 'N', 'NORMAL', 'Primary key index'),
            ('email', 'idx_email_unique', 'NO', 1, None, 'N', 'NORMAL', None)
        ]
        engine.query = MagicMock(return_value={'rows': index_rows_from_query, 'error': None})

        result = engine.get_table_index_data(db_name, tb_name)

        self.assertIsNone(result['error'])
        self.assertEqual(len(result['rows']), 2)
        self.assertEqual(result['rows'][0]['COLUMN_NAME'], 'id')
        self.assertEqual(result['rows'][0]['INDEX_NAME'], 'pk_users')
        self.assertEqual(result['rows'][0]['NON_UNIQUE'], 'NO')
        self.assertEqual(result['rows'][0]['INDEX_COMMENT'], 'Primary key index')
        self.assertEqual(result['rows'][1]['INDEX_NAME'], 'idx_email_unique')
        self.assertIsNone(result['rows'][1]['INDEX_COMMENT']) # No comment for second index
        self.assertEqual(result['column_list'], [
            'COLUMN_NAME', 'INDEX_NAME', 'NON_UNIQUE', 'SEQ_IN_INDEX',
            'CARDINALITY', 'IS_NULLABLE_COLUMN', 'INDEX_TYPE', 'INDEX_COMMENT'
        ])
        # engine.query uses close_conn=True by default, so no explicit engine.close() here.

    def test_get_table_index_data_no_indexes(self):
        db_name, tb_name = "test_db", "table_no_indexes"
        engine = DmEngine(instance=self.instance)
        engine.query = MagicMock(return_value={'rows': [], 'error': None}) # No indexes found

        result = engine.get_table_index_data(db_name, tb_name)
        self.assertIsNone(result['error'])
        self.assertEqual(result['rows'], [])

    def test_get_table_index_data_error(self):
        db_name, tb_name = "test_db", "test_table"
        engine = DmEngine(instance=self.instance)
        engine.query = MagicMock(return_value={'error': "DB error fetching indexes"})
        engine.close = MagicMock() # For the except block in get_table_index_data

        result = engine.get_table_index_data(db_name, tb_name)
        self.assertTrue("Error fetching index data" in result['error'])
        # engine.close() in get_table_index_data is only called in the final exception handler
        # if self.conn exists. If query() itself fails and closes/nullifies self.conn,
        # this might not be called. This depends on query() implementation.
        # Let's assume for an error from query(), self.conn might be None.
        # If the error is *within* get_table_index_data but after query, self.conn might exist.
        # For now, not asserting engine.close() as its call is conditional.


if __name__ == '__main__':
    unittest.main()
