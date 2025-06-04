import dmPython # Import the Dameng Python driver
import re # For regex checks in filter_sql

from sql.engines import EngineBase
# from common.utils.timer import Timer # Ensure this line is removed or commented
# from django.utils.html import escape

class DmEngine(EngineBase):
    """Engine for Dameng Database (DM)."""

    def get_connection(self, db_name=None):
        """
        Establishes and returns a connection to the Dameng database.
        If a connection (self.conn) already exists, it's returned.
        Otherwise, a new connection is established using instance configurations.

        Args:
            db_name (str, optional): The specific database/schema name.
                                     Currently, this implementation does not use db_name
                                     to switch schemas within an existing connection.

        Returns:
            dmPython.Connection: A dmPython connection object, or None if connection fails.

        Raises:
            Exception: Propagates exceptions from dmPython.connect on failure.
        """
        if self.conn:
            return self.conn

        user = self.instance.user
        password = self.instance.password
        host_parts = self.instance.host.split(':')
        host = host_parts[0]
        port = int(host_parts[1]) if len(host_parts) > 1 else 5236

        try:
            # Removed 'with Timer() as t:' block
            self.conn = dmPython.connect(
                user=user,
                password=password,
                server=host,
                port=port
            )
            # Removed logging that used t.elapsed
        except Exception as e:
            self.conn = None
            raise e
        return self.conn

    def query_check(self, db_name=None, sql=''):
        """
        Performs a basic syntax check on the SQL query.
        Currently, it verifies if a connection can be established and a cursor created.

        Args:
            db_name (str, optional): The database/schema name.
            sql (str): The SQL query to check.

        Returns:
            dict: A dictionary with 'status' (0 for success, 1 for failure),
                  'msg' (message), and 'data' (empty dict).
        """
        conn = None
        result = {'status': 0, 'msg': 'Syntax check successful.', 'data': {}}
        try:
            conn = self.get_connection(db_name=db_name)
            if conn:
                cursor = conn.cursor()
                if cursor:
                    cursor.close()
            else:
                result['status'] = 1
                result['msg'] = 'Syntax check failed: Could not establish connection.'
        except Exception as e:
            result['status'] = 1
            result['msg'] = f'Syntax check failed: {str(e)[:200]}'
        return result

    def filter_sql(self, sql='', limit_num=0):
        """
        Adds a limit clause to the SQL query if limit_num is specified and
        no common limit clause seems to exist. Uses 'FETCH FIRST N ROWS ONLY' for Dameng.

        Args:
            sql (str): The SQL query string.
            limit_num (int): The number of rows to limit to.

        Returns:
            str: The modified SQL query with a limit clause if applicable,
                 otherwise the original SQL query.
        """
        sql_original = sql.strip()
        if sql_original.endswith(';'):
            sql_to_check = sql_original[:-1].strip()
        else:
            sql_to_check = sql_original

        if limit_num > 0:
            # Using word boundaries (\b) to avoid matching parts of words.
            limit_pattern = r"(LIMIT\s+\d+|TOP\s+\d+|ROWNUM\s*(<|<=)\s*\d+|FETCH\s+FIRST\s+\d+\s+ROWS\s+ONLY|SELECT\s+TOP\s+\d+)"
            if not re.search(limit_pattern, sql_to_check, re.IGNORECASE):
                return f"{sql_to_check} FETCH FIRST {int(limit_num)} ROWS ONLY"

        return sql_original

    def query(self, db_name=None, sql='', limit_num=0, close_conn=True):
        """
        Executes a SQL query and returns the results, including column list,
        rows, and affected row count. Manages connection lifecycle based on close_conn.

        Args:
            db_name (str, optional): The database/schema name to connect to.
            sql (str): The SQL query to execute.
            limit_num (int, optional): Number of rows to limit the query to. Defaults to 0 (no limit).
            close_conn (bool, optional): Whether to close the connection after the query.
                                         Defaults to True. If False, self.conn is kept open.

        Returns:
            dict: A dictionary containing:
                  'column_list' (list of column names),
                  'rows' (list of tuples representing rows),
                  'effect_row' (int, number of affected/returned rows),
                  'error' (str, error message if any, else None).
        """
        result = {'column_list': [], 'rows': [], 'effect_row': 0, 'error': None}
        current_conn = None
        cursor = None

        try:
            current_conn = self.get_connection(db_name=db_name)
            if not current_conn:
                result['error'] = "Failed to establish database connection."
                return result

            actual_sql = self.filter_sql(sql, limit_num)

            cursor = current_conn.cursor()
            cursor.execute(actual_sql)

            if cursor.description:
                result['column_list'] = [desc[0] for desc in cursor.description]

            try:
                fetched_rows = cursor.fetchall()
                result['rows'] = fetched_rows
            except Exception:
                pass

            if cursor.rowcount is not None and cursor.rowcount >= 0:
                result['effect_row'] = cursor.rowcount
            elif result['rows']:
                result['effect_row'] = len(result['rows'])

        except Exception as e:
            result['error'] = f"Error: {str(e)}"
        finally:
            if cursor:
                try:
                    cursor.close()
                except Exception:
                    pass

            if current_conn and close_conn:
                try:
                    current_conn.close()
                    if current_conn is self.conn:
                        self.conn = None
                except Exception:
                    pass

        return result

    def get_all_databases(self):
        """
        Retrieves a list of all database (schema) names from the Dameng server.
        This method uses the main query() method for execution.

        Returns:
            list: A list of database/schema names. Returns empty list on error or if no databases found.
        """
        db_names = []
        sql = "SELECT NAME FROM SYSOBJECTS WHERE TYPE$ = 'SCH';"
        try:
            query_result = self.query(sql=sql, close_conn=False) # Keep connection for potential subsequent metadata calls
            if query_result and not query_result.get('error') and query_result.get('rows'):
                db_names = [row[0] for row in query_result['rows'] if row and row[0]]
        except Exception as e:
            # Error should be handled by self.query, this is a fallback.
            pass
        return db_names

    def get_all_tables(self, db_name):
        """
        Retrieves a list of all table names (user and system) within a given database (schema).
        This method uses the main query() method for execution.

        Args:
            db_name (str): The name of the database/schema.

        Returns:
            list: A list of table names. Returns empty list on error or if no tables found.
        """
        table_names = []
        safe_db_name = db_name.replace("'", "''")
        sql = f"""
            SELECT O.NAME
            FROM SYSOBJECTS O
            JOIN SYSOBJECTS S ON O.SCHID = S.ID
            WHERE O.TYPE$ = 'SCHOBJ'
              AND O.SUBTYPE$ IN ('UTAB', 'STAB')
              AND S.TYPE$ = 'SCH'
              AND S.NAME = '{safe_db_name}';
        """
        try:
            query_result = self.query(db_name=db_name, sql=sql, close_conn=False)
            if query_result and not query_result.get('error') and query_result.get('rows'):
                table_names = [row[0] for row in query_result['rows'] if row and row[0]]
        except Exception as e:
            pass
        return table_names

    def get_all_columns_by_tb(self, db_name, tb_name, **kwargs):
        """
        Retrieves a list of all column names for a given table in a specific database (schema).
        This method uses the main query() method for execution.

        Args:
            db_name (str): The name of the database/schema.
            tb_name (str): The name of the table.

        Returns:
            list: A list of column names. Returns empty list on error or if no columns found.
        """
        column_names = []
        safe_db_name = db_name.replace("'", "''")
        safe_tb_name = tb_name.replace("'", "''")
        sql = f"""
            SELECT C.NAME
            FROM SYSCOLUMNS C
            JOIN SYSOBJECTS T ON C.ID = T.ID
            JOIN SYSOBJECTS S ON T.SCHID = S.ID
            WHERE T.NAME = '{safe_tb_name}'
              AND S.NAME = '{safe_db_name}'
              AND T.TYPE$ = 'SCHOBJ'
              AND T.SUBTYPE$ IN ('UTAB', 'STAB')
              AND S.TYPE$ = 'SCH'
            ORDER BY C.COLID;
        """
        try:
            query_result = self.query(db_name=db_name, sql=sql, close_conn=False)
            if query_result and not query_result.get('error') and query_result.get('rows'):
                column_names = [row[0] for row in query_result['rows'] if row and row[0]]
        except Exception as e:
            pass
        return column_names

    def describe_table(self, db_name, tb_name, **kwargs):
        """
        Retrieves a description of a table, including column names and their data types.
        This method uses the main query() method for execution.

        Args:
            db_name (str): The name of the database/schema.
            tb_name (str): The name of the table.

        Returns:
            list: A list of dictionaries, where each dictionary describes a column
                  (e.g., {'name': 'col_name', 'type': 'VARCHAR', 'comment': 'col_comment'}).
                  Returns empty list on error or if table not found/no columns.
        """
        description = []
        safe_db_name = db_name.replace("'", "''")
        safe_tb_name = tb_name.replace("'", "''")
        sql = f"""
            SELECT C.NAME, C.TYPE$
            FROM SYSCOLUMNS C
            JOIN SYSOBJECTS T ON C.ID = T.ID
            JOIN SYSOBJECTS S ON T.SCHID = S.ID
            WHERE T.NAME = '{safe_tb_name}'
              AND S.NAME = '{safe_db_name}'
              AND T.TYPE$ = 'SCHOBJ'
              AND T.SUBTYPE$ IN ('UTAB', 'STAB')
              AND S.TYPE$ = 'SCH'
            ORDER BY C.COLID;
        """
        try:
            query_result = self.query(db_name=db_name, sql=sql, close_conn=False) # Keep connection for potential subsequent calls
            if query_result and not query_result.get('error') and query_result.get('rows'):
                description = [{'name': row[0], 'type': row[1], 'comment': ''}
                               for row in query_result['rows'] if row and len(row) >= 2]
        except Exception as e:
            pass
        return description
