import dmPython # Import the Dameng Python driver
import re # For regex checks in filter_sql

from sql.engines import EngineBase

class DmEngine(EngineBase):
    """Engine for Dameng Database (DM)."""

    def get_connection(self, db_name=None):
        """
        Establishes and returns a connection to the Dameng database.
        """
        if self.conn:
            return self.conn

        user = self.instance.user
        password = self.instance.password
        host_parts = self.instance.host.split(':')
        host = host_parts[0]
        port = int(host_parts[1]) if len(host_parts) > 1 else 5236

        try:
            self.conn = dmPython.connect(
                user=user,
                password=password,
                server=host,
                port=port
            )
        except Exception as e:
            self.conn = None
            raise e
        return self.conn

    def query_check(self, db_name=None, sql=''):
        """
        Performs a basic syntax check on the SQL query.
        Returns a dictionary with 'status', 'msg', 'data', and 'error' keys.
        """
        conn = None
        # Initialize with success state, error is None
        result = {'status': 0, 'msg': 'Syntax check successful.', 'data': {}, 'error': None}
        error_message = None
        try:
            conn = self.get_connection(db_name=db_name)
            if conn:
                cursor = conn.cursor()
                if cursor:
                    # Basic check: if cursor can be created.
                    # A more robust check might try to EXPLAIN the query.
                    cursor.close()
            else:
                error_message = 'Syntax check failed: Could not establish connection.'

        except Exception as e:
            error_message = f'Syntax check failed: {str(e)[:200]}' # Truncate error for UI

        if error_message:
            result['status'] = 1
            result['msg'] = error_message
            result['error'] = error_message

        return result

    def filter_sql(self, sql='', limit_num=0):
        """
        Adds a limit clause to the SQL query if limit_num is specified and
        no common limit clause seems to exist. Uses 'FETCH FIRST N ROWS ONLY' for Dameng.
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
        Executes a SQL query and returns the results.
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
        """
        db_names = []
        sql = "SELECT NAME FROM SYSOBJECTS WHERE TYPE$ = 'SCH';"
        try:
            query_result = self.query(sql=sql, close_conn=False)
            if query_result and not query_result.get('error') and query_result.get('rows'):
                db_names = [row[0] for row in query_result['rows'] if row and row[0]]
        except Exception as e:
            pass
        return db_names

    def get_all_tables(self, db_name):
        """
        Retrieves a list of all table names within a given database (schema).
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
            query_result = self.query(db_name=db_name, sql=sql, close_conn=False)
            if query_result and not query_result.get('error') and query_result.get('rows'):
                description = [{'name': row[0], 'type': row[1], 'comment': ''}
                               for row in query_result['rows'] if row and len(row) >= 2]
        except Exception as e:
            pass
        return description
