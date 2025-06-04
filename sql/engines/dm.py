import dmPython # Import the Dameng Python driver
import re # For regex checks in filter_sql

from sql.engines import EngineBase
from common.utils.timer import Timer # For logging connection time, if desired

class DmEngine(EngineBase):
    def get_connection(self, db_name=None):
        # Implemented
        if self.conn:
            return self.conn
        user = self.instance.user
        password = self.instance.password
        host_parts = self.instance.host.split(':')
        host = host_parts[0]
        port = int(host_parts[1]) if len(host_parts) > 1 else 5236
        try:
            with Timer() as t:
                self.conn = dmPython.connect(
                    user=user,
                    password=password,
                    server=host,
                    port=port
                )
            print(f"Attempted Dameng connection to {host}:{port} for user {user}. Status: {'Connected' if self.conn else 'Failed'}")
        except Exception as e:
            print(f"Error connecting to Dameng ({self.instance.name}): {e}")
            self.conn = None
            raise e
        return self.conn

    def query_check(self, db_name=None, sql=''):
        # Implemented
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
            print(f"Syntax check failed for Dameng: {e}. SQL: {sql}")
            result['status'] = 1
            result['msg'] = f'Syntax check failed: {str(e)[:200]}'
        finally:
            pass
        return result

    def filter_sql(self, sql='', limit_num=0):
        """
        Adds a limit clause to the SQL query if limit_num is specified and
        no limit clause seems to exist.
        For Dameng, 'FETCH FIRST N ROWS ONLY' is a standard option.
        """
        sql_original = sql.strip()
        # Remove trailing semicolon if present, to safely append clauses
        if sql_original.endswith(';'):
            sql_to_check = sql_original[:-1].strip()
        else:
            sql_to_check = sql_original

        if limit_num > 0:
            # Basic check for existing limit clauses (case-insensitive)
            # This is a heuristic and won't catch all complex cases or dialect variations.
            # It looks for keywords that usually signify a limit is already applied.
            # Using word boundaries (\b) to avoid matching parts of words.
            limit_pattern = r"(\bLIMIT\s+\d+|\bTOP\s+\d+|\bROWNUM\s*(<|<=)\s*\d+|\bFETCH\s+FIRST\s+\d+\s+ROWS\s+ONLY|\bSELECT\s+TOP\s+\d+)"
            if not re.search(limit_pattern, sql_to_check, re.IGNORECASE):
                # Dameng supports standard SQL "FETCH FIRST N ROWS ONLY"
                # Ensure there's a space before appending.
                return f"{sql_to_check} FETCH FIRST {int(limit_num)} ROWS ONLY"

        return sql_original # Return original (with semicolon if it had one) if no limit added or if already present

    def get_all_databases(self):
        # Implemented
        conn = None
        db_names = []
        sql = "SELECT NAME FROM SYSOBJECTS WHERE TYPE$ = 'SCH';"
        try:
            conn = self.get_connection()
            if conn:
                cursor = conn.cursor()
                cursor.execute(sql)
                rows = cursor.fetchall()
                db_names = [row[0] for row in rows if row]
                cursor.close()
        except Exception as e:
            print(f"Error getting all databases: {e}")
            db_names = []
        return db_names

    def get_all_tables(self, db_name):
        # Implemented
        conn = None
        table_names = []
        sql = """
            SELECT O.NAME
            FROM SYSOBJECTS O
            JOIN SYSOBJECTS S ON O.SCHID = S.ID
            WHERE O.TYPE$ = 'SCHOBJ'
              AND O.SUBTYPE$ IN ('UTAB', 'STAB')
              AND S.TYPE$ = 'SCH'
              AND S.NAME = %s;
        """
        try:
            conn = self.get_connection()
            if conn:
                cursor = conn.cursor()
                cursor.execute(sql, (db_name,))
                rows = cursor.fetchall()
                table_names = [row[0] for row in rows if row]
                cursor.close()
        except Exception as e:
            print(f"Error getting all tables for {db_name}: {e}")
            table_names = []
        return table_names

    def get_all_columns_by_tb(self, db_name, tb_name, **kwargs):
        # Implemented
        conn = None
        column_names = []
        sql = """
            SELECT C.NAME
            FROM SYSCOLUMNS C
            JOIN SYSOBJECTS T ON C.ID = T.ID
            JOIN SYSOBJECTS S ON T.SCHID = S.ID
            WHERE T.NAME = %s
              AND S.NAME = %s
              AND T.TYPE$ = 'SCHOBJ'
              AND T.SUBTYPE$ IN ('UTAB', 'STAB')
              AND S.TYPE$ = 'SCH'
            ORDER BY C.COLID;
        """
        try:
            conn = self.get_connection()
            if conn:
                cursor = conn.cursor()
                cursor.execute(sql, (tb_name, db_name))
                rows = cursor.fetchall()
                column_names = [row[0] for row in rows if row]
                cursor.close()
        except Exception as e:
            print(f"Error getting columns for {db_name}.{tb_name}: {e}")
            column_names = []
        return column_names

    def describe_table(self, db_name, tb_name, **kwargs):
        # Implemented
        conn = None
        description = []
        sql = """
            SELECT C.NAME, C.TYPE$
            FROM SYSCOLUMNS C
            JOIN SYSOBJECTS T ON C.ID = T.ID
            JOIN SYSOBJECTS S ON T.SCHID = S.ID
            WHERE T.NAME = %s
              AND S.NAME = %s
              AND T.TYPE$ = 'SCHOBJ'
              AND T.SUBTYPE$ IN ('UTAB', 'STAB')
              AND S.TYPE$ = 'SCH'
            ORDER BY C.COLID;
        """
        try:
            conn = self.get_connection()
            if conn:
                cursor = conn.cursor()
                cursor.execute(sql, (tb_name, db_name))
                rows = cursor.fetchall()
                for row in rows:
                    if row:
                        description.append({'name': row[0], 'type': row[1], 'comment': ''})
                cursor.close()
        except Exception as e:
            print(f"Error describing table {db_name}.{tb_name}: {e}")
            description = []
        return description

    def query(self, db_name=None, sql='', limit_num=0, close_conn=True):
        # Placeholder - will be implemented
        return {"column_list": [], "rows": [], "effect_row": 0, "error": None}
