# -*- coding: UTF-8 -*-

import dmPython
import re
from sql.engines import EngineBase
from sql.engines.models import ResultSet, ReviewSet


class DamengEngine(EngineBase):
    name = "Dameng"
    info = "Dameng Engine"
    test_query = "SELECT 1 FROM DUAL"  # Or an equivalent basic query for Dameng DB

    def get_connection(self, db_name=None):
        # Remove any pseudo-implementation like 'pass' or 'return None'
        if self.conn:
            return self.conn

        # target_db_name = db_name or self.db_name # Not used in connect directly for now
        try:
            self.conn = dmPython.connect(
                user=self.user,
                password=self.password,
                server=self.host,
                port=self.port,
                # dmPython might use 'database' or 'db' or similar for db_name,
                # or it might be part of the DSN.
                # For now, let's assume it's an argument if target_db_name is present.
                # This might need adjustment based on dmPython's exact API.
                # If target_db_name is not directly supported in connect(),
                # it might be handled by executing "USE database" after connection
                # or by connecting without a specific db_name if dmPython doesn't support it.
                # For now, we will omit db_name from connect() if it's not a standard param,
                # as dmPython typically connects to a server/instance and then you select a DB.
                # The user might need to specify the DB in connection strings or DSN for some drivers.
                # Let's assume for now it connects to the default DB or the one specified in a DSN if used.
                # We will revisit if db_name needs to be handled differently.
            )
            # If a specific db_name is required and not part of connect(),
            # and dmPython supports "USE <database>" type commands,
            # one might execute it here.
            # e.g., if target_db_name:
            #   cursor = self.conn.cursor()
            #   cursor.execute(f"USE {target_db_name}") # This is speculative
            #   cursor.close()

        except dmPython.Error as e:
            # Log the error or raise a custom exception
            # For now, let's re-raise to see the error if it occurs during testing by user
            raise Exception(f"Dameng connection failed: {e}")
        return self.conn

    def query_check(self, db_name=None, sql=''):
        # Basic check: remove comments, trim whitespace.
        # More sophisticated checks/splitting specific to Dameng SQL dialect could be added.
        # For now, let's assume a fairly standard SQL behavior.
        # Archery's core review logic might also apply checks.
        # This method in engine is more for driver/dialect specific preprocessing.

        # Example: Remove simple /* ... */ and -- comments, then strip
        import re
        sql = re.sub(r"/\*.*?\*/", "", sql, flags=re.DOTALL)  # Remove /* ... */ comments
        sql = re.sub(r"--.*?\n", "", sql)  # Remove -- comments
        sql = sql.strip()

        # Returning a dict as expected by Archery's workflow
        # 'bad_query': True if a query is identified as problematic by this check.
        # 'filtered_sql': The processed SQL.
        # 'has_star': Optional, True if 'select *' is found (example of a simple check)

        # For now, no "bad_query" detection here, just filtering.
        # Archery's sql_review module handles more complex rule-based checks.
        return {'bad_query': False, 'filtered_sql': sql}

    def filter_sql(self, sql='', limit_num=0):
        sql = sql.strip()
        if sql.lower().startswith("select") and limit_num > 0:
            # Check if SQL already has a LIMIT clause
            if "limit" not in sql.lower():  # Basic check, might need regex for robustness
                # Dameng syntax: LIMIT <N> or LIMIT <M, N>
                # Assuming limit_num is the count (N)
                sql = f"{sql} LIMIT {limit_num}"
        return sql

    def query(self, db_name=None, sql='', limit_num=0, close_conn=True, parameters=None, **kwargs):
        result_set = ResultSet()
        cursor = None  # Initialize cursor to None for finally block
        try:
            conn = self.get_connection(db_name=db_name)
            cursor = conn.cursor()

            # Apply base_filter_sql from EngineBase, which should handle limit_num
            # The actual filtering (adding LIMIT) should ideally be database-specific.
            # For now, we assume filter_sql in EngineBase or a more specific one here handles it.
            # If dmPython uses a different placeholder style, parameters might need adjustment.
            # PEP 249 specifies qmark, numeric, named, format, pyformat. dmPython default is usually qmark or numeric.
            cursor.execute(sql, parameters or [])  # Pass parameters if any

            if cursor.description:
                result_set.column_list = [desc[0] for desc in cursor.description]

                # Fetch rows based on limit_num if not already handled by filter_sql
                # However, EngineBase.filter_sql is expected to add the LIMIT clause.
                # If limit_num was applied by filter_sql, fetchall() is fine.
                # If not, and limit_num > 0, we might do cursor.fetchmany(limit_num)
                # For simplicity, assume filter_sql handled it or fetchall is acceptable.
                rows = cursor.fetchall()
                result_set.rows = [tuple(row) for row in rows]
                result_set.affected_rows = cursor.rowcount if cursor.rowcount != -1 else len(
                    rows)  # rowcount might be -1
            else:  # For statements that don't return rows (e.g., DDL not via execute but query)
                result_set.affected_rows = cursor.rowcount

            # Handle DML statements that might be run via query() for some DBs,
            # though Archery usually uses execute() for DML.
            # If it's a DML and autocommit is not on, a conn.commit() might be needed here.
            # Assuming dmPython connections are in autocommit mode by default or that Archery manages transactions.

        except dmPython.Error as e:
            result_set.error = f"Dameng query failed: {e}"
        finally:
            if close_conn and self.conn:
                if cursor:  # Ensure cursor exists before trying to close
                    cursor.close()
                self.conn.close()
                self.conn = None
        return result_set

    def get_all_databases(self):
        # This is a placeholder query and might need to be adjusted for Dameng's specific system catalog.
        # Using INFORMATION_SCHEMA.SCHEMATA as a common starting point.
        # Actual Dameng system view might be different (e.g., DBA_SCHEMAS, V$DATABASE, etc.)
        sql = "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA;"

        # The self.query() method is expected to handle connection and execution,
        # and return a ResultSet object.
        # ResultSet should have an 'error' attribute if something went wrong,
        # and 'rows' attribute for the data.
        result = self.query(sql=sql)

        # Archery expects result.rows to be a list of tuples/lists, e.g., [('db1',), ('db2',)]
        return result

    def get_all_tables(self, db_name, **kwargs):
        # Query to list all tables in a given database/schema.
        # Placeholder, INFORMATION_SCHEMA is common.
        # Actual Dameng system view might be different (e.g., DBA_TABLES, ALL_TABLES).
        sql = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = %s;"

        # The self.query() method is expected to handle connection and execution.
        # It should take `db_name` and `parameters` for the query.
        result = self.query(db_name=db_name, sql=sql, parameters=[db_name])

        # Archery expects result.rows to be a list of tuples/lists, e.g., [('table1',), ('table2',)]
        return result

    def get_all_columns_by_tb(self, db_name, tb_name, **kwargs):
        # Actual Dameng DB logic to fetch all columns for a table will be added later
        return ResultSet()

    def describe_table(self, db_name, tb_name, **kwargs):
        # Actual Dameng DB logic to describe a table will be added later
        return ResultSet()

    def execute_check(self, db_name=None, sql=''):
        # Actual Dameng DB specific execute check logic will be added later
        return ReviewSet()

    def execute(self, **kwargs):
        # Actual Dameng DB execution logic will be added later
        return ReviewSet()

    def get_execute_percentage(self):
        # Actual Dameng DB logic to get execution percentage will be added later
        return 0.0

    def get_rollback(self, workflow):
        # Actual Dameng DB logic to generate rollback SQL will be added later
        return list()

    def query_masking(self, db_name=None, sql='', resultset=None):
        # Actual Dameng DB specific data masking logic will be added later
        return resultset
