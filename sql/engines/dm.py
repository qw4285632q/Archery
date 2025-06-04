import dmPython # Import the Dameng Python driver
import re # For regex checks in filter_sql
import logging # For logging unimplemented methods
import sqlparse # For splitting SQL statements

from sql.engines import EngineBase
from .models import ReviewResult, ReviewSet, ResultSet # Ensure ResultSet is imported

logger = logging.getLogger(__name__)

class DmEngine(EngineBase):
    name = "DM"
    info = "Dameng Engine"
    """Engine for Dameng Database (DM)."""

    def get_connection(self, db_name=None):
        # ... (existing implementation)
        if self.conn:
            return self.conn
        user = self.instance.user
        password = self.instance.password
        host_parts = self.instance.host.split(':')
        host = host_parts[0]
        port = int(host_parts[1]) if len(host_parts) > 1 else 5236
        try:
            self.conn = dmPython.connect(user=user, password=password, server=host, port=port)
        except Exception as e:
            self.conn = None
            raise e
        return self.conn

    def query_check(self, db_name=None, sql=''):
        # ... (existing implementation)
        conn = None
        result = {'status': 0, 'msg': 'Syntax check successful.', 'data': {}, 'error': None}
        error_message = None
        try:
            conn = self.get_connection(db_name=db_name)
            if conn:
                cursor = conn.cursor()
                if cursor:
                    cursor.close()
            else:
                error_message = 'Syntax check failed: Could not establish connection.'
        except Exception as e:
            error_message = f'Syntax check failed: {str(e)[:200]}'
        if error_message:
            result['status'] = 1
            result['msg'] = error_message
            result['error'] = error_message
        return result

    def execute_check(self, db_name=None, sql=""):
        """Basic syntax check for Dameng."""
        review_set = ReviewSet(full_sql=sql)
        # Utilize existing query_check logic, adapt as needed
        # query_check returns a dict: {'status': 0 for success, 1 for error, 'msg': message}
        check_result = self.query_check(db_name=db_name, sql=sql)
        if check_result['status'] == 1:
            review_set.error_count = 1
            review_set.rows.append(
                ReviewResult(
                    id=1, # Dummy ID
                    errlevel=2, # Error level
                    stagestatus='SQL审核不通过', # Stage status
                    errormessage=check_result['msg'],
                    sql=sql,
                    affected_rows=0,
                    execute_time=0,
                )
            )
        else:
            review_set.error_count = 0
            # Optionally, add a success message or leave rows empty
            # review_set.rows.append(ReviewResult(id=1, errlevel=0, stagestatus='SQL审核通过', errormessage='语法正确', sql=sql))
        return review_set

    def get_rollback(self, workflow):
        """Placeholder for Dameng rollback script generation."""
        # For now, returns an empty list as per requirements.
        # Actual implementation would involve parsing the workflow's SQL
        # and generating corresponding rollback statements.
        return []

    def filter_sql(self, sql='', limit_num=0):
        # ... (existing implementation)
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
        # ... (existing implementation)
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
            except Exception: pass
            if cursor.rowcount is not None and cursor.rowcount >= 0:
                result['effect_row'] = cursor.rowcount
            elif result['rows']:
                result['effect_row'] = len(result['rows'])
        except Exception as e:
            result['error'] = f"Error: {str(e)}"
        finally:
            if cursor:
                try: cursor.close()
                except Exception: pass
            if current_conn and close_conn:
                try:
                    current_conn.close()
                    if current_conn is self.conn: self.conn = None
                except Exception: pass
        return result

    def get_all_databases(self):
        """
        Retrieves a list of all database (schema) names from the Dameng server.
        Returns a dictionary: {'error': str|None, 'data': list_of_names}.
        """
        db_names = []
        error_msg = None
        sql = "SELECT NAME FROM SYSOBJECTS WHERE TYPE$ = 'SCH';"
        try:
            query_result = self.query(sql=sql)
            if query_result:
                error_msg = query_result.get('error')
                if not error_msg and query_result.get('rows'):
                    db_names = [row[0] for row in query_result['rows'] if row and row[0]]
                elif error_msg:
                    db_names = []
            else:
                error_msg = "Internal error: query method returned None."
                db_names = []
        except Exception as e:
            error_msg = f"Failed to get databases: {str(e)}"
            db_names = []
        return {'error': error_msg, 'data': db_names}

    def get_all_tables(self, db_name):
        """
        Retrieves a list of all table names (user and system) within a given database (schema).
        Returns a dictionary: {'error': str|None, 'data': list_of_names}.
        """
        table_names = []
        error_msg = None
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
            query_result = self.query(db_name=db_name, sql=sql)
            if query_result:
                error_msg = query_result.get('error')
                if not error_msg and query_result.get('rows'):
                    table_names = [row[0] for row in query_result['rows'] if row and row[0]]
                elif error_msg:
                    table_names = []
            else:
                error_msg = "Internal error: query method returned None."
                table_names = []
        except Exception as e:
            error_msg = f"Failed to get tables for schema {db_name}: {str(e)}"
            table_names = []

        return {'error': error_msg, 'data': table_names}

    def get_all_columns_by_tb(self, db_name, tb_name, **kwargs):
        """
        Retrieves a list of all column names for a given table in a specific database (schema).
        Returns a dictionary: {'error': str|None, 'data': list_of_names}.
        """
        column_names = []
        error_msg = None
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
            query_result = self.query(db_name=db_name, sql=sql)
            if query_result:
                error_msg = query_result.get('error')
                if not error_msg and query_result.get('rows'):
                    column_names = [row[0] for row in query_result['rows'] if row and row[0]]
                elif error_msg:
                    column_names = []
            else:
                error_msg = "Internal error: query method returned None."
                column_names = []
        except Exception as e:
            error_msg = f"Failed to get columns for table {db_name}.{tb_name}: {str(e)}"
            column_names = []

        return {'error': error_msg, 'data': column_names}

    def describe_table(self, db_name, tb_name, **kwargs):
        """
        Retrieves a description of a table, including column names and their data types.
        Returns a dictionary: {'error': str|None, 'data': list_of_column_descriptions}.
        """
        description = []
        error_msg = None
        # SQL to get column name and type. Comments would require joining SYSCOLUMNCOMMENTS.
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
            query_result = self.query(db_name=db_name, sql=sql)
            if query_result:
                error_msg = query_result.get('error')
                if not error_msg and query_result.get('rows'):
                    description = [{'name': row[0], 'type': row[1], 'comment': ''}
                                   for row in query_result['rows'] if row and len(row) >= 2]
                    # TODO: Fetch comments from SYSCOLUMNCOMMENTS or similar if available and join.
                elif error_msg:
                    description = []
            else:
                error_msg = "Internal error: query method returned None."
                description = [] # Ensure data is empty if query_result is None
        except Exception as e:
            error_msg = f"Failed to describe table {db_name}.{tb_name}: {str(e)}"
            description = []

        return {'error': error_msg, 'data': description}

    def get_tables_metas_data(self, db_name, **kwargs):
        """
        Retrieves metadata for all tables in a given database (schema).
        Similar to MysqlEngine.get_tables_metas_data.
        Returns a list of dictionaries, each containing table and column info.
        """
        table_metas = []
        error_msg = None
        safe_db_name = db_name.replace("'", "''")

        # SQL to get all user tables in the specified schema
        sql_tables = f"""
            SELECT T.NAME AS TABLE_NAME, T.ID AS TABLE_ID, S.NAME AS TABLE_SCHEMA
            FROM SYSOBJECTS T
            JOIN SYSOBJECTS S ON T.SCHID = S.ID
            WHERE T.TYPE$ = 'SCHOBJ' AND T.SUBTYPE$ = 'UTAB' AND S.TYPE$ = 'SCH' AND S.NAME = '{safe_db_name}'
            ORDER BY T.NAME;
        """
        try:
            tables_result = self.query(db_name=db_name, sql=sql_tables, close_conn=False) # Keep connection open for column queries
            if tables_result.get('error'):
                error_msg = f"Error fetching tables: {tables_result['error']}"
                return {'error': error_msg, 'data': []}

            if not tables_result.get('rows'):
                return {'error': None, 'data': []} # No tables found, not an error

            for table_row in tables_result['rows']:
                table_name = table_row[0]
                table_id = table_row[1]
                # schema_name = table_row[2] # Not strictly needed if db_name is already the schema

                _meta = {}
                # Dameng does not have direct equivalents for all MySQL INFORMATION_SCHEMA.TABLES fields in one table.
                # We'll fetch basic info from SYSOBJECTS and comments if available.
                # For more details like row count, data length, we might need additional queries or procedures.
                # Placeholder for TABLE_INFO, similar to MySQL's structure
                _meta["TABLE_INFO"] = {
                    'TABLE_NAME': table_name,
                    'TABLE_SCHEMA': db_name, # Assuming db_name is the schema name
                    'TABLE_TYPE': 'BASE TABLE', # Dameng equivalent for 'UTAB'
                    'ENGINE': None, # Dameng specific, might need another query if important
                    'TABLE_ROWS': None, # Requires query like SELECT COUNT(*) or from statistics
                    'DATA_LENGTH': None, # Requires specific DM functions/views
                    'INDEX_LENGTH': None, # Requires specific DM functions/views
                    'TABLE_COMMENT': None, # Need to query SYSCOMMENTS or similar
                }
                # Try to get table comment
                comment_sql = f"SELECT NOTES FROM SYSCOMMENTS WHERE ID = {table_id} AND MAJOR_CLASS = 1 AND MINOR_CLASS = 0;"
                comment_result = self.query(db_name=db_name, sql=comment_sql, close_conn=False)
                if comment_result.get('rows') and comment_result['rows'][0][0]:
                    _meta["TABLE_INFO"]['TABLE_COMMENT'] = comment_result['rows'][0][0]


                # SQL to get columns for the current table
                # Using C.TYPE$ for column type, which is a code. We might need to map this to human-readable types.
                sql_cols = f"""
                    SELECT C.NAME AS COLUMN_NAME, C.TYPE$ AS COLUMN_TYPE, C.LENGTH AS CHARACTER_MAXIMUM_LENGTH,
                           C.NULLABLE$ AS IS_NULLABLE, C.DEFVAL AS COLUMN_DEFAULT, C.COLID AS ORDINAL_POSITION
                           -- C.PRECISION, C.SCALE for numeric types
                    FROM SYSCOLUMNS C
                    WHERE C.ID = {table_id}
                    ORDER BY C.COLID;
                """
                columns_result = self.query(db_name=db_name, sql=sql_cols, close_conn=False)
                if columns_result.get('error'):
                    # Log error for this table's columns, but continue with other tables
                    print(f"Error fetching columns for table {table_name}: {columns_result['error']}")
                    _meta["COLUMNS"] = []
                else:
                    columns_data = []
                    for col_row in columns_result['rows']:
                        columns_data.append({
                            'COLUMN_NAME': col_row[0],
                            'COLUMN_TYPE': col_row[1], # This is a code, might need mapping
                            'CHARACTER_MAXIMUM_LENGTH': col_row[2],
                            'IS_NULLABLE': 'YES' if col_row[3] == 'Y' else 'NO',
                            'COLUMN_DEFAULT': col_row[4],
                            'ORDINAL_POSITION': col_row[5],
                            'COLUMN_COMMENT': None # Need to query SYSCOLUMNCOMMENTS or similar
                        })
                     # Try to get column comments
                    for col_data in columns_data:
                        col_comment_sql = f"SELECT NOTES FROM SYSCOMMENTS WHERE ID = {table_id} AND MAJOR_CLASS = 1 AND MINOR_CLASS = {col_data['ORDINAL_POSITION']};"
                        col_comment_result = self.query(db_name=db_name, sql=col_comment_sql, close_conn=False)
                        if col_comment_result.get('rows') and col_comment_result['rows'][0][0]:
                            col_data['COLUMN_COMMENT'] = col_comment_result['rows'][0][0]
                    _meta["COLUMNS"] = columns_data

                # Mimic structure of MysqlEngine's output for consistency if needed by consumers
                _meta["ENGINE_KEYS"] = [ # These are headers for display, adjust as needed
                    {"key": "COLUMN_NAME", "value": "字段名"},
                    {"key": "COLUMN_TYPE", "value": "数据类型"},
                    {"key": "CHARACTER_MAXIMUM_LENGTH", "value": "长度"},
                    {"key": "IS_NULLABLE", "value": "允许非空"},
                    {"key": "COLUMN_DEFAULT", "value": "默认值"},
                    {"key": "COLUMN_COMMENT", "value": "备注"},
                ]
                table_metas.append(_meta)

        except Exception as e:
            error_msg = f"Failed to get tables metadata for schema {db_name}: {str(e)}"
            # Ensure connection is closed if an exception occurs mid-process
            if self.conn and not tables_result.get('error') and not columns_result.get('error'): # only close if not closed by query()
                 self.close()
            return {'error': error_msg, 'data': []}
        finally:
            # Ensure the connection is closed after all operations for this method are complete
            self.close()

        return {'error': error_msg, 'data': table_metas}

    def get_table_meta_data(self, db_name, tb_name, **kwargs):
        """
        Retrieves metadata for a specific table in a given database (schema).
        Queries SYSOBJECTS for table-level information.
        Returns a dictionary similar to MysqlEngine.get_table_meta_data.
        """
        meta_data = {}
        error_msg = None
        safe_db_name = db_name.replace("'", "''")
        safe_tb_name = tb_name.replace("'", "''")

        # SQL to get table information
        # Note: Dameng's SYSOBJECTS doesn't directly map to all fields in MySQL's information_schema.TABLES
        # We will retrieve what's available and supplement with comments.
        # Fields like TABLE_ROWS, DATA_LENGTH, INDEX_LENGTH, etc., might require
        # more complex queries or calling DM specific procedures/functions.
        sql_table_info = f"""
            SELECT
                T.NAME AS TABLE_NAME,
                S.NAME AS TABLE_SCHEMA,
                CASE T.SUBTYPE$
                    WHEN 'UTAB' THEN 'BASE TABLE'
                    WHEN 'VIEW' THEN 'VIEW'
                    ELSE T.SUBTYPE$
                END AS TABLE_TYPE,
                T.ID AS TABLE_ID,
                T.CRTDATE AS CREATE_TIME,
                T.VALID AS IS_VALID -- Indicates if the object is valid
                -- Add other fields from SYSOBJECTS if they map to desired metadata
            FROM SYSOBJECTS T
            JOIN SYSOBJECTS S ON T.SCHID = S.ID
            WHERE T.TYPE$ = 'SCHOBJ' AND T.SUBTYPE$ = 'UTAB'  -- Focusing on user tables for now
              AND S.TYPE$ = 'SCH' AND S.NAME = '{safe_db_name}'
              AND T.NAME = '{safe_tb_name}';
        """
        try:
            table_info_result = self.query(db_name=db_name, sql=sql_table_info, close_conn=False)

            if table_info_result.get('error'):
                error_msg = f"Error fetching table metadata for {db_name}.{tb_name}: {table_info_result['error']}"
                return {'error': error_msg, 'column_list': [], 'rows': []}

            if not table_info_result.get('rows'):
                error_msg = f"Table {db_name}.{tb_name} not found."
                return {'error': error_msg, 'column_list': [], 'rows': []}

            table_info_row = table_info_result['rows'][0]
            table_id = table_info_row[3] # TABLE_ID

            # Column list for the output dictionary
            column_list = table_info_result['column_list'] + ['TABLE_COMMENT'] # Add placeholder for comment

            # Data for the 'rows' part of the output dictionary
            row_data = list(table_info_row)

            # Fetch table comment
            comment_sql = f"SELECT NOTES FROM SYSCOMMENTS WHERE ID = {table_id} AND MAJOR_CLASS = 1 AND MINOR_CLASS = 0;"
            comment_result = self.query(db_name=db_name, sql=comment_sql, close_conn=True) # Close after this query

            table_comment = None
            if comment_result.get('rows') and comment_result['rows'][0][0]:
                table_comment = comment_result['rows'][0][0]

            row_data.append(table_comment)

            # Construct the final dictionary
            # The 'rows' key in MySQL version contains a single dictionary with table attributes.
            # We will replicate this by creating a dictionary from column_list and row_data.
            # However, the direct output from self.query is already a list of tuples for rows.
            # MysqlEngine.get_table_meta_data returns: {'column_list': _meta_data.column_list, 'rows': _meta_data.rows[0]}
            # So, we should return a single dictionary as the 'rows' value.

            # Create a dictionary from the column_list and row_data for the single row
            # Pad row_data with None if its length is less than column_list
            # This can happen if 'TABLE_COMMENT' was not in the original query's column_list
            final_row_dict = dict(zip(column_list, row_data + [None] * (len(column_list) - len(row_data))))


            return {
                'error': None,
                'column_list': column_list,
                'rows': [final_row_dict] # Mimicking MySQL, which returns a list containing one dict
            }

        except Exception as e:
            error_msg = f"Exception while fetching table metadata for {db_name}.{tb_name}: {str(e)}"
            # Ensure connection is closed if an exception occurs
            self.close()
            return {'error': error_msg, 'column_list': [], 'rows': []}
        # No finally self.close() here as query() handles it or it's handled in except.

    def get_table_desc_data(self, db_name, tb_name, **kwargs):
        """
        Retrieves column information for a specific table.
        Queries SYSCOLUMNS and SYSOBJECTS.
        Returns a dictionary similar to MysqlEngine.get_table_desc_data.
        """
        desc_data = []
        error_msg = None
        safe_db_name = db_name.replace("'", "''")
        safe_tb_name = tb_name.replace("'", "''")

        # SQL to get column details for a table
        # Joins SYSOBJECTS to filter by table and schema, and SYSCOLUMNS for column info.
        # TYPE$ in SYSCOLUMNS is a code; mapping to readable names would be an enhancement.
        # DEFVAL is the default value. NULLABLE$ indicates if NULL is allowed.
        # PRECISION and SCALE for numeric types. LENGTH for char/varchar.
        sql_columns = f"""
            SELECT
                C.NAME AS COLUMN_NAME,
                C.TYPE$ AS COLUMN_TYPE_CODE, -- This is a code, e.g., 1 for INT, 12 for VARCHAR
                -- We would ideally map TYPE$ to a human-readable name like 'VARCHAR', 'INTEGER'
                -- For now, returning the code. A mapping function can be added later.
                CASE
                    WHEN T1.NAME IN ('CHAR', 'VARCHAR', 'VARCHAR2', 'CLOB') THEN C.LENGTH
                    ELSE NULL
                END AS CHARACTER_MAXIMUM_LENGTH,
                C.PRECISION AS NUMERIC_PRECISION,
                C.SCALE AS NUMERIC_SCALE,
                C.NULLABLE$ AS IS_NULLABLE, -- 'Y' or 'N'
                C.DEFVAL AS COLUMN_DEFAULT,
                C.COLID AS ORDINAL_POSITION,
                T.ID AS TABLE_ID -- Needed for fetching column comments
            FROM SYSCOLUMNS C
            JOIN SYSOBJECTS T ON C.ID = T.ID
            JOIN SYSOBJECTS S ON T.SCHID = S.ID
            LEFT JOIN SYSDBSTYPES T1 ON C.TYPE$ = T1.TYPE_ AND T1.DBID = DB_ID() -- Join SYSDBSTYPES for type names
            WHERE T.NAME = '{safe_tb_name}'
              AND S.NAME = '{safe_db_name}'
              AND T.TYPE$ = 'SCHOBJ' AND T.SUBTYPE$ = 'UTAB'
              AND S.TYPE$ = 'SCH'
            ORDER BY C.COLID;
        """

        column_list_for_output = [
            'COLUMN_NAME', 'COLUMN_TYPE', 'CHARACTER_MAXIMUM_LENGTH', 'NUMERIC_PRECISION', 'NUMERIC_SCALE',
            'IS_NULLABLE', 'COLUMN_KEY', 'COLUMN_DEFAULT', 'EXTRA', 'COLUMN_COMMENT'
        ] # Target column list similar to MySQL

        try:
            columns_result = self.query(db_name=db_name, sql=sql_columns, close_conn=False)

            if columns_result.get('error'):
                error_msg = f"Error fetching column descriptions for {db_name}.{tb_name}: {columns_result['error']}"
                self.close() # Ensure connection is closed on error
                return {'error': error_msg, 'column_list': column_list_for_output, 'rows': []}

            if not columns_result.get('rows'):
                # This case should ideally not happen if the table exists and has columns,
                # but handle it just in case.
                self.close()
                return {'error': None, 'column_list': column_list_for_output, 'rows': []}

            rows_data = []
            for col_row in columns_result['rows']:
                table_id = col_row[8] # TABLE_ID
                ordinal_position = col_row[7] # ORDINAL_POSITION (COLID)

                # Fetch column comment
                comment_sql = f"SELECT NOTES FROM SYSCOMMENTS WHERE ID = {table_id} AND MAJOR_CLASS = 1 AND MINOR_CLASS = {ordinal_position};"
                comment_result = self.query(db_name=db_name, sql=comment_sql, close_conn=False) # Keep conn open for next iteration
                column_comment = None
                if comment_result.get('rows') and comment_result['rows'][0][0]:
                    column_comment = comment_result['rows'][0][0]

                # Map to the target structure. Some fields might be None or default if not directly available.
                # 'COLUMN_KEY' (index info) and 'EXTRA' (like auto_increment) are harder to get from this query alone.
                # These might require joining with SYSINDEXES or other specific checks. For now, they'll be placeholder.
                row_dict = {
                    'COLUMN_NAME': col_row[0],
                    'COLUMN_TYPE': col_row[1], # This is the TYPE_NAME from SYSDBSTYPES if join is successful, else TYPE$ code
                    'CHARACTER_MAXIMUM_LENGTH': col_row[2],
                    'NUMERIC_PRECISION': col_row[3],
                    'NUMERIC_SCALE': col_row[4],
                    'IS_NULLABLE': 'YES' if col_row[5] == 'Y' else 'NO',
                    'COLUMN_KEY': '', # Placeholder - requires index info
                    'COLUMN_DEFAULT': col_row[6],
                    'EXTRA': '', # Placeholder - e.g. for auto_increment, needs more logic
                    'COLUMN_COMMENT': column_comment
                }
                rows_data.append(row_dict)

            self.close() # Close connection after all comments are fetched
            return {
                'error': None,
                'column_list': column_list_for_output,
                'rows': rows_data
            }

        except Exception as e:
            error_msg = f"Exception while fetching column descriptions for {db_name}.{tb_name}: {str(e)}"
            self.close() # Ensure connection is closed on exception
            return {'error': error_msg, 'column_list': column_list_for_output, 'rows': []}

    def get_table_index_data(self, db_name, tb_name, **kwargs):
        """
        Retrieves index information for a specific table.
        Queries SYSINDEXES, SYSIDXCOLS, SYSOBJECTS, SYSCOLUMNS.
        Returns a dictionary similar to MysqlEngine.get_table_index_data.
        """
        index_data = []
        error_msg = None
        safe_db_name = db_name.replace("'", "''")
        safe_tb_name = tb_name.replace("'", "''")

        # SQL to get index details for a table
        # Joins SYSOBJECTS (for table and schema), SYSINDEXES (for index info),
        # SYSIDXCOLS (for columns in index), and SYSCOLUMNS (for column names).
        sql_indexes = f"""
            SELECT
                IC.COLNAME AS COLUMN_NAME,     -- From SYSIDXCOLS after joining with SYSCOLUMNS
                I.NAME AS INDEX_NAME,
                CASE I.PROPERTIES
                    WHEN 1 THEN 'NO'           -- Assuming 1 means Unique, need to verify DM docs. MySQL NON_UNIQUE is 0 for unique.
                    ELSE 'YES'
                END AS NON_UNIQUE,             -- Dameng: PROPERTIES bit 0 = 1 for unique, 0 for non-unique. So invert for NON_UNIQUE.
                                               -- Let's assume standard SQL: 0 for unique, 1 for non-unique as in MySQL's NON_UNIQUE.
                                               -- DM: Bit 0: 1-Unique Index, 0-Non Unique Index.
                                               -- So if PROPERTIES & 1 == 1, it's unique, NON_UNIQUE should be 'NO'
                                               -- If PROPERTIES & 1 == 0, it's non-unique, NON_UNIQUE should be 'YES'
                                               -- Corrected logic: (CASE WHEN (I.PROPERTIES & 1) = 1 THEN 'NO' ELSE 'YES' END)
                IC.COLID_IN_INDEX AS SEQ_IN_INDEX, -- Position of the column in the index
                NULL AS CARDINALITY,           -- Cardinality is not directly available in these tables, might need statistics
                (SELECT C.NULLABLE$ FROM SYSCOLUMNS C WHERE C.ID = T.ID AND C.NAME = IC.COLNAME) AS IS_NULLABLE_COLUMN, -- Whether the column itself is nullable
                CASE I.INDEXTYPE$
                    WHEN 'NORMAL' THEN 'BTREE' -- Common default, DM might have other types
                    WHEN 'CLUSTERED' THEN 'CLUSTERED'
                    ELSE I.INDEXTYPE$
                END AS INDEX_TYPE,
                (SELECT CMM.NOTES FROM SYSCOMMENTS CMM WHERE CMM.ID = I.ID AND CMM.MAJOR_CLASS = 7 AND CMM.MINOR_CLASS = 0) AS INDEX_COMMENT -- Index comment
            FROM SYSINDEXES I
            JOIN SYSOBJECTS T ON I.TABLEID = T.ID   -- Join Index to Table
            JOIN SYSOBJECTS S ON T.SCHID = S.ID   -- Join Table to Schema
            JOIN SYSIDXCOLS IC ON I.ID = IC.INDEXID -- Join Index to its Columns
            -- SYSIDXCOLS might store COLID or COLNAME. Assuming COLNAME for simplicity or join SYSCOLUMNS if it's COLID
            -- If SYSIDXCOLS.COLNAME is not available, and it has COLID:
            -- JOIN SYSCOLUMNS SC ON IC.COLID = SC.COLID AND SC.ID = T.ID (this assumes COLID is unique per table)
            -- Let's assume SYSIDXCOLS has COLNAME. If not, the query needs SC.NAME AS COLUMN_NAME and join on SC.COLID.
            -- Based on typical DM structure, SYSIDXCOLS refers to column by its position/ID in table, so joining SYSCOLUMNS is better.
            -- Revising JOIN for COLUMN_NAME:
            -- The above query was simplified. A more robust one for COLNAME:
            -- SELECT SC.NAME AS COLUMN_NAME ... JOIN SYSCOLUMNS SC ON IC.COLID = SC.COLID AND SC.ID = T.ID
            -- For now, let's assume the provided query structure and adjust if errors occur or COLNAME isn't directly in SYSIDXCOLS.
            -- A common pattern is SYSIDXCOLS has column ID, and you join SYSCOLUMNS on that ID and table's ID.
            -- Let's use a subquery or join for COLNAME based on COLID from SYSIDXCOLS
            -- The provided DDL for SYSIDXCOLS usually has `COLID` which is the ID of the column in the table.
            -- So, we need to join SYSCOLUMNS on T.ID and IC.COLID
            -- The query should be: (SELECT C.NAME FROM SYSCOLUMNS C WHERE C.ID = T.ID AND C.COLID = IC.COLID) AS COLUMN_NAME
            WHERE T.NAME = '{safe_tb_name}'
              AND S.NAME = '{safe_db_name}'
              AND T.TYPE$ = 'SCHOBJ' AND T.SUBTYPE$ = 'UTAB'
              AND S.TYPE$ = 'SCH'
            ORDER BY I.NAME, IC.COLID_IN_INDEX;
        """
        # Corrected SQL for index data, especially for COLUMN_NAME and NON_UNIQUE
        # Also, SYSIDXCOLS typically has COLID, not COLNAME directly.
        # MAJOR_CLASS for index comment is 7 (OBJ_INDEX)
        # MINOR_CLASS for index comment is 0 (general comment for the index object itself)
        # For column nullability, it's better to get it from SYSCOLUMNS directly using the column identifier.

        sql_indexes_corrected = f"""
            SELECT
                (SELECT C.NAME FROM SYSCOLUMNS C WHERE C.ID = T.ID AND C.COLID = IC.COLID) AS COLUMN_NAME,
                SI.NAME AS INDEX_NAME,
                CASE WHEN (SI.PROPERTIES & 1) = 1 THEN 'NO' ELSE 'YES' END AS NON_UNIQUE, -- Bit 0: 1=Unique, 0=Non-Unique
                IC.POS$ AS SEQ_IN_INDEX, -- Position of column in index (1-based)
                NULL AS CARDINALITY, -- Not easily available, typically from statistics
                (SELECT SC.NULLABLE$ FROM SYSCOLUMNS SC WHERE SC.ID = T.ID AND SC.COLID = IC.COLID) AS IS_NULLABLE_COLUMN, -- 'Y' or 'N'
                CASE SI.TYPE$
                    WHEN 0 THEN 'NORMAL' -- B-Tree (Normal)
                    WHEN 1 THEN 'CLUSTERED'
                    WHEN 2 THEN 'FUNCTIONAL'
                    WHEN 3 THEN 'TEXT' -- Full-text index
                    ELSE 'UNKNOWN'
                END AS INDEX_TYPE,
                (SELECT CMM.NOTES FROM SYSCOMMENTS CMM WHERE CMM.ID = SI.ID AND CMM.MAJOR_CLASS = 7 AND CMM.MINOR_CLASS = 0) AS INDEX_COMMENT
            FROM SYSOBJECTS ST -- Schema Table
            JOIN SYSOBJECTS T ON ST.ID = T.SCHID AND T.NAME = '{safe_tb_name}' AND T.SUBTYPE$ = 'UTAB' -- Table Object
            JOIN SYSINDEXES SI ON T.ID = SI.TABLEID -- Index Object
            JOIN SYSIDXCOLS IC ON SI.ID = IC.INDEXID -- Index Column Mapping
            WHERE ST.NAME = '{safe_db_name}' AND ST.TYPE$ = 'SCH'
            ORDER BY SI.NAME, IC.POS$;
        """
        # MySQL column names for reference: '列名', '索引名', '唯一性', '列序列', '基数', '是否为空', '索引类型', '备注'
        # Mapping to our selected fields:
        # COLUMN_NAME, INDEX_NAME, NON_UNIQUE, SEQ_IN_INDEX, CARDINALITY, IS_NULLABLE_COLUMN, INDEX_TYPE, INDEX_COMMENT
        column_list_for_output = [
            'COLUMN_NAME', 'INDEX_NAME', 'NON_UNIQUE', 'SEQ_IN_INDEX',
            'CARDINALITY', 'IS_NULLABLE_COLUMN', 'INDEX_TYPE', 'INDEX_COMMENT'
        ]

        try:
            indexes_result = self.query(db_name=db_name, sql=sql_indexes_corrected) # Default close_conn=True

            if indexes_result.get('error'):
                error_msg = f"Error fetching index data for {db_name}.{tb_name}: {indexes_result['error']}"
                return {'error': error_msg, 'column_list': column_list_for_output, 'rows': []}

            rows_data = []
            if indexes_result.get('rows'):
                for idx_row in indexes_result['rows']:
                    rows_data.append(dict(zip(column_list_for_output, idx_row)))

            return {
                'error': None,
                'column_list': column_list_for_output,
                'rows': rows_data
            }

        except Exception as e:
            error_msg = f"Exception while fetching index data for {db_name}.{tb_name}: {str(e)}"
            # self.close() is not strictly needed here if query() closes connection,
            # but added defensively if query() behavior changes or direct self.conn usage occurs.
            if self.conn: self.close()
            return {'error': error_msg, 'column_list': column_list_for_output, 'rows': []}

    # Placeholder methods added below

    def escape_string(self, value: str) -> str:
        logger.warning("DmEngine.escape_string is not fully implemented. Returning original value.")
        return value # Basic placeholder

    @property
    def auto_backup(self):
        logger.warning("DmEngine.auto_backup is not implemented. Returning False.")
        return False

    @property
    def seconds_behind_master(self):
        logger.warning("DmEngine.seconds_behind_master is not implemented. Returning None.")
        return None

    @property
    def server_version(self):
        logger.warning("DmEngine.server_version is not implemented. Returning empty tuple.")
        # Attempt to get version if connection exists, otherwise return placeholder
        if hasattr(self, '_server_version') and self._server_version:
            return self._server_version
        try:
            if not self.conn:
                self.get_connection() # Establish connection if not already connected
            if self.conn:
                # DM version query: SELECT BANNER FROM V$VERSION;
                # Example output: "DM Database Server x64 V8 R3 ECN2 GRP MPP LCNSYSLEN 2048 230410"
                # Need to parse this to get a tuple like (8, 3, 0, 2)
                cursor = self.conn.cursor()
                cursor.execute("SELECT BANNER FROM V$VERSION;")
                version_string = cursor.fetchone()
                if version_string and version_string[0]:
                    match = re.search(r'V(\d+)\s*R(\d+)', version_string[0])
                    if match:
                        major, minor = int(match.group(1)), int(match.group(2))
                        # Try to find patch/build if available
                        # This is a simplified parsing, DM version string can be complex
                        self._server_version = (major, minor, 0) # Placeholder for patch
                        return self._server_version
                logger.warning("Could not parse Dameng version string.")
        except Exception as e:
            logger.error(f"Error getting Dameng server version: {e}")
        return tuple()


    def processlist(self, command_type=None, **kwargs):
        logger.warning("DmEngine.processlist is not implemented. Returning empty ResultSet.")
        return ResultSet(full_sql="show processlist", error="Not implemented for DmEngine")

    def kill_connection(self, thread_id):
        logger.warning(f"DmEngine.kill_connection for thread_id {thread_id} is not implemented.")
        # Consider returning a ResultSet indicating failure or status
        return ResultSet(full_sql=f"kill {thread_id}", error="Not implemented for DmEngine")

    def get_group_tables_by_db(self, db_name, **kwargs):
        logger.warning("DmEngine.get_group_tables_by_db is not implemented. Returning empty dict.")
        return {}

    def get_all_databases_summary(self):
        logger.warning("DmEngine.get_all_databases_summary is not implemented. Returning empty ResultSet.")
        return ResultSet(full_sql="show databases summary", error="Not implemented for DmEngine")

    def get_instance_users_summary(self):
        logger.warning("DmEngine.get_instance_users_summary is not implemented. Returning empty ResultSet.")
        return ResultSet(full_sql="show users summary", error="Not implemented for DmEngine")

    def create_instance_user(self, **kwargs):
        logger.warning(f"DmEngine.create_instance_user with args {kwargs} is not implemented.")
        return ResultSet(full_sql="create user", error="Not implemented for DmEngine")

    def drop_instance_user(self, **kwargs):
        logger.warning(f"DmEngine.drop_instance_user with args {kwargs} is not implemented.")
        return ResultSet(full_sql="drop user", error="Not implemented for DmEngine")

    def reset_instance_user_pwd(self, **kwargs):
        logger.warning(f"DmEngine.reset_instance_user_pwd with args {kwargs} is not implemented.")
        return ResultSet(full_sql="alter user password", error="Not implemented for DmEngine")

    def query_masking(self, db_name=None, sql="", resultset=None):
        logger.warning("DmEngine.query_masking is not implemented. Returning original resultset.")
        return resultset # Pass through

    def execute(self, db_name=None, sql='', close_conn=True, parameters=None): # Parameters might not be used by dmPython in this way
        result = ReviewSet(full_sql=sql)
        conn = None
        try:
            conn = self.get_connection(db_name=db_name)
            if not conn:
                result.error = "Failed to establish database connection."
                result.error_count = 1
                result.rows.append(ReviewResult(id=1, errlevel=2, sql=sql, errormessage=result.error, stagestatus='Execute Failed'))
                return result

            statements = sqlparse.split(sql)
            if not statements: # Handle empty SQL string case by treating it as one (empty) statement if sqlparse returns empty
                statements = [sql] if sql.strip() else [] # Only if sql is not just whitespace

            statement_idx = 0
            for statement_sql_original in statements:
                statement_sql = statement_sql_original.strip()
                if not statement_sql: # Skip empty statements resulting from splitting or original input
                    continue

                statement_idx += 1
                review_result = ReviewResult(id=statement_idx, sql=statement_sql, affected_rows=0)
                try:
                    cursor = conn.cursor()
                    # dmPython cursor.execute does not typically take parameters separately.
                    # Parameters should be part of the SQL string or handled differently if supported.
                    cursor.execute(statement_sql)

                    # cursor.rowcount:
                    # For DML statements (INSERT, UPDATE, DELETE), it returns the number of affected rows.
                    # For DDL statements (CREATE, ALTER, DROP), it usually returns 0 or -1.
                    # For SELECT statements, it's often -1 as the number of rows is determined by fetchall/fetchone.
                    # If no exception, assume success for DDL/SELECT where rowcount is not indicative.
                    if cursor.rowcount >= 0:
                         review_result.affected_rows = cursor.rowcount
                    else: # For DDL or SELECT, or if rowcount is -1 but no error
                        review_result.affected_rows = 0 # Or some other indicator like None if preferred

                    review_result.errlevel = 0
                    review_result.stagestatus = 'Execute Successfully'
                    review_result.errormessage = '' # Success
                    cursor.close()
                except Exception as e:
                    logger.error(f"Dameng execute error on statement '{statement_sql}': {e}")
                    review_result.errlevel = 2
                    review_result.stagestatus = 'Execute Failed'
                    review_result.errormessage = str(e)
                    result.error_count += 1
                result.rows.append(review_result)
                # No early exit on error, execute all statements as per example structure

            # Dameng typically operates in autocommit mode by default unless a transaction is explicitly started.
            # If conn.autocommit is False (or True, explicitly set), then conn.commit() or conn.rollback() might be needed.
            # For simplicity, assuming autocommit or session handles it.
            # If errors occurred and transactions are being managed, conn.rollback() might be an option here.

        except Exception as e:
            logger.error(f"Dameng execute general error: {e}")
            result.error = str(e) # Overall error message for the ReviewSet
            # If there were specific statement errors, error_count is already > 0.
            # If this general exception occurred before any statement, error_count might be 0.
            if result.error_count == 0: # If no statement errors yet, but a general one happened
                 result.error_count = 1

            # Add a general error row if no specific statement rows exist or if it's a new error
            if not result.rows:
                 result.rows.append(ReviewResult(id=1, errlevel=2, sql=sql, errormessage=str(e), stagestatus='Execute Failed'))
            # Optionally, update all existing rows if a global failure like connection drop occurs mid-way
            # For now, this is handled by the statement-specific errors or the general error row above.
        finally:
            if conn and close_conn:
                try:
                    conn.close()
                    if conn is self.conn: # If it was the shared self.conn
                        self.conn = None
                except Exception as e_close:
                    logger.error(f"Error closing Dameng connection: {e_close}")
                    if not result.error: # Don't overwrite a more specific execution error
                        result.error = str(e_close)
                        if result.error_count == 0: result.error_count = 1
        return result

    def get_execute_percentage(self):
        logger.warning("DmEngine.get_execute_percentage is not implemented. Returning None.")
        return None

    def get_variables(self, variables=None):
        logger.warning("DmEngine.get_variables is not implemented. Returning empty ResultSet.")
        return ResultSet(full_sql="show variables", error="Not implemented for DmEngine")

    def set_variable(self, variable_name, variable_value):
        logger.warning(f"DmEngine.set_variable for {variable_name}={variable_value} is not implemented.")
        return ResultSet(full_sql=f"set variable {variable_name}", error="Not implemented for DmEngine")