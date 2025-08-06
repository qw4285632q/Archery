# -*- coding: UTF-8 -*-
import logging
import traceback
import re
import sqlparse
import json

from . import EngineBase
import pyodbc
from .models import ResultSet, ReviewSet, ReviewResult
from sql.models import SqlBackupHistory
from sql.utils.data_masking import brute_mask

logger = logging.getLogger("default")


class MssqlEngine(EngineBase):
    test_query = "SELECT 1"

    def get_connection(self, db_name=None):
        connstr = """DRIVER=ODBC Driver 18 for SQL Server;SERVER={0},{1};UID={2};PWD={3};
client charset = UTF-8;connect timeout=10;CHARSET={4};TrustServerCertificate=yes;""".format(
            self.host,
            self.port,
            self.user,
            self.password,
            self.instance.charset or "UTF8",
        )
        if db_name:
            connstr = f"{connstr};DATABASE={db_name}"
        if self.conn:
            return self.conn
        self.conn = pyodbc.connect(connstr)
        return self.conn

    name = "MsSQL"

    info = "MsSQL engine"

    @property
    def auto_backup(self):
        return True

    def get_all_databases(self):
        """获取数据库列表, 返回一个ResultSet"""
        sql = "SELECT name FROM master.sys.databases order by name"
        result = self.query(sql=sql)
        db_list = [
            row[0]
            for row in result.rows
            if row[0] not in ("master", "msdb", "tempdb", "model")
        ]
        result.rows = db_list
        return result

    def get_all_tables(self, db_name, **kwargs):
        """获取table 列表, 返回一个ResultSet"""
        sql = """SELECT TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_TYPE = 'BASE TABLE' order by TABLE_NAME;"""
        result = self.query(db_name=db_name, sql=sql)
        tb_list = [row[0] for row in result.rows if row[0] not in ["test"]]
        result.rows = tb_list
        return result

    def get_group_tables_by_db(self, db_name):
        """
        根据传入的数据库名，获取该库下的表和注释，并按首字符分组，比如 'a': ['account1','apply']
        :param db_name:
        :return:
        """
        data = {}
        sql = f"""
        SELECT t.name AS table_name, 
            case when td.value is not null then convert(varchar(max),td.value) else '' end AS table_comment
        FROM    sysobjects t
        LEFT OUTER JOIN sys.extended_properties td
        ON      td.major_id = t.id
        AND     td.minor_id = 0
        AND     td.name = 'MS_Description'
        WHERE t.type = 'u' ORDER BY t.name;"""
        result = self.query(db_name=db_name, sql=sql)
        for row in result.rows:
            table_name, table_cmt = row[0], row[1]
            if table_name[0] not in data:
                data[table_name[0]] = list()
            data[table_name[0]].append([table_name, table_cmt])
        return data

    def get_table_meta_data(self, db_name, tb_name, **kwargs):
        """数据字典页面使用：获取表格的元信息，返回一个dict{column_list: [], rows: []}"""
        sql = f"""
            SELECT space.*,table_comment,index_length,IDENT_CURRENT(?) as auto_increment
            FROM (
            SELECT 
                t.NAME AS table_name,
                t.create_date as create_time,
                t.modify_date as update_time,
                p.rows AS table_rows,
                SUM(a.total_pages) * 8 AS data_total,
                SUM(a.used_pages) * 8 AS data_length,
                (SUM(a.total_pages) - SUM(a.used_pages)) * 8 AS data_free
            FROM 
                sys.tables t
            INNER JOIN      
                sys.indexes i ON t.OBJECT_ID = i.object_id
            INNER JOIN 
                sys.partitions p ON i.object_id = p.OBJECT_ID AND i.index_id = p.index_id
            INNER JOIN 
                sys.allocation_units a ON p.partition_id = a.container_id
            WHERE 
                t.NAME =?
                AND t.is_ms_shipped = 0
                AND i.OBJECT_ID > 255 
            GROUP BY 
                t.Name, t.create_date, t.modify_date, p.Rows) 
            AS space 
            INNER JOIN (
            SELECT      t.name AS table_name,
                        convert(varchar(max),td.value) AS table_comment
            FROM		sysobjects t
            LEFT OUTER JOIN sys.extended_properties td
                ON      td.major_id = t.id
                AND     td.minor_id = 0
                AND     td.name = 'MS_Description'
            WHERE t.type = 'u' and t.name = '{tb_name}') AS comment 
            ON space.table_name = comment.table_name
            INNER JOIN (
            SELECT
                t.NAME				AS table_name,
                SUM(page_count * 8) AS index_length
            FROM sys.dm_db_index_physical_stats(
                db_id(), object_id(?), NULL, NULL, 'DETAILED') AS s
            JOIN sys.indexes AS i
            ON s.[object_id] = i.[object_id] AND s.index_id = i.index_id
            INNER JOIN      
                sys.tables t ON t.OBJECT_ID = i.object_id
            GROUP BY t.NAME
            ) AS index_size 
            ON index_size.table_name = space.table_name;
        """
        _meta_data = self.query(
            db_name,
            sql,
            parameters=(
                tb_name,
                tb_name,
                tb_name,
            ),
        )
        return {"column_list": _meta_data.column_list, "rows": _meta_data.rows[0]}

    def get_table_desc_data(self, db_name, tb_name, **kwargs):
        """获取表格字段信息"""
        sql = f"""
            select COLUMN_NAME 列名, case when ISNUMERIC(CHARACTER_MAXIMUM_LENGTH)=1 
then DATA_TYPE + '(' + convert(varchar(max), CHARACTER_MAXIMUM_LENGTH) + ')' else DATA_TYPE end 列类型,
                COLLATION_NAME 列字符集,
                IS_NULLABLE 是否为空,
                COLUMN_DEFAULT 默认值
            from INFORMATION_SCHEMA.columns where TABLE_CATALOG=? and TABLE_NAME = ?;"""
        _desc_data = self.query(
            db_name,
            sql,
            parameters=(
                db_name,
                tb_name,
            ),
        )
        return {"column_list": _desc_data.column_list, "rows": _desc_data.rows}

    def get_table_index_data(self, db_name, tb_name, **kwargs):
        """获取表格索引信息"""
        sql = f"""SELECT 
stuff((select ',' + COL_NAME(t.object_id,t.column_id) from sys.index_columns as t where i.object_id = t.object_id and 
i.index_id = t.index_id and t.is_included_column = 0 order by key_ordinal for xml path('')),1,1,'') as 列名,
                i.name AS 索引名,
                is_unique as 唯一性,is_primary_key as 是否主建
            FROM sys.indexes AS i  
            WHERE i.object_id = OBJECT_ID(?)
            group by i.name,i.object_id,i.index_id,is_unique,is_primary_key;"""
        _index_data = self.query(db_name, sql, parameters=(tb_name,))
        return {"column_list": _index_data.column_list, "rows": _index_data.rows}

    def get_tables_metas_data(self, db_name, **kwargs):
        """获取数据库所有表格信息，用作数据字典导出接口"""
        sql = """SELECT t.name AS TABLE_NAME, 
            case when td.value is not null then convert(varchar(max),td.value) else '' end AS TABLE_COMMENT
        FROM    sysobjects t
        LEFT OUTER JOIN sys.extended_properties td
        ON      td.major_id = t.id
        AND     td.minor_id = 0
        AND     td.name = 'MS_Description'
        WHERE t.type = 'u' ORDER BY t.name;"""
        result = self.query(db_name=db_name, sql=sql)
        # query result to dict
        tbs = []
        for row in result.rows:
            tbs.append(dict(zip(result.column_list, row)))
        table_metas = []
        for tb in tbs:
            _meta = dict()
            engine_keys = [
                {"key": "COLUMN_NAME", "value": "字段名"},
                {"key": "COLUMN_TYPE", "value": "数据类型"},
                {"key": "COLLATION_NAME", "value": "列字符集"},
                {"key": "IS_NULLABLE", "value": "允许非空"},
                {"key": "COLUMN_DEFAULT", "value": "默认值"},
            ]
            _meta["ENGINE_KEYS"] = engine_keys
            _meta["TABLE_INFO"] = tb
            sql_cols = f"""select COLUMN_NAME, case when ISNUMERIC(CHARACTER_MAXIMUM_LENGTH)=1 
then DATA_TYPE + '(' + convert(varchar(max), CHARACTER_MAXIMUM_LENGTH) + ')' else DATA_TYPE end COLUMN_TYPE,
                COLLATION_NAME,
                IS_NULLABLE,
                COLUMN_DEFAULT
            from INFORMATION_SCHEMA.columns where TABLE_CATALOG=? and TABLE_NAME = '{tb["TABLE_NAME"]}';"""
            query_result = self.query(
                db_name=db_name, sql=sql_cols, close_conn=False, parameters=(db_name,)
            )

            columns = []
            # 转换查询结果为dict
            for row in query_result.rows:
                columns.append(dict(zip(query_result.column_list, row)))
            _meta["COLUMNS"] = tuple(columns)
            table_metas.append(_meta)
        return table_metas

    def get_all_columns_by_tb(self, db_name, tb_name, **kwargs):
        """获取所有字段, 返回一个ResultSet"""
        result = self.describe_table(db_name, tb_name)
        column_list = [row[0] for row in result.rows]
        result.rows = column_list
        return result

    def describe_table(self, db_name, tb_name, **kwargs):
        """return ResultSet"""
        sql = r"""select
        c.name ColumnName,
        t.name ColumnType,
        c.length  ColumnLength,
        c.scale   ColumnScale,
        c.isnullable ColumnNull,
            case when i.id is not null then 'Y' else 'N' end TablePk
        from (select name,id,uid from sysobjects where (xtype='U' or xtype='V') ) o 
        inner join syscolumns c on o.id=c.id 
        inner join systypes t on c.xtype=t.xusertype 
        left join sysusers u on u.uid=o.uid
        left join (select name,id,uid,parent_obj from sysobjects where xtype='PK' )  opk on opk.parent_obj=o.id 
        left join (select id,name,indid from sysindexes) ie on ie.id=o.id and ie.name=opk.name
        left join sysindexkeys i on i.id=o.id and i.colid=c.colid and i.indid=ie.indid
        WHERE O.name NOT LIKE 'MS%' AND O.name NOT LIKE 'SY%'
        and O.name=?
        order by o.name,c.colid"""
        result = self.query(db_name=db_name, sql=sql, parameters=(tb_name,))
        return result

    def query_check(self, db_name=None, sql=""):
        # 查询语句的检查、注释去除、切分
        result = {"msg": "", "bad_query": False, "filtered_sql": sql, "has_star": False}
        banned_keywords = [
            "ascii",
            "char",
            "charindex",
            "concat",
            "concat_ws",
            "difference",
            "format",
            "len",
            "nchar",
            "patindex",
            "quotename",
            "replace",
            "replicate",
            "reverse",
            "right",
            "soundex",
            "space",
            "str",
            "string_agg",
            "string_escape",
            "string_split",
            "stuff",
            "substring",
            "trim",
            "unicode",
        ]
        keyword_warning = ""
        star_patter = r"(^|,|\s)\*(\s|\(|$)"
        sql_whitelist = ["select", "sp_helptext"]
        # 根据白名单list拼接pattern语句
        whitelist_pattern = "^" + "|^".join(sql_whitelist)
        # 删除注释语句，进行语法判断，执行第一条有效sql
        try:
            sql = sql.format(sql, strip_comments=True)
            sql = sqlparse.split(sql)[0]
            result["filtered_sql"] = sql.strip()
            sql_lower = sql.lower()
        except IndexError:
            result["bad_query"] = True
            result["msg"] = "没有有效的SQL语句"
            return result
        if re.match(whitelist_pattern, sql_lower) is None:
            result["bad_query"] = True
            result["msg"] = "仅支持{}语法!".format(",".join(sql_whitelist))
            return result
        if re.search(star_patter, sql_lower) is not None:
            keyword_warning += "禁止使用 * 关键词\n"
            result["has_star"] = True
        for keyword in banned_keywords:
            pattern = r"(^|,| |=){}( |\(|$)".format(keyword)
            if re.search(pattern, sql_lower) is not None:
                keyword_warning += "禁止使用 {} 关键词\n".format(keyword)
                result["bad_query"] = True
        if result.get("bad_query") or result.get("has_star"):
            result["msg"] = keyword_warning
        return result

    def filter_sql(self, sql="", limit_num=0):
        sql_lower = sql.lower()
        # 对查询sql增加limit限制
        if re.match(r"^select", sql_lower):
            if sql_lower.find(" top ") == -1:
                if sql_lower.find(" distinct ") > 0:
                    return sql_lower.replace(
                        "distinct", "distinct top {}".format(limit_num)
                    )
                return sql_lower.replace("select", "select top {}".format(limit_num))
        return sql.strip()

    def query(
        self,
        db_name=None,
        sql="",
        limit_num=0,
        close_conn=True,
        parameters: tuple = None,
        **kwargs,
    ):
        """返回 ResultSet"""
        result_set = ResultSet(full_sql=sql)
        try:
            conn = self.get_connection(db_name)
            cursor = conn.cursor()
            # https://github.com/mkleehammer/pyodbc/wiki/Cursor#executesql-parameters
            if parameters:
                cursor.execute(sql, *parameters)
            else:
                cursor.execute(sql)
            if int(limit_num) > 0:
                rows = cursor.fetchmany(int(limit_num))
            else:
                rows = cursor.fetchall()
            fields = cursor.description

            result_set.column_list = [i[0] for i in fields] if fields else []
            result_set.rows = [tuple(x) for x in rows]
            result_set.affected_rows = len(result_set.rows)
        except Exception as e:
            logger.warning(
                f"MsSQL语句执行报错，语句：{sql}，错误信息{traceback.format_exc()}"
            )
            result_set.error = str(e)
        finally:
            if close_conn:
                self.close()
        return result_set

    def query_masking(self, db_name=None, sql="", resultset=None):
        """传入 sql语句, db名, 结果集,
        返回一个脱敏后的结果集"""
        # 仅对select语句脱敏
        if re.match(r"^select", sql, re.I):
            filtered_result = brute_mask(self.instance, resultset)
            filtered_result.is_masked = True
        else:
            filtered_result = resultset
        return filtered_result

    def execute_check(self, db_name=None, sql=""):
        """上线单执行前的检查, 返回Review set"""
        check_result = ReviewSet(full_sql=sql)
        # 切分语句，追加到检测结果中，默认全部检测通过
        split_reg = re.compile("^GO$", re.I | re.M)
        sql = re.split(split_reg, sql, 0)
        sql = filter(None, sql)
        split_sql = [f"""use [{db_name}]"""]
        for i in sql:
            split_sql = split_sql + [i]
        rowid = 1
        for statement in split_sql:
            check_result.rows.append(
                ReviewResult(
                    id=rowid,
                    errlevel=0,
                    stagestatus="Audit completed",
                    errormessage="None",
                    sql=statement,
                    affected_rows=0,
                    execute_time=0,
                )
            )
            rowid += 1
        return check_result

    def execute_workflow(self, workflow):
        sql_content = workflow.sqlworkflowcontent.sql_content.strip()
        parsed = sqlparse.parse(sql_content)[0]
        stmt_type = parsed.get_type()

        if workflow.is_backup and stmt_type in ('DELETE', 'UPDATE', 'INSERT'):
            # 备份数据
            try:
                self._backup(workflow)
            except Exception as e:
                logger.error(f"MsSQL backup failed: {e}\n{traceback.format_exc()}")
                # 备份失败，直接返回错误，不继续执行
                result = ReviewSet(
                    full_sql=workflow.sqlworkflowcontent.sql_content,
                )
                result.rows = [ReviewResult(
                    id=1,
                    errlevel=2,
                    stagestatus='Execute Failed',
                    errormessage=f'Backup failed: {e}',
                    sql=workflow.sqlworkflowcontent.sql_content
                )]
                result.error = f'Backup failed: {e}'
                return result
        return self.execute(
            db_name=workflow.db_name, sql=workflow.sqlworkflowcontent.sql_content
        )

    def _backup(self, workflow):
        # 准备
        db_name = workflow.db_name
        sql_content = workflow.sqlworkflowcontent.sql_content.strip()

        # 解析SQL语句，获取表名和where条件
        table_name, where_clause = self._parse_sql(sql_content)
        if not table_name:
            raise Exception("Failed to parse table name from SQL")

        # 构建备份查询
        backup_sql = f"SELECT * FROM {table_name}"
        if where_clause:
            backup_sql += f" WHERE {where_clause}"

        # 查询数据
        backup_result = self.query(db_name, backup_sql)
        if backup_result.error:
            raise Exception(f"Failed to query backup data: {backup_result.error}")

        # 保存备份
        if backup_result.rows:
            # 将数据序列化为JSON
            backup_data = json.dumps(
                [dict(zip(backup_result.column_list, row)) for row in backup_result.rows],
                default=str,
            )
            # 存储到历史记录表
            SqlBackupHistory.objects.create(
                workflow=workflow,
                table_name=table_name,
                sql_statement=sql_content,
                backup_data=backup_data,
            )

    def _parse_sql(self, sql):
        """简单的SQL解析，用于提取表名和where条件, 只支持简单的UPDATE/DELETE语句"""
        # 移除注释
        sql = re.sub(r"--.*", "", sql)
        sql = re.sub(r"/\*.*\*/", "", sql, flags=re.DOTALL)
        sql = sql.strip()

        parsed = sqlparse.parse(sql)[0]
        stmt_type = parsed.get_type()
        table_name = None
        where_clause = None

        if stmt_type == 'UPDATE':
            # Extract table name
            for t in parsed.tokens:
                if isinstance(t, sqlparse.sql.Identifier):
                    table_name = t.value
                    break
            # Extract where clause
            where_token = next((t for t in parsed.tokens if isinstance(t, sqlparse.sql.Where)), None)
            if where_token:
                where_clause = where_token.value
        elif stmt_type == 'DELETE':
            # Extract table name
            from_seen = False
            for t in parsed.tokens:
                if t.is_keyword and t.normalized == 'FROM':
                    from_seen = True
                    continue
                if from_seen and isinstance(t, sqlparse.sql.Identifier):
                    table_name = t.value
                    break
            # Extract where clause
            where_token = next((t for t in parsed.tokens if isinstance(t, sqlparse.sql.Where)), None)
            if where_token:
                where_clause = where_token.value
        elif stmt_type == 'INSERT':
            # Extract table name
            into_seen = False
            for t in parsed.tokens:
                if into_seen and not t.is_whitespace:
                    table_name = t.value
                    if '(' in table_name:
                        table_name = table_name.split('(')[0].strip()
                    break

                if t.is_keyword and t.normalized == 'INTO':
                    into_seen = True

        return table_name, where_clause

    def get_rollback(self, workflow):
        """
        获取回滚语句
        """
        # NOTE: This method constructs rollback SQL strings manually.
        # It assumes that primary keys are not updated.
        # 获取备份历史
        backup_history = SqlBackupHistory.objects.filter(workflow=workflow)
        if not backup_history:
            return []

        rollback_sql_list = []
        for history in backup_history:
            original_sql = history.sql_statement
            parsed = sqlparse.parse(original_sql)[0]
            stmt_type = parsed.get_type()
            table_name = history.table_name
            try:
                # json.loads may fail
                backup_data = json.loads(history.backup_data)
            except Exception:
                # if backup_data is not valid json, we can't generate rollback sql
                # just skip this history
                logger.warning(f"Failed to parse backup_data for workflow {workflow.id}, history {history.id}. Skipping.")
                continue

            if stmt_type == 'DELETE':
                for row in backup_data:
                    columns = ", ".join(row.keys())
                    values = ", ".join([self._format_sql_value(v) for v in row.values()])
                    rollback_sql = f"INSERT INTO {table_name} ({columns}) VALUES ({values});"
                    rollback_sql_list.append([original_sql, rollback_sql])

            elif stmt_type == 'UPDATE':
                # 获取主键
                primary_key = self._get_primary_key(workflow.db_name, table_name)

                for row in backup_data:
                    if primary_key:
                        # 使用主键构建回滚语句
                        set_clause = ", ".join(
                            [f"[{k}]={self._format_sql_value(v)}" for k, v in row.items() if k != primary_key])
                        where_clause = f"[{primary_key}] = {self._format_sql_value(row.get(primary_key))}"
                        if not set_clause:
                            # Skip if table only has a primary key, nothing to update
                            continue
                    else:
                        # 没有主键，使用所有列构建回滚语句
                        set_clause = ", ".join([f"[{k}]={self._format_sql_value(v)}" for k, v in row.items()])
                        where_clause_parts = []
                        for k, v in row.items():
                            if v is None:
                                where_clause_parts.append(f"[{k}] IS NULL")
                            else:
                                where_clause_parts.append(f"[{k}] = {self._format_sql_value(v)}")
                        where_clause = " AND ".join(where_clause_parts)

                    rollback_sql = f"UPDATE {table_name} SET {set_clause} WHERE {where_clause};"
                    rollback_sql_list.append([original_sql, rollback_sql])
            elif stmt_type == 'INSERT':
                parsed = sqlparse.parse(original_sql)[0]

                # Find values parenthesis
                values_paren = None
                values_keyword_found = False
                for token in parsed.tokens:
                    if token.is_keyword and token.normalized.upper() == 'VALUES':
                        values_keyword_found = True
                        continue
                    if values_keyword_found and isinstance(token, sqlparse.sql.Parenthesis):
                        values_paren = token
                        break

                if not values_paren:
                    continue

                # Extract values from parenthesis
                value_tokens = values_paren.tokens[1:-1]
                values = [t.value for t in value_tokens if not t.is_whitespace and t.value != ',']

                # Find column parenthesis
                column_paren = None
                for token in parsed.tokens:
                    if token.is_keyword and token.normalized.upper() == 'VALUES':
                        break
                    if isinstance(token, sqlparse.sql.Parenthesis):
                        column_paren = token

                columns = []
                if column_paren:
                    col_tokens = column_paren.tokens[1:-1]
                    columns = [t.value.strip('[]') for t in col_tokens if not t.is_whitespace and t.value != ',']
                else:
                    all_cols_rs = self.get_all_columns_by_tb(workflow.db_name, table_name)
                    if not all_cols_rs.error:
                        columns = all_cols_rs.rows

                if len(columns) == len(values):
                    primary_key = self._get_primary_key(workflow.db_name, table_name)
                    if primary_key and primary_key in columns:
                        pk_index = columns.index(primary_key)
                        pk_value = values[pk_index]
                        rollback_sql = f"DELETE FROM {table_name} WHERE [{primary_key}] = {pk_value};"
                        rollback_sql_list.append([original_sql, rollback_sql])
                    else:
                        where_clause_parts = []
                        for i in range(len(columns)):
                            col = columns[i]
                            val = values[i]
                            if val.upper() == 'NULL':
                                where_clause_parts.append(f"[{col}] IS NULL")
                            else:
                                where_clause_parts.append(f"[{col}] = {val}")
                        where_clause = " AND ".join(where_clause_parts)
                        rollback_sql = f"DELETE FROM {table_name} WHERE {where_clause};"
                        rollback_sql_list.append([original_sql, rollback_sql])
        return rollback_sql_list

    def _get_primary_key(self, db_name, tb_name):
        """
        获取表的主键
        """
        sql = f"SELECT KU.column_name FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS TC INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KU ON TC.CONSTRAINT_TYPE = 'PRIMARY KEY' AND TC.CONSTRAINT_NAME = KU.CONSTRAINT_NAME AND KU.table_name = TC.table_name AND KU.table_schema = TC.table_schema WHERE TC.TABLE_SCHEMA = '{db_name}' AND TC.TABLE_NAME = '{tb_name}';"
        result = self.query(db_name=db_name, sql=sql)
        if result.rows:
            return result.rows[0][0]
        return None

    def _format_sql_value(self, value):
        """
        Formats a Python value for use in a SQL query.
        - None is converted to NULL.
        - Strings are quoted and single quotes are escaped.
        - Other types are converted to strings.
        """
        if value is None:
            return "NULL"
        elif isinstance(value, str):
            # Escape single quotes for SQL by replacing them with two single quotes
            return "'" + value.replace("'", "''") + "'"
        else:
            return str(value)

    def execute(self, db_name=None, sql="", close_conn=True, parameters=None):
        """执行sql语句 返回 Review set"""
        execute_result = ReviewSet(full_sql=sql)
        conn = self.get_connection(db_name=db_name)
        cursor = conn.cursor()
        split_reg = re.compile("^GO$", re.I | re.M)
        sql = re.split(split_reg, sql, 0)
        sql = filter(None, sql)
        split_sql = [f"""use [{db_name}]"""]
        for i in sql:
            split_sql = split_sql + [i]
        rowid = 1
        for statement in split_sql:
            try:
                cursor.execute(statement)
            except Exception as e:
                logger.warning(
                    f"Mssql命令执行报错，语句：{sql}， 错误信息：{traceback.format_exc()}"
                )
                execute_result.error = str(e)
                execute_result.rows.append(
                    ReviewResult(
                        id=rowid,
                        errlevel=2,
                        stagestatus="Execute Failed",
                        errormessage=f"异常信息：{e}",
                        sql=statement,
                        affected_rows=0,
                        execute_time=0,
                    )
                )
                break
            else:
                execute_result.rows.append(
                    ReviewResult(
                        id=rowid,
                        errlevel=0,
                        stagestatus="Execute Successfully",
                        errormessage="None",
                        sql=statement,
                        affected_rows=cursor.rowcount,
                        execute_time=0,
                    )
                )
            rowid += 1
        if execute_result.error:
            # 如果失败, 将剩下的部分加入结果集, 并将语句回滚
            for statement in split_sql[rowid:]:
                execute_result.rows.append(
                    ReviewResult(
                        id=rowid,
                        errlevel=2,
                        stagestatus="Execute Failed",
                        errormessage=f"前序语句失败, 未执行",
                        sql=statement,
                        affected_rows=0,
                        execute_time=0,
                    )
                )
                rowid += 1
            cursor.rollback()
            for row in execute_result.rows:
                if row.stagestatus == "Execute Successfully":
                    row.stagestatus += "\nRollback Successfully"
        else:
            cursor.commit()
        if close_conn:
            self.close()
        return execute_result

    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None
