# -*- coding: UTF-8 -*-

import json
import logging
import re
import traceback
import sqlparse

from sql.models import SqlBackupHistory
from .mysql import MysqlEngine
from .models import ReviewResult, ReviewSet

logger = logging.getLogger("default")


class TidbEngine(MysqlEngine):
    name = "TiDB"
    info = "TiDB engine"

    def __init__(self, instance=None):
        super().__init__(instance=instance)

    def execute_workflow(self, workflow):
        """
        TiDB的备份流程
        """
        if not workflow.is_backup:
            return super().execute_workflow(workflow)

        # 备份数据
        try:
            self.backup(workflow)
        except Exception as e:
            logger.error(f"TiDB backup failed: {e}\n{traceback.format_exc()}")
            # 备份失败，直接返回错误，不继续执行
            result = ReviewSet(
                full_sql=workflow.sqlworkflowcontent.sql_content,
                rows=[
                    ReviewResult(
                        id=1,
                        errlevel=2,
                        stagestatus="Execute Failed",
                        errormessage=f"Backup failed: {e}",
                        sql=workflow.sqlworkflowcontent.sql_content,
                    )
                ],
            )
            result.error = f"Backup failed: {e}"
            return result

        # 执行SQL
        # 备份成功后，设置is_backup为False，防止父类再次备份
        workflow.is_backup = False
        return super().execute_workflow(workflow)

    def backup(self, workflow):
        # 准备
        db_name = workflow.db_name
        sql_content = workflow.sqlworkflowcontent.sql_content.strip()

        # 解析SQL语句，获取表名和where条件
        table_name, where_clause = self.parse_sql(sql_content)
        if not table_name:
            raise Exception("Failed to parse table name from SQL")

        # 构建备份查询
        if '.' in table_name:
            backup_sql = f"SELECT * FROM `{table_name}`"
        else:
            backup_sql = f"SELECT * FROM `{db_name}`.`{table_name}`"
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

    def parse_sql(self, sql):
        """简单的SQL解析，用于提取表名和where条件, 只支持简单的UPDATE/DELETE语句"""
        # 移除注释
        sql = re.sub(r"--.*", "", sql)
        sql = re.sub(r"/\*.*\*/", "", sql, flags=re.DOTALL)
        sql = sql.strip()

        # 匹配UPDATE
        update_match = re.match(r"UPDATE\s+`?([^`]+)`?\s+SET.*?(?:\s+WHERE\s+(.*))?$", sql, re.IGNORECASE | re.DOTALL)
        if update_match:
            table_name = update_match.group(1).strip()
            if '.' in table_name:
                table_name = table_name.split('.')[1]
            where_clause = update_match.group(2) or ''
            return table_name.strip('`'), where_clause

        # 匹配DELETE
        delete_match = re.match(r"DELETE\s+FROM\s+`?([^`]+)`?\s*(?:\s+WHERE\s+(.*))?$", sql, re.IGNORECASE | re.DOTALL)
        if delete_match:
            table_name = delete_match.group(1).strip()
            if '.' in table_name:
                table_name = table_name.split('.')[1]
            where_clause = delete_match.group(2) or ''
            return table_name.strip('`'), where_clause

        return None, None

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
                    rollback_sql = f"INSERT INTO `{table_name}` ({columns}) VALUES ({values});"
                    rollback_sql_list.append([original_sql, rollback_sql])

            elif stmt_type == 'UPDATE':
                # 获取主键
                primary_key = self._get_primary_key(workflow.db_name, table_name)

                for row in backup_data:
                    if primary_key:
                        # 使用主键构建回滚语句
                        set_clause = ", ".join(
                            [f"`{k}`={self._format_sql_value(v)}" for k, v in row.items() if k != primary_key])
                        where_clause = f"`{primary_key}` = {self._format_sql_value(row.get(primary_key))}"
                        if not set_clause:
                            # Skip if table only has a primary key, nothing to update
                            continue
                    else:
                        # 没有主键，使用所有列构建回滚语句
                        set_clause = ", ".join([f"`{k}`={self._format_sql_value(v)}" for k, v in row.items()])
                        where_clause_parts = []
                        for k, v in row.items():
                            if v is None:
                                where_clause_parts.append(f"`{k}` IS NULL")
                            else:
                                where_clause_parts.append(f"`{k}` = {self._format_sql_value(v)}")
                        where_clause = " AND ".join(where_clause_parts)

                    rollback_sql = f"UPDATE `{table_name}` SET {set_clause} WHERE {where_clause};"
                    rollback_sql_list.append([original_sql, rollback_sql])
        return rollback_sql_list

    def _get_primary_key(self, db_name, tb_name):
        """
        获取表的主键
        """
        sql = f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA = '{db_name}' AND TABLE_NAME = '{tb_name}' AND CONSTRAINT_NAME = 'PRIMARY';"
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
