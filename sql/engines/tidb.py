# -*- coding: UTF-8 -*-

import json
import logging
import re
import traceback

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
            table_name = update_match.group(1)
            where_clause = update_match.group(2) or ''
            return table_name, where_clause

        # 匹配DELETE
        delete_match = re.match(r"DELETE\s+FROM\s+`?([^`]+)`?\s*(?:\s+WHERE\s+(.*))?$", sql, re.IGNORECASE | re.DOTALL)
        if delete_match:
            table_name = delete_match.group(1)
            where_clause = delete_match.group(2) or ''
            return table_name, where_clause

        return None, None
