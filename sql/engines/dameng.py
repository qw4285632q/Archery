# -*- coding: UTF-8 -*-

from sql.engines import EngineBase
from sql.engines.models import ResultSet, ReviewSet


class DamengEngine(EngineBase):
    name = "Dameng"
    info = "Dameng Engine"
    test_query = "SELECT 1 FROM DUAL"  # Or an equivalent basic query for Dameng DB

    def get_connection(self, db_name=None):
        # Actual Dameng DB driver and connection logic will be added later
        return None

    def query_check(self, db_name=None, sql=''):
        # Actual Dameng DB specific query check logic will be added later
        return {'bad_query': False, 'filtered_sql': sql}

    def filter_sql(self, sql='', limit_num=0):
        # Actual Dameng DB specific SQL filtering logic will be added later
        return sql

    def query(self, db_name=None, sql='', limit_num=0, close_conn=True, parameters=None, **kwargs):
        # Actual Dameng DB query execution logic will be added later
        return ResultSet()

    def get_all_databases(self):
        # Actual Dameng DB logic to fetch all databases will be added later
        return ResultSet()

    def get_all_tables(self, db_name, **kwargs):
        # Actual Dameng DB logic to fetch all tables will be added later
        return ResultSet()

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
