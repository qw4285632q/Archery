import unittest
from .mssql import MssqlEngine

class TestMssqlEngine(unittest.TestCase):
    def setUp(self):
        self.engine = MssqlEngine()

    def test_parse_sql_update_with_join(self):
        sql = "UPDATE t1 SET t1.c1 = t2.c2 FROM t1 INNER JOIN t2 ON t1.id = t2.id WHERE t1.id = 1"
        table_name, where_clause = self.engine._parse_sql(sql)
        self.assertEqual(table_name, "t1")
        self.assertEqual(where_clause, "t1.id = 1")

    def test_parse_sql_simple_update(self):
        sql = "UPDATE MyTable SET a = 1 WHERE b = 2"
        table_name, where_clause = self.engine._parse_sql(sql)
        self.assertEqual(table_name, "MyTable")
        self.assertEqual(where_clause, "b = 2")

    def test_parse_sql_simple_delete(self):
        sql = "DELETE FROM MyTable WHERE b = 2"
        table_name, where_clause = self.engine._parse_sql(sql)
        self.assertEqual(table_name, "MyTable")
        self.assertEqual(where_clause, "b = 2")

    def test_parse_sql_delete_with_join(self):
        sql = "DELETE FROM t1 FROM t1 INNER JOIN t2 ON t1.id = t2.id WHERE t1.id = 1"
        table_name, where_clause = self.engine._parse_sql(sql)
        self.assertEqual(table_name, "t1 FROM t1 INNER JOIN t2 ON t1.id = t2.id")
        self.assertEqual(where_clause, "t1.id = 1")
