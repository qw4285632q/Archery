import unittest
from .dameng import DamengEngine

class TestDamengEngine(unittest.TestCase):
    def setUp(self):
        self.engine = DamengEngine()

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
