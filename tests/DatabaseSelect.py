import unittest

from ConcurrentDatabase.Database import Database


class DatabaseTests(unittest.TestCase):

    def setUp(self):
        self.database = Database(":memory:")
        self.table = self.database.create_table("test_table",
                                                {"id": "INTEGER", "random": "INTEGER", "random2": "INTEGER",
                                                 "random3": "INTEGER"})

    def tearDown(self):
        self.database.close()

    def load_values(self):
        for i in range(100):
            self.table.add(id=i, random=i, random2=i, random3=i)

    def test_select(self):
        self.load_values()
        rows = self.table.select("id > 50")
        self.assertEqual(len(rows), 49)
        self.assertEqual(rows[0]['id'], 51)
        self.assertEqual(rows[-1]['id'], 99)

    def test_select_with_limit(self):
        self.load_values()
        rows = self.table.select("id > 50", limit=10)
        self.assertEqual(len(rows), 10)
        self.assertEqual(rows[0]['id'], 51)
        self.assertEqual(rows[-1]['id'], 60)

    def test_select_with_limit_and_offset(self):
        self.load_values()
        rows = self.table.select("id > 50", limit=10, offset=10)
        self.assertEqual(len(rows), 10)
        self.assertEqual(rows[0]['id'], 61)
        self.assertEqual(rows[-1]['id'], 70)

    def test_select_with_order_by(self):
        self.load_values()
        rows = self.table.select("id > 50", order_by="id DESC")
        self.assertEqual(len(rows), 49)
        self.assertEqual(rows[0]['id'], 99)
        self.assertEqual(rows[-1]['id'], 51)

    def test_select_with_order_by_and_limit(self):
        self.load_values()
        rows = self.table.select("id > 50", order_by="id DESC", limit=10)
        self.assertEqual(len(rows), 10)
        self.assertEqual(rows[0]['id'], 99)
        self.assertEqual(rows[-1]['id'], 90)

    def test_select_with_order_by_and_limit_and_offset(self):
        self.load_values()
        rows = self.table.select("id > 50", order_by="id DESC", limit=10, offset=10)
        self.assertEqual(len(rows), 10)
        self.assertEqual(rows[0]['id'], 89)
        self.assertEqual(rows[-1]['id'], 80)

    def test_select_with_multiple_where(self):
        self.load_values()
        rows = self.table.select("id > 50 AND random < 60")
        self.assertEqual(len(rows), 9)
        self.assertEqual(rows[0]['id'], 51)
        self.assertEqual(rows[-1]['id'], 59)
