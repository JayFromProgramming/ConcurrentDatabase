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

    def test_delete(self):
        self.load_values()
        entry = self.table.select("id = 50")[0]
        # Change value
        entry['random'] = 100
        self.assertEqual(self.table.select("id = 50")[0]['random'], 50)
        del entry
        # Check if the value was flushed to the database when the entry was deleted
        entry = self.table.select("id = 50")[0]
        self.assertEqual(entry['random'], 100)
