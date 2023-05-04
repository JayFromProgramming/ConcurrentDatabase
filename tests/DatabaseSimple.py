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

    def test_create_table(self):
        self.assertEqual(self.table, self.database.get_table("test_table"))

    def test_insertion(self):
        row = self.table.get_row(id=1)
        if not row:
            row = self.table.add(id=1, random=4, random2=5, random3=6)

        self.assertEqual(self.table.get_row(id=1), row)

    def test_entry_cache(self):
        row = self.table.add(id=1, random=4, random2=5, random3=6)
        row2 = self.table.get_row(id=1)
        self.assertEqual(id(row), id(row2))

    def test_alter_table(self):
        table = self.database.create_table("test_table", {"id": "INTEGER", "random": "INTEGER", "random2": "INTEGER",
                                                          "random3": "INTEGER"})
        row = table.get_row(id=1)
        if not row:
            row = table.add(id=1, random=4, random2=5, random3=6)

        self.assertEqual("random4" in table.columns, False)

        self.database.update_table("test_table", 1, ["ALTER TABLE test_table ADD COLUMN random4 INTEGER"])

        self.assertEqual(table.get_row(id=1), row)
        self.assertEqual("random4" in table.columns, True)

    def test_drop_table(self):
        table = self.database.create_table("test_table", {"id": "INTEGER", "random": "INTEGER", "random2": "INTEGER",
                                                          "random3": "INTEGER"})
        self.assertEqual(self.database.get_table("test_table"), table)
        self.assertEqual(self.database.table_version_table.get_row(table_name="test_table")['version'], 0)

        self.database.drop_table("test_table")
        try:
            self.database.get_table("test_table")
            self.fail("Table not dropped")
        except KeyError:
            # Validate that the table was removed from the table_version_table
            pass
        row = self.database.table_version_table.get_row(table_name="test_table")
        self.assertEqual(row, None)

    def test_cached_set(self):
        table = self.database.create_table("test_table", {"id": "INTEGER", "random": "INTEGER", "random2": "INTEGER",
                                                          "random3": "INTEGER"})
        row = table.get_row(id=1)
        if not row:
            row = table.add(id=1, random=4, random2=5, random3=6)

        row['random3'] = 4
        result = self.database.get("SELECT * FROM test_table WHERE id=1")
        self.assertEqual(result[0][3], 6)  # Assert that the value was not changed in the database yet
        row.flush()
        result = self.database.get("SELECT * FROM test_table WHERE id=1")
        self.assertEqual(result[0][3], 4)  # Assert that the value was changed in the database

    def test_cached_set2(self):
        table = self.database.create_table("test_table", {"id": "INTEGER", "random": "INTEGER", "random2": "INTEGER",
                                                          "random3": "INTEGER"})
        row = table.get_row(id=1)
        if not row:
            row = table.add(id=1, random=4, random2=5, random3=6)

        row['random3'] = 4
        row['random2'] = 3
        row['random'] = 2
        result = self.database.get("SELECT * FROM test_table WHERE id=1")
        self.assertEqual(result[0][3], 6)  # Assert that the value was not changed in the database yet
        self.assertEqual(result[0][2], 5)  # Assert that the value was not changed in the database yet
        self.assertEqual(result[0][1], 4)  # Assert that the value was not changed in the database yet
        row.flush()
        result = self.database.get("SELECT * FROM test_table WHERE id=1")
        self.assertEqual(result[0][3], 4)  # Assert that the value was changed in the database
        self.assertEqual(result[0][2], 3)  # Assert that the value was changed in the database
        self.assertEqual(result[0][1], 2)  # Assert that the value was changed in the database

    def test_cached_set3(self):
        table = self.database.get_table("test_table")
        for i in range(1000):
            table.add(id=i, random=i + 1, random2=i + 1, random3=i + 1)

        # table.flush()

        for i in range(1000):
            row = table.get_row(id=i)
            row['random3'] = i
            row['random2'] = i
            row['random'] = i

        table.flush()

        for i in range(1000):
            result = self.database.get("SELECT * FROM test_table WHERE id={}".format(i))
            self.assertEqual(result[0][3], i)  # Assert that the value was not changed in the database yet

    def test_get1(self):
        self.database.execute("INSERT INTO test_table VALUES (1, 2, 3, 4)")
        self.database.execute("INSERT INTO test_table VALUES (2, 3, 4, 5)")
        self.database.execute("INSERT INTO test_table VALUES (3, 4, 5, 6)")

        table = self.database.get_table("test_table")
        self.assertEqual(table.get_row(id=1)[1], 2)
        self.assertEqual(table.get_row(id=2)["random"], 3)
        self.assertEqual(table.get_row(id=3)["random2"], 5)

        try:
            table.get_row(id=1)[5]
            self.fail("Failed to raise IndexError")
        except IndexError:
            pass

        try:
            table.get_row(id=1)["random5"]
            self.fail("Failed to raise KeyError")
        except KeyError:
            pass


if __name__ == '__main__':
    unittest.main()
