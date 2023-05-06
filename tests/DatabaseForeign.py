import unittest

from ConcurrentDatabase.Database import Database, CreateTableLink


class DatabaseTests(unittest.TestCase):

    def setUp(self):
        self.database = Database(":memory:", no_gc=True)
        self.users = self.database.create_table(
            "users", {"id": "INTEGER PRIMARY KEY AUTOINCREMENT", "name": "TEXT", "age": "INTEGER"})

        self.classes = self.database.create_table(
            "classes", {"class_id": "INTEGER PRIMARY KEY AUTOINCREMENT", "name": "TEXT", "teacher_id": "INTEGER"},
            linked_tables=[CreateTableLink(target_table="users", target_key="id", source_key="teacher_id",
                                           on_delete="CASCADE")])

        self.participants = self.database.create_table(
            "participants", {"user_id": "INTEGER", "class_id": "INTEGER"},
            primary_keys=["user_id", "class_id"],
            linked_tables=[CreateTableLink(target_table="users", target_key="id", source_key="user_id",
                                           on_delete="CASCADE", on_update="CASCADE"),
                           CreateTableLink(target_table="classes", target_key="class_id", source_key="class_id",
                                           on_delete="CASCADE", on_update="CASCADE")])

    def tearDown(self):
        self.database.close()
        import os
        # if os.path.exists("unit_test.db"):
        #     os.remove("unit_test.db")

    def load_values(self):
        for i in range(100):
            self.users.add(id=i, name="user{}".format(i), age=i)

        for i in range(10):
            self.classes.add(class_id=i, name="class{}".format(i), teacher_id=i)

        for i in range(100):
            self.participants.add(user_id=i, class_id=i % 10)

    def test_dynamic_relations(self):
        self.load_values()
        print(self.database.table_links)
        class1 = self.classes.get_row(class_id=1)
        participants = class1.get("participants")
        self.assertEqual(len(participants), 10)
        user1 = self.users.get_row(id=1)
        classes = user1.get("classes")  # This will only return the classes where the user is the teacher
        self.assertEqual(len(classes), 1)

    def test_cascade(self):
        self.load_values()
        class1 = self.classes.get_row(class_id=1)
        self.assertEqual(len(class1.get("participants")), 10)
        del class1
        self.users.delete(id=1)
        self.assertEqual(self.users.get_row(id=1), None)
        class1 = self.classes.get_row(class_id=1)
        self.assertEqual(len(class1.get("participants")), 0)

    # def test_update(self):

