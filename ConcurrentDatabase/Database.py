import sqlite3
import sys
import threading
import time

from loguru import logger as logging
from typing import List

from .DynamicEntry import DynamicEntry
from .DynamicTable import DynamicTable


class CustomLock:

    def __init__(self):
        self.lock = threading.Lock()
        self.lock_count = 0
        self.queued_lock_count = 0

    def acquire(self, blocking=True, timeout=-1):
        self.lock_count += 1
        self.queued_lock_count += 1
        acquired = self.lock.acquire(blocking, timeout)
        if acquired:
            return True
        else:
            self.queued_lock_count -= 1
            return False

    def release(self):
        self.queued_lock_count -= 1
        self.lock.release()

    def locked(self):
        return self.lock.locked()


class Database(sqlite3.Connection):

    def __init__(self, *args, no_gc=False, **kwargs):
        super().__init__(*args, check_same_thread=False, **kwargs)
        self.open = True
        self.lock = CustomLock()
        self.tables = {}
        self.database_name = args[0]
        self.create_table("table_versions", {"table_name": "TEXT", "version": "INTEGER"}, ["table_name"])
        self.table_version_table = self.get_table("table_versions")

        # sqlite3.enable_callback_tracebacks(True)
        # super().set_trace_callback(logging.debug)

        # Start the database garbage collector
        if not no_gc:
            self.gc_thread = threading.Thread(target=self.__gc_loop, daemon=True)
            self.gc_thread.start()

    def create_table(self, table_name: str, columns: dict, primary_keys: List[str] = None) -> DynamicTable:
        """
        Create a table in the database.
        :param table_name: The name of the table to create.
        :param columns: A dictionary of the columns to create in the table.
        :param primary_keys: A list of the primary keys in the table.
        """
        if not self.open:
            raise RuntimeError("Database is closed")
        if table_name != "table_versions":
            sql = f"CREATE TABLE IF NOT EXISTS {table_name} ("
            for column in columns:
                sql += f"{column} {columns[column]}, "
            if primary_keys:
                sql += f"PRIMARY KEY ({', '.join(primary_keys)}), "
            sql = sql[:-2] + ")"
            self.run(sql)
            # Add the table to the tables dictionary
            self.tables[table_name] = DynamicTable(table_name, self)
            # Add the table to the table_versions table (unless this is the table_versions table)
            if not self.table_version_table.get_row(table_name=table_name):
                self.table_version_table.update_or_add(table_name=table_name, version=0)
        else:
            self.run(f"CREATE TABLE IF NOT EXISTS table_versions (table_name TEXT PRIMARY KEY, version INTEGER)")
            self.tables[table_name] = DynamicTable(table_name, self)
        return self.tables[table_name]

    def get_table(self, table_name: str) -> DynamicTable:
        """
        Get a table from the database.
        :param table_name: The name of the table to get.
        :return: A DynamicTable object.
        """
        if not self.open:
            raise RuntimeError("Database is closed")
        if table_name in self.tables:
            return self.tables[table_name]
        else:
            # Load the table from the database
            result = self.run(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'").fetchall()
            if result:
                self.tables[table_name] = DynamicTable(table_name, self)
                return self.tables[table_name]
            else:
                raise KeyError(f"Table {table_name} not found in database {self.database_name}")

    def update_table(self, table_name: str, version: int,
                     update_query: List[str] = None) -> None:
        if not self.open:
            raise RuntimeError("Database is closed")
        """
        Update a table in the database.
        :param table_name: The name of the table to update.
        :param version: The version of the table to update.
        :param update_query: A custom update query if table elements need to be updated in a specific way.
        """
        # Check if the table is already up to date
        if self.table_version_table.get_row(table_name=table_name)["version"] == version:
            return
        elif self.table_version_table.get_row(table_name=table_name)["version"] > version:
            return
        # Check if the revision increment is only 1 more than the current version
        elif self.table_version_table.get_row(table_name=table_name)["version"] + 1 != version:
            raise ValueError(f"Table {table_name} version {version} is not 1 more than the current version "
                             f"{self.table_version_table.get_row(table_name=table_name)['version']}")
        # Check if the table exists
        if table_name not in self.tables:
            raise KeyError(f"Table {table_name} not found in database {self.database_name}")

        # Update the table
        if update_query:
            for query in update_query:
                self.run(query)

            # Update the table version
            self.table_version_table.update_or_add(table_name=table_name, version=version)

            # Reload the table object
            self.tables[table_name].update_schema()
        else:
            raise NotImplementedError("Updating tables with columns is not yet implemented")

    def drop_table(self, table_name: str):
        """
        Drop a table from the database.
        :param table_name: The name of the table to drop.
        """
        if not self.open:
            raise RuntimeError("Database is closed")
        # Check if the table exists
        if table_name not in self.tables:
            raise KeyError(f"Table {table_name} not found in database {self.database_name}")
        self.run(f"DROP TABLE {table_name}")
        # Remove the table from the table_versions table
        self.table_version_table.delete(table_name=table_name)
        # del self.tables[table_name]
        self.tables.pop(table_name)

    def run(self, sql, *args, **kwargs) -> sqlite3.Cursor:
        """
        Run a query on the database with thread safety.
        :param sql: The SQL query to run.
        :param args: The arguments to pass to the query.
        :param kwargs: The keyword arguments to pass to the query.
        :return: A cursor object, use cursor.fetchall() to get the results. (The cursor is not thread safe)
        """
        if not self.open:
            raise RuntimeError("Database is closed")
        self.lock.acquire(timeout=5)
        cursor = super().cursor()
        try:
            cursor.execute(sql, *args)
        except sqlite3.OperationalError as e:
            # If the error is a syntax error, print the query
            logging.error(f"Database Error: {e}")
            if "syntax error" in str(e):
                logging.error(f"Query: {sql}")
        finally:
            if kwargs.get("commit", True):
                try:
                    super().commit()
                except sqlite3.OperationalError as e:
                    logging.error(f"Database Error: Commit failed {e}")
            self.lock.release()
        return cursor

    def batch_transaction(self, transactions: list, *args, **kwargs) -> sqlite3.Cursor:
        """
        Run a batch of queries on the database with thread safety.
        :param sql: The SQL queries to run.
        :param args: The arguments to pass to the query.
        :param kwargs: The keyword arguments to pass to the query.
        :return: A cursor object, use cursor.fetchall() to get the results. (The cursor is not thread safe)
        """
        if not self.open:
            raise RuntimeError("Database is not open")
        self.lock.acquire(timeout=5)
        cursor = super().cursor()
        try:
            sql = ";\n".join(filter(None, transactions))
            cursor.executescript(sql)
        except sqlite3.OperationalError as e:
            logging.error(f"Database Error: {e}")
        finally:
            if kwargs.get("commit", True):
                try:
                    super().commit()
                except sqlite3.OperationalError as e:
                    logging.error(f"Database Error: Commit failed {e}")
            self.lock.release()
        return cursor

    def run_many(self, sql, *args, **kwargs) -> sqlite3.Cursor:
        """
        Run a query on the database with thread safety.
        :param sql: The SQL query to run.
        :param args: The arguments to pass to the query.
        :param kwargs: The keyword arguments to pass to the query.
        """
        if not self.open:
            raise RuntimeError("Database is not open")
        self.lock.acquire()
        cursor = super().cursor()
        cursor.executemany(sql, *args)
        if kwargs.get("commit", True):
            try:
                super().commit()
            except sqlite3.OperationalError as e:
                logging.error(f"Database Error: Commit failed {e}")
        self.lock.release()
        return cursor

    def close(self):
        """
        Close the connection to the database.
        Will flush all cached data to the database.
        """
        for table in self.tables.values():
            del table
        self.lock.acquire()
        super().close()
        self.open = False
        self.lock.release()

    def __del__(self):
        if self.open:
            self.close()

    def get(self, sql, *args) -> List[dict]:
        cursor = self.run(sql, *args)
        result = cursor.fetchall()
        cursor.close()
        return result

    def __gc(self):
        for table_name in list(self.tables.keys()):
            if sys.getrefcount(self.tables[table_name]) <= 2:
                if self.tables[table_name].__gc_loop():
                    self.tables.pop(table_name)

    def __gc_loop(self):
        """
        Check the total number of references to each table object and delete any that are not being used.
        :return:
        """
        while self.open:
            self.__gc()
            time.sleep(60)
