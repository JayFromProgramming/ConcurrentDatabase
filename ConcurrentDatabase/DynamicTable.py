import datetime
import sqlite3
import sys
import typing

from typing import List

from .ColumnWrapper import ColumnWrapper
from .DynamicEntry import DynamicEntry

from loguru import logger as logging


class DynamicTable:
    """
    A class that allows you to access a table in a database as if it were a dictionary.
    """

    def __init__(self, table_name, database):
        self.table_name = table_name
        self.database = database  # type: Database  # The database that this table is in
        self.columns = []  # type: list[ColumnWrapper]  # A list of all the columns in the table
        self.entries = []  # type: list[DynamicEntry]  # A list of all the entries that have been loaded
        self.primary_keys = []  # type: list[ColumnWrapper]  # A list of the columns that are primary keys
        self._load_columns()

        self.parent_tables = []  # type: list[DynamicTable]  # A list of all the tables that reference this table
        self.child_tables = []  # type: list[DynamicTable]  # A list of all the tables that this table references

    @property
    def foreign_tables(self):
        """
        Get a list of all the tables that this table references.
        :return: The list of tables.
        """
        return self.child_tables + self.parent_tables

    def _load_columns(self):
        sql = f"PRAGMA table_info({self.table_name})"  # Get the columns of the table
        columns = self.database.get(sql)
        for row in columns:
            column = ColumnWrapper(self, row)
            self.columns.append(column)

    def _validate_columns(self, **kwargs):
        """
        Validate that all columns are valid and that all constraints are met.
        :param kwargs: The columns to validate.
        :return: None
        :raises KeyError: If a column is not found in the table.
        """
        for column in kwargs:
            if column not in self.columns:
                raise KeyError(f"Column [{column}] not found in table [{self.table_name}]")
            actual_column = self.columns[self.columns.index(column)]
            actual_column.validate(kwargs[column])

    def _contains_primary_keys(self, **kwargs):
        """
        Validate that all primary keys are present in the kwargs
        :param kwargs:
        :return:
        """
        for primary_key in self.primary_keys:
            if primary_key.name not in kwargs:
                raise KeyError(f"Primary key [{primary_key.name}] not specified")

    def update_schema(self):
        """
        Update the schema of the table.
        :return: None
        """
        self.columns = []
        self._load_columns()

    def get_entry_by_row(self, row_num: int):
        """
        Get an entry by the row number.
        """
        result = self.database.get(f"SELECT * FROM {self.table_name} LIMIT 1 OFFSET {row_num}")
        if result:
            return DynamicEntry(self, load_tuple=result[0])
        else:
            return None

    def get_row(self, **kwargs) -> typing.Optional[DynamicEntry]:
        """
        Get a row from the table.
        :param kwargs: The filters to apply to the query.
        :return: The row.
        """
        self._validate_columns(**kwargs)
        # self._contains_primary_keys(**kwargs)

        # Check if the DynamicEntry is already loaded
        for entry in self.entries:
            if entry == kwargs:
                return entry

        # Build the query
        sql = f"SELECT * FROM {self.table_name}"
        if len(kwargs) > 0:
            sql += " WHERE "
            for column_name in kwargs:
                column = self.columns[self.columns.index(column_name)]
                sql += self._create_filter(column, kwargs[column_name]) + " AND "
            sql = sql[:-5]
        result = self.database.get(sql)
        if result:
            entry = DynamicEntry(self, load_tuple=result[0])
            self.entries.append(entry)
            return entry
        else:
            return None

    def get_rows(self, **kwargs) -> List[DynamicEntry]:
        """
        Get a set of rows from the table.
        :param kwargs: The filters to apply to the query.
        :return: The row.
        """
        # For each column validate that it is a valid column and that the constraints are met.
        self._validate_columns(**kwargs)

        # Build the query
        sql = f"SELECT * FROM {self.table_name}"
        if len(kwargs) > 0:
            sql += " WHERE "
            for column_name in kwargs:
                column = self.columns[self.columns.index(column_name)]
                sql += self._create_filter(column, kwargs[column]) + " AND "
            sql = sql[:-5]
        result = self.database.get(sql)
        if result:
            entries = [DynamicEntry(self, load_tuple=row) for row in result]
            self.entries.extend(entries)
            return entries
        else:
            return []

    def get_all(self, reverse=False) -> List[DynamicEntry]:
        """
        Get all rows from the table. This is not recommended for large tables.
        :return: The rows.
        """
        db_load = self.database.get(f"SELECT * FROM {self.table_name} ORDER BY rowid {'DESC' if reverse else 'ASC'}")
        if db_load:  # Append any new entries to self.entries and don't overwrite pre-existing entries
            for entry in self.entries:
                if entry not in db_load:
                    db_load.append(entry)
            new_entries = [DynamicEntry(self, load_tuple=row) for row in db_load]
            self.entries.extend(new_entries)
            return self.entries
        else:
            return []

    def get_related_entries(self, entry: DynamicEntry) -> List[DynamicEntry]:
        """
        Get all entries that reference the given entry in this table.
        :param foreign_key: The foreign key that references the entry.
        :param entry: The entry that is referenced.
        :return: The entries from this table that reference the given entry.
        """
        # Determine which foreign keys reference the entry
        source_table = entry.table
        # Find the foreign key from this table to the source table
        link = [link for link in self.database.table_links if link.has_link(self, source_table)]
        if len(link) == 0:
            raise ValueError(f"Table [{self.table_name}] does not reference table [{source_table.table_name}]")
        elif len(link) > 1:
            raise ValueError(f"Table [{self.table_name}] has multiple links to table [{source_table.table_name}]")
        else:
            link = link[0]
        # Get the foreign key from this table to the source table
        local_key, foreign_key = link.get_foreign_key(self)
        # Get the entries that reference the entry
        sql = f"SELECT * FROM {self.table_name} WHERE {local_key.name} = {entry[foreign_key.name]}"
        result = self.database.get(sql)
        if result:
            entries = [DynamicEntry(self, load_tuple=row) for row in result]
            for entry in entries:
                if entry not in self.entries:
                    self.entries.append(entry)
            return entries
        else:
            return []

    def select(self, where: str, limit: int = -1, offset: int = 0, order_by: str = None) -> List[DynamicEntry]:
        """
        Select rows from the table.
        :param where: The where clause of the query.
        :param limit: The limit of the query.
        :param offset: The offset of the query.
        :param order_by: The order by clause of the query.
        :return: The rows.
        :Note this method has no query validation
        """
        result = self.database.get(f"SELECT * FROM {self.table_name} WHERE {where}"
                                   f"{f' ORDER BY {order_by}' if order_by else ''}"
                                   f"{f' LIMIT {limit}' if limit > 0 else ''}"
                                   f"{f' OFFSET {offset}' if offset > 0 else ''}")
        if result:
            # Check if some of the entries are already loaded
            entries = [DynamicEntry(self, load_tuple=row) for row in result]
            for entry in entries:
                if entry not in self.entries:
                    self.entries.append(entry)
            return entries
        else:
            return []

    def custom_query(self, sql: str) -> sqlite3.Cursor:
        """
        Run a custom query on the table.
        :param sql: The query to run.
        :return: The result of the query.
        """
        return self.database.get(sql)

    def add(self, **kwargs) -> DynamicEntry:
        """
        Add a row to the table.
        :param kwargs: The values of the entry
        :return: A DynamicEntry object representing the row.
        """
        # For each column validate that it is a valid column
        self._validate_columns(**kwargs)
        # self._contains_primary_keys(**kwargs)

        # Build the query
        sql = f"INSERT INTO {self.table_name} ("
        for column in kwargs:
            sql += f"{column}, "
        sql = sql[:-2] + ") VALUES ("
        for _ in kwargs:
            sql += "?, "
        sql = sql[:-2] + ")"
        try:
            self.database.run(sql, tuple(kwargs.values()))
        except sqlite3.IntegrityError as e:
            raise ValueError(f"Integrity error: {e}")
        entry = DynamicEntry(self, **kwargs)
        self.entries.append(entry)
        return entry

    def update_or_add(self, **kwargs) -> DynamicEntry:
        """
        Update a row if it exists, otherwise add it.
        :param kwargs: The values of the entry
        :return: A DynamicEntry object representing the row.
        """
        # For each column validate that it is a valid column
        self._validate_columns(**kwargs)
        self._contains_primary_keys(**kwargs)
        # Use get_row only on the primary keys included in the kwargs
        primary_keys = {key: kwargs[key] for key in kwargs if key in self.primary_keys}
        row = self.get_row(**primary_keys)
        if row:
            # print(f"Updating row {row}")
            row.set(**kwargs)
            return row
        else:
            # print("Adding row")
            return self.add(**kwargs)

    def delete(self, **kwargs):
        """
        Delete a row from the table.
        :param kwargs: The filters to apply to the query.
        :return: None
        """
        # For each column validate that it is a valid column and that the constraints are met.
        self._validate_columns(**kwargs)
        self._contains_primary_keys(**kwargs)

        entry = self.get_row(**kwargs)

        if entry:
            self.entries.remove(entry)

        # Build the query
        sql = f"DELETE FROM {self.table_name}"
        if len(kwargs) > 0:
            sql += " WHERE "
            for column_name in kwargs:
                column = self.columns[self.columns.index(column_name)]
                sql += self._create_filter(column, kwargs[column]) + " AND "
            sql = sql[:-5]
        result = self.database.run(sql)
        return result

    def delete_many(self, **kwargs):
        """
        Deletes rows with values matching the kwargs
        :param kwargs:
        :return:
        """
        # For each column validate that it is a valid column and that the constraints are met.
        self._validate_columns(**kwargs)

        if len(kwargs) == 0:
            raise ValueError("Must specify at least one column filter for delete_many")

        entries = self.get_rows(**kwargs)
        for entry in entries:
            self.entries.remove(entry)

        # Build the query
        sql = f"DELETE FROM {self.table_name}"
        if len(kwargs) > 0:
            sql += " WHERE "
            for column_name in kwargs:
                column = self.columns[self.columns.index(column_name)]
                sql += self._create_filter(column, kwargs[column]) + " AND "
            sql = sql[:-5]
        self.database.run(sql)

    def flush(self):
        """
        Flush all dirty DynamicEntries to the database.
        :return:
        """
        queries = []
        for entry in self.entries:
            queries.append(entry.flush_many())
        self.database.batch_transaction(queries)

    def get_column(self, column_name: str) -> ColumnWrapper:
        """
        Get a column by name.
        :param column_name: The name of the column.
        :return: The column.
        """
        return self.columns[self.columns.index(column_name)]

    def _create_filter(self, column, value):
        """
        Create an SQL filter from a kwargs key and value.
        :param key: The column name.
        :param value: The value to filter. value or [lower, upper] for ranges.
        :return:
        """
        if isinstance(value, list):  # Range
            if len(value) != 2:
                raise ValueError(f"Invalid range for column {column.name}")
            return f"{column.name} >= {column.safe_value(value[0])} AND {column.name} <= {column.safe_value(value[1])}"
        elif isinstance(value, tuple):  # Multiple values
            return ''
        else:
            return f"{column.name} = {column.safe_value(value)}"

    def has_dirty_entries(self) -> bool:
        """
        Check if there are any dirty entries.
        :return: True if there are dirty entries, False otherwise.
        """
        for entry in self.entries:
            if entry.is_dirty():
                return True
        return False

    def __getitem__(self, key):
        if key in self.columns:
            return self.database.get(f"SELECT {key} FROM {self.table_name}")
        else:
            raise KeyError(f"Column {key} not found in table {self.table_name}")

    def __setitem__(self, key, value):
        if key in self.columns:
            self.database.run(f"UPDATE {self.table_name} SET {key} = ?", value)
        else:
            raise KeyError(f"Column {key} not found in table {self.table_name}")

    def __delitem__(self, key):
        if key in self.columns:
            self.database.run(f"ALTER TABLE {self.table_name} DROP COLUMN {key}")
            self.columns.remove(key)
        else:
            raise KeyError(f"Column {key} not found in table {self.table_name}")

    def __iter__(self):
        """
        Iterate over the entries in the table.
        """
        # Load all entries
        return self.get_all()

    def __len__(self):
        """
        Get the number of entries in the table.
        """
        sql = f"SELECT COUNT(*) FROM {self.table_name}"
        return self.database.get(sql)[0][0]

    def __contains__(self, key):
        return key in self.columns

    def __repr__(self):
        return f"DynamicTable({self.table_name}, {self.database})"

    def __str__(self):
        return f"DynamicTable({self.table_name}, {self.database})"

    def __del__(self):
        # Check if the database is still open
        if self.has_dirty_entries():
            try:
                self.flush()
            except RuntimeError:
                pass  # Database is closed, flush was unsuccessful

    def __gc(self) -> bool:
        """
        Check for any entries that no longer have any external references and remove them.
        :return: False if there are still references to entries from the table, True if it is safe to GC the table.
        """
        still_referenced = False
        for i in range(len(self.entries)):
            if sys.getrefcount(self.entries[i]) <= 2:
                del self.entries[i]
            else:
                still_referenced = True
        return not still_referenced
