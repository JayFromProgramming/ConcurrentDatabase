from loguru import logger as logging


class ColumnWrapper:

    def __init__(self, table, pragma):
        self.table = table

        # Result of pragma table_info (position, name, type, notnull, default_value, primary_key)
        self.position = pragma[0]  # type: int
        self.name = pragma[1]  # type: str
        self.type = pragma[2].upper()  # type: str
        self.not_null = pragma[3]  # type: int
        self.default_value = pragma[4]  # type: str
        self.primary_key = pragma[5]  # type: int

        # Used for foreign keys
        self.is_foreign_key = False  # type: bool
        self.is_child = False  # type: bool
        self.linked_table = None  # type: DynamicTable or None
        self.linked_column = None  # type: ColumnWrapper or None

        if self.primary_key:
            self.table.primary_keys.append(self)

    def attach_linked_table(self, linked_table, linked_column, child: bool = False):
        self.is_foreign_key = True
        self.is_child = child
        self.linked_table = linked_table
        self.linked_column = linked_column

    def validate(self, value):

        if (self.not_null and value is None) and self.default_value == "":
            raise ValueError(f"Column {self.name} cannot be null")
        elif value is None:
            return

        if isinstance(value, list):  # If the value is a range of values then validate each value in the range
            for item in value:
                self.validate(item)
            return
        # Validate the duck type of the column is correct (aka if it is a string of an integer its still an integer)
        if self.type == "INTEGER" or self.type == "INT":
            try:
                int(value)
            except ValueError:
                raise ValueError(f"Column {self.name} must of duck type {self.type}")
        elif self.type == "REAL":
            try:
                float(value)
            except ValueError:
                raise ValueError(f"Column {self.name} must of duck type {self.type}")
        elif self.type == "TEXT" or self.type == "STRING":
            if not isinstance(value, str) and not isinstance(value, int) and not isinstance(value, float):
                raise ValueError(f"Column {self.name} must of duck type {self.type} not {type(value)}")
        elif self.type == "BLOB":
            if not isinstance(value, bytes):
                raise ValueError(f"Column {self.name} must of exact type {self.type}")
        elif self.type == "BOOLEAN":
            if not isinstance(value, bool):
                raise ValueError(f"Column {self.name} must of exact type {self.type}")
        else:
            logging.warning(f"Unknown column type {self.type}")

    def __str__(self):
        return f"[{self.position}]{'-PRIMARY KEY' if self.primary_key else ''}-{self.name}-({self.type})" \
               f"{'-NOT NULL' if self.not_null else ''}" \
               f"{'-DEFAULT ' + self.default_value if self.default_value else ''}"

    def __repr__(self):
        return self.__str__()

    def __eq__(self, other):
        if isinstance(other, ColumnWrapper):
            return self.name == other.name
        elif isinstance(other, str):
            return self.name == other
        elif isinstance(other, int):
            return self.position == other
        else:
            return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return hash(self.name)

    def __contains__(self, item):
        return item == self.name

    def safe_value(self, value):
        """
        Returns a value that is safe to be inserted into a SQL statement
        :param column: The column that the value is for
        :param value: The value to be inserted
        :return:
        """
        if value is None:
            return "NULL"
        elif self.type == "TEXT" or self.type == "STRING":
            return f"'{value}'"
        elif self.type == "INTEGER" or self.type == "INT":
            return str(value)
        elif self.type == "BOOLEAN":
            return str(value)
        elif self.type == "REAL":
            return str(value)
        elif self.type == "BLOB":
            return str(value)
        else:
            logging.warning(f"Unknown column type {self.type}, assuming TEXT")
            return f"'{value}'"
