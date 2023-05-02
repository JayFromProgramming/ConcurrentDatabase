[![Build Checks](https://github.com/JayFromProgramming/ConcurrentDatabase/actions/workflows/python-package.yml/badge.svg)](https://github.com/JayFromProgramming/ConcurrentDatabase/actions/workflows/python-package.yml)
[![Upload Python Package](https://github.com/JayFromProgramming/ConcurrentDatabase/actions/workflows/python-publish.yml/badge.svg)](https://github.com/JayFromProgramming/ConcurrentDatabase/actions/workflows/python-publish.yml)

# ConcurrentDatabase
A simple sql wrapper for making a database be object oriented

## Installation
```bash
pip install ConcurrentDatabase
```

## Database Initialization
```python
from ConcurrentDatabase.Database import Database

db = Database("test.db")

table = db.create_table("example_table", {
    "id": "INTEGER PRIMARY KEY",
    "name": "TEXT",
    "location": "TEXT"
}, primary_keys=["id"])

```

## Inserting Data
```python

table = db.get_table("example_table") 

table.add(name="Jay", location="USA")
table.add(name="John", location="USA")
```

## Updating Data
```python
table = db.get_table("example_table")

row = table.get_row(name="Jay")
row["name"] = "JayFromProgramming"  # Changes are saved in memory until you call row.flush()
row.flush()
# or
row.set(name="JayFromProgramming")  # Flushes immediately
```

## Deleting Data
```python
table = db.get_table("example_table")

row = table.get_row(name="Jay")
row.delete()
# or
table.delete(name="Jay")
```