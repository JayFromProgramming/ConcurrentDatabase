[![Build Tests](https://github.com/JayFromProgramming/ConcurrentDatabase/actions/workflows/python-package.yml/badge.svg)](https://github.com/JayFromProgramming/ConcurrentDatabase/actions/workflows/python-package.yml)
[![Upload Python Package](https://github.com/JayFromProgramming/ConcurrentDatabase/actions/workflows/python-publish.yml/badge.svg)](https://github.com/JayFromProgramming/ConcurrentDatabase/actions/workflows/python-publish.yml)

# ConcurrentDatabase
A simple sql wrapper for making a database be object oriented

## Installation
```bash
pip install ConcurrentDatabase
```

## Usage
```python
from ConcurrentDatabase import Database

db = Database("sqlite:///test.db")
```