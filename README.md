# lotad

A Python library for tracking and analyzing data drift between DuckDB databases. 
Lotad helps you identify schema changes, data differences, and structural modifications between database versions.

## Features

- Compare table schemas and data between two DuckDB databases
- Track missing tables, columns, and type mismatches 
- Analyze row-level data differences using consistent hashing
- Generate detailed comparison reports
- Configurable tracking with support for excluding specific tables and columns

## Installation

```bash
pip install lotad
```

## Quick Start

```python
from lotad import DatabaseComparator

# Initialize comparator with database paths
comparator = DatabaseComparator('path/to/db1.db', 'path/to/db2.db')

# Run full comparison
results = comparator.compare_all()

# Generate comparison report
comparator.generate_comparison_report(results, 'comparison_report.txt')
```

## Basic Usage

Compare specific tables while ignoring others:

```python
# Compare only specified tables
results = comparator.compare_all(tables=['users', 'orders'])

# Exclude specific tables from comparison
results = comparator.compare_all(ignore_tables=['logs', 'temp_data'])
```

Access detailed comparison results:

```python
# Get schema differences
schema_diff = comparator.compare_table_schemas('users')
print(f"Missing columns in DB2: {schema_diff['missing_in_db2']}")
print(f"Type mismatches: {schema_diff['type_mismatches']}")

# Compare table data
data_diff = comparator.compare_table_data('orders')
print(f"Row count difference: {data_diff['row_count_diff']}")

## License

This project is licensed under the MIT License.

## Why Lotad?

Named after the water lily Pokémon, Lotad helps you stay afloat in a sea of database changes by tracking data drift between your DuckDB instances.
```
