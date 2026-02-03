---
description: Repository Information Overview
alwaysApply: true
---

# Teleonomy: DataFrame-SQL Schema Alignment Library

## Summary

Teleonomy is a Python utility library for unified bidirectional DataFrame ↔ SQL schema alignment, orchestration, and CRUD operations. It provides comprehensive tools for schema introspection, data profiling, type correction, and database operations across multiple database dialects (PostgreSQL, MySQL, SQLite, Oracle, MSSQL).

## Structure

- **`crud/`** - CRUD operations module
  - SQL generation for INSERT, UPSERT, UPDATE operations
  - Test harness for validating database operations
  - CSV-based evolution pipeline
  - Logging utilities for tracing and debugging

- **`schema_introspect/`** - Schema introspection and alignment module
  - Schema manager for inspecting database tables and constraints
  - Schema forge for logical type correction and validation
  - Schema corrector for dialect-specific alignment
  - DataFrame-to-schema converters and aligners

- **`preops/`** - Data preprocessing module
  - Data profiler for column analysis (nulls, uniqueness, types)
  - Data cleaner for value normalization
  - Type caster for pandas DataFrames
  - DDL creation from DataFrames

## Language & Runtime

**Language**: Python  
**Version**: Python 3.10+ (based on union type hints `|` operator, dataclass features)  
**Package Manager**: pip (no `pyproject.toml` or `setup.py` found)  
**Build System**: None (library designed for direct import)

## Dependencies

**Core Dependencies**:
- **pandas** - Data manipulation and DataFrame operations
- **sqlalchemy** - SQL toolkit and ORM with multi-dialect support (PostgreSQL, MySQL, SQLite, Oracle, MSSQL)
- **numpy** - Numerical computations (optional, used in preprocessing)

**No lock files found** (`requirements.txt`, `poetry.lock`, `Pipfile` absent)

## Key Modules & Functionality

### CRUD Module (`crud/`)
- Lazy SQL generation for INSERT, UPSERT, UPDATE operations
- Multi-dialect support with database-specific syntax handling
- Test harness (`EnhancedCrudTestHarness`) for full-cycle testing
- Data normalization and NULL-safe mutation
- SQL statement tracing and debugging

### Schema Introspect Module (`schema_introspect/`)
- **SchemaManager**: Inspects tables, columns, constraints, PKs, FKs, indexes
- **DataCorrector** (Schema Forge): Corrects logical types (INTEGER, DECIMAL, STRING, BOOL, DATETIME, JSON, BINARY)
- **SchemaAligner**: Aligns DataFrames to target SQL schema with dialect-specific handling
- **Unification Orchestrator**: Coordinates DataFrame → SQL workflow with error handling and reporting

### Preprocessing Module (`preops/`)
- **Data Profiler**: Analyzes column statistics (cardinality, nulls, types, datetime detection)
- **Data Cleaner**: Normalizes and sanitizes data values
- **Type Caster**: Converts DataFrame columns to appropriate types
- **DDL Creator**: Generates CREATE TABLE statements from DataFrames

## Build & Installation

No formal build/installation process. To use as a library:

```bash
# Install required dependencies
pip install pandas sqlalchemy numpy

# Include modules in your project or add to Python path
python -c "import sys; sys.path.insert(0, '/path/to/Teleonomy')"
```

## Testing

**Test Framework**: unittest/pytest compatible  
**Test File**: `crud/evo_harness_test.py`  
**Test Features**:
- Comprehensive test data generation with smart mutation
- Full-cycle testing (INSERT → UPSERT → UPDATE)
- Validation with database queries
- Test result reporting and difference tracking

**Run Tests**:
```bash
python -m pytest crud/evo_harness_test.py -v
```

## Database Support

Multi-dialect support with dialect-specific optimizations:
- **PostgreSQL** - ALTER COLUMN TYPE syntax
- **MySQL** - MODIFY COLUMN syntax
- **Oracle** - Enhanced data type handling with IDENTITY columns
- **MSSQL** - ALTER COLUMN syntax with identity support
- **SQLite** - Limited ALTER TABLE support

## Configuration

No external configuration files required. Configuration is provided via:
- Python dataclass instances (`CorrectionConfig`, `AlignmentConfig`)
- Function parameters with sensible defaults
- Environment-specific connection strings for SQLAlchemy engines

## Entry Points & Main Classes

- **`unify_dataframe_to_sql()`** - Primary orchestration function in `schema_introspect/schema_unify.py`
- **`SchemaManager`** - Schema introspection interface
- **`DataCorrector`** - Type correction engine  
- **`SchemaAligner`** - DataFrame-to-SQL alignment
- **`EnhancedCrudTestHarness`** - Test data generation and validation

## License

MIT License (2026)
