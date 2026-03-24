"""Catalog-layer skeleton for the exercise.

The real DuckDB path is roughly:

`AttachedDatabase -> Catalog -> Schema -> DuckTableEntry -> DataTable`

This module creates the same educational shape. Methods intentionally raise
`NotImplementedError` so the candidate can implement them incrementally.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class ColumnDefinition:
    """Logical definition for one table column.

    Goal for the candidate:
    - validate column names
    - preserve the declared Python type
    - carry nullability into the storage layer
    """

    name: str
    python_type: type
    nullable: bool = True


@dataclass(frozen=True)
class TableDefinition:
    """Logical table schema.

    Goal for the candidate:
    - keep column order stable
    - expose quick lookup by name when helpful
    - act as the contract for row validation during appends
    """

    name: str
    columns: list[ColumnDefinition] = field(default_factory=list)


@dataclass
class DuckTableEntry:
    """Catalog entry for a table.

    Goal for the candidate:
    - link logical metadata to the physical `DataTable`
    - provide a single object that higher layers can register in a `Schema`
    """

    definition: TableDefinition
    data_table: Any


class Schema:
    """Namespace that owns table entries.

    Goal for the candidate:
    - create and store `DuckTableEntry` objects
    - reject duplicate names
    - return existing entries through `get_table()`
    """

    def __init__(self, name: str) -> None:
        self.name = name
        self.tables: dict[str, DuckTableEntry] = {}

    def create_table(self, definition: TableDefinition, data_table: Any) -> DuckTableEntry | ValueError:
        """Create and register a `DuckTableEntry` inside this schema."""
        if not definition.name in self.tables:
            table_entry = DuckTableEntry(definition, data_table)
            self.tables[definition.name] = table_entry
            return table_entry
        else:
            raise ValueError(f"Table already exist: {definition.name}")

    def get_table(self, table_name: str) -> DuckTableEntry | None:
        """Return a table entry by name."""
        return self.tables.get(table_name)
        


class Catalog:
    """Collection of schemas for one attached database.

    Goal for the candidate:
    - create schemas
    - look them up by name
    - keep a simple API that mirrors a database catalog
    """

    def __init__(self) -> None:
        self.schemas: dict[str, Schema] = {}

    def create_schema(self, schema_name: str) -> Schema:
        """Create a schema and return it."""
        if schema_name not in self.schemas:
            schema = Schema(schema_name)
            self.schemas[schema_name] = schema
            return schema
        return self.schemas[schema_name]
        

    def get_schema(self, schema_name: str) -> Schema | None:
        """Return a previously created schema."""
        return self.schemas.get(schema_name)
        


class AttachedDatabase:
    """Top-level object that owns a catalog.

    Goal for the candidate:
    - expose the owned catalog
    - behave like one attached database file in DuckDB terminology
    """

    def __init__(self, name: str) -> None:
        self.name = name
        self.catalog = Catalog()

    def get_catalog(self) -> Catalog:
        """Return the owned catalog."""
        return self.catalog
