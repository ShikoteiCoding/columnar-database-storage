"""High-level database facade skeleton."""

from __future__ import annotations

from typing import Any

from .catalog import AttachedDatabase, ColumnDefinition, DuckTableEntry, TableDefinition
from .checkpoint import MetadataWriter, SingleFileTableDataWriter
from .storage import DataTable
from .checkpoint import SingleFileTableDataWriter, MetadataWriter


class MiniDatabaseEngine:
    """Small facade over catalog and storage objects.

    Goal for the candidate:
    - create schemas and tables through the attached database catalog
    - insert and scan rows through logical names
    - expose checkpoint metadata from the underlying `DataTable`
    """

    def __init__(
        self,
        database_name: str,
        table_data_writer: SingleFileTableDataWriter | None = None,
    ) -> None:
        self.database = AttachedDatabase(database_name)
        self.data_writer = table_data_writer or SingleFileTableDataWriter(MetadataWriter())

    def create_schema(self, schema_name: str) -> None:
        """Create a schema if needed through the attached database catalog."""
        self.database.catalog.create_schema(schema_name)

    def create_table(
        self, schema_name: str, table_name: str, columns: list[ColumnDefinition]
    ) -> None:
        """Create a table and attach a `DataTable` implementation."""
        table_definition = self.build_table_definition(table_name, columns)
        data_table = DataTable(table_definition)
        schema = self.database.get_catalog().get_schema(schema_name)
        
        if schema:
            schema.create_table(table_definition, data_table)

    def insert_rows(
        self, schema_name: str, table_name: str, rows: list[dict[str, Any]]
    ) -> None:
        """Append rows into a table."""
        """Create a table and attach a `DataTable` implementation."""
        schema = self.database.get_catalog().get_schema(schema_name)

        if schema:
            table = schema.get_table(table_name)
            if table:
                table.data_table.append_rows(rows)

    def scan_rows(
        self, schema_name: str, table_name: str, row_start: int, count: int
    ) -> list[dict[str, Any]]:
        """Read rows from a table."""
        schema = self.database.get_catalog().get_schema(schema_name)

        if schema:
            table = schema.get_table(table_name)
            if table:
                return table.data_table.scan_rows(row_start, count)
        
        return []

    def checkpoint_table(self, schema_name: str, table_name: str) -> dict[str, Any]:
        """Checkpoint a table and return the produced metadata."""
        schema = self.database.get_catalog().get_schema(schema_name)

        if schema:
            table = schema.get_table(table_name)
            if table:
                return table.data_table.checkpoint(self.data_writer)
            
        return {}

    @staticmethod
    def build_table_definition(
        table_name: str, columns: list[ColumnDefinition]
    ) -> TableDefinition:
        """Convenience helper for the demo script and tests."""
        return TableDefinition(name=table_name, columns=columns)
