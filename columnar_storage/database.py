"""High-level database facade skeleton."""

from __future__ import annotations

from typing import Any

from .catalog import AttachedDatabase, ColumnDefinition, DuckTableEntry, TableDefinition
from .checkpoint import MetadataWriter, SingleFileTableDataWriter
from .storage import DataTable


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
        self.table_data_writer = table_data_writer or SingleFileTableDataWriter(MetadataWriter())

    def create_schema(self, schema_name: str) -> None:
        """Create a schema if needed through the attached database catalog."""
        raise NotImplementedError("Question 9: implement MiniDatabaseEngine.create_schema()")

    def create_table(self, schema_name: str, table_name: str, columns: list[ColumnDefinition]) -> None:
        """Create a table and attach a `DataTable` implementation."""
        raise NotImplementedError("Question 9: implement MiniDatabaseEngine.create_table()")

    def insert_rows(self, schema_name: str, table_name: str, rows: list[dict[str, Any]]) -> None:
        """Append rows into a table."""
        raise NotImplementedError("Question 9: implement MiniDatabaseEngine.insert_rows()")

    def scan_rows(self, schema_name: str, table_name: str, row_start: int, count: int) -> list[dict[str, Any]]:
        """Read rows from a table."""
        raise NotImplementedError("Question 9: implement MiniDatabaseEngine.scan_rows()")

    def checkpoint_table(self, schema_name: str, table_name: str) -> dict[str, Any]:
        """Checkpoint a table and return the produced metadata."""
        raise NotImplementedError("Question 9: implement MiniDatabaseEngine.checkpoint_table()")

    @staticmethod
    def build_table_definition(table_name: str, columns: list[ColumnDefinition]) -> TableDefinition:
        """Convenience helper for the demo script and tests."""
        return TableDefinition(name=table_name, columns=columns)
