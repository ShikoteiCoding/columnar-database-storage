import unittest

from columnar_storage.catalog import (
    AttachedDatabase,
    ColumnDefinition,
    DuckTableEntry,
    Schema,
    TableDefinition,
)
from columnar_storage.storage import DataTable


class CatalogQuestionTests(unittest.TestCase):
    """Question 1: catalog hierarchy."""

    def test_database_exposes_catalog_that_creates_and_returns_schema(self) -> None:
        database = AttachedDatabase("demo")
        catalog = database.get_catalog()
        # A new workload usually creates its schema before any tables are registered.
        created = catalog.create_schema("analytics")
        # Later requests resolve that same schema by name rather than carrying the object around.
        looked_up = catalog.get_schema("analytics")

        self.assertIs(database.get_catalog(), catalog)
        self.assertIsInstance(created, Schema)
        self.assertIsNotNone(looked_up)
        self.assertIs(looked_up, created)

    def test_schema_registers_table_entry(self) -> None:
        schema = Schema("analytics")
        definition = TableDefinition(
            name="events",
            columns=[ColumnDefinition("id", int, nullable=False)],
        )
        table = DataTable(definition, row_group_size=4)

        # The schema keeps the catalog-facing handle that points to the physical table storage.
        entry = schema.create_table(definition, table)
        looked_up = schema.get_table("events")

        self.assertIsInstance(entry, DuckTableEntry)
        self.assertEqual(entry.definition.name, "events")
        self.assertIs(entry.data_table, table)
        self.assertIsNotNone(looked_up)
        self.assertIs(looked_up, entry)

    def test_schema_rejects_duplicate_table_names(self) -> None:
        schema = Schema("analytics")
        definition = TableDefinition(
            name="events",
            columns=[ColumnDefinition("id", int, nullable=False)],
        )
        table = DataTable(definition, row_group_size=4)
        schema.create_table(definition, table)

        # A repeated create should fail like a migration colliding with an existing production table.
        with self.assertRaises(ValueError):
            schema.create_table(definition, table)


if __name__ == "__main__":
    unittest.main()
