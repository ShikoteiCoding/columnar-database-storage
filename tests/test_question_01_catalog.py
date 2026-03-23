import unittest

from columnar_storage.catalog import AttachedDatabase, ColumnDefinition, Schema, TableDefinition
from columnar_storage.storage import DataTable


class CatalogQuestionTests(unittest.TestCase):
    """Question 1: catalog hierarchy."""

    def test_database_creates_and_returns_schema(self) -> None:
        database = AttachedDatabase("demo")
        created = database.create_schema("analytics")

        self.assertIsInstance(created, Schema)
        self.assertIs(database.get_schema("analytics"), created)

    def test_schema_registers_table_entry(self) -> None:
        schema = Schema("analytics")
        definition = TableDefinition(
            name="events",
            columns=[ColumnDefinition("id", int, nullable=False)],
        )
        table = DataTable(definition, row_group_size=4)

        entry = schema.create_table(definition, table)

        self.assertEqual(entry.definition.name, "events")
        self.assertIs(entry.data_table, table)
        self.assertIs(schema.get_table("events"), entry)

    def test_schema_rejects_duplicate_table_names(self) -> None:
        schema = Schema("analytics")
        definition = TableDefinition(
            name="events",
            columns=[ColumnDefinition("id", int, nullable=False)],
        )
        table = DataTable(definition, row_group_size=4)
        schema.create_table(definition, table)

        with self.assertRaises(ValueError):
            schema.create_table(definition, table)


if __name__ == "__main__":
    unittest.main()
