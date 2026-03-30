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

    def make_definition(self, name: str = "events") -> TableDefinition:
        return TableDefinition(
            name=name,
            columns=[ColumnDefinition("id", int, nullable=False)],
        )

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
        definition = self.make_definition()
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
        definition = self.make_definition()
        first_table = DataTable(definition, row_group_size=4)
        second_table = DataTable(definition, row_group_size=8)
        schema.create_table(definition, first_table)

        # A repeated create should fail even when the caller passes a different storage object.
        with self.assertRaises(ValueError):
            schema.create_table(definition, second_table)

    def test_catalog_handles_repeated_schema_creation_explicitly(self) -> None:
        database = AttachedDatabase("demo")
        catalog = database.get_catalog()
        created = catalog.create_schema("analytics")

        # Repeating the same catalog action should be deterministic: either idempotent
        # or rejected clearly, but never silently create a shadow schema.
        try:
            repeated = catalog.create_schema("analytics")
        except ValueError:
            self.assertIs(catalog.get_schema("analytics"), created)
        else:
            self.assertIs(repeated, created)
            self.assertIs(catalog.get_schema("analytics"), created)

    def test_missing_schema_and_table_lookups_do_not_spill_or_overlap(self) -> None:
        database = AttachedDatabase("demo")
        catalog = database.get_catalog()
        analytics = catalog.create_schema("analytics")
        analytics.create_table(
            self.make_definition("events"),
            DataTable(self.make_definition("events"), row_group_size=4),
        )

        # Missing names should resolve cleanly without leaking another schema or table.
        self.assertIsNone(catalog.get_schema("missing"))
        self.assertIsNone(analytics.get_table("missing"))

    def test_same_table_name_in_different_schemas_does_not_overlap(self) -> None:
        database = AttachedDatabase("demo")
        catalog = database.get_catalog()
        analytics = catalog.create_schema("analytics")
        staging = catalog.create_schema("staging")

        analytics_definition = self.make_definition("events")
        staging_definition = self.make_definition("events")
        analytics_table = DataTable(analytics_definition, row_group_size=4)
        staging_table = DataTable(staging_definition, row_group_size=8)

        analytics_entry = analytics.create_table(analytics_definition, analytics_table)
        staging_entry = staging.create_table(staging_definition, staging_table)

        # Namespace boundaries prevent one schema registration from spilling into another.
        self.assertIs(analytics.get_table("events"), analytics_entry)
        self.assertIs(staging.get_table("events"), staging_entry)
        self.assertIsNot(analytics_entry, staging_entry)
        self.assertIsNot(analytics_entry.data_table, staging_entry.data_table)


if __name__ == "__main__":
    unittest.main()
