import unittest

from columnar_storage.catalog import ColumnDefinition, TableDefinition
from columnar_storage.storage import DataTable


class DataTableQuestionTests(unittest.TestCase):
    """Question 7: full table hierarchy."""

    def test_append_rows_creates_multiple_row_groups(self) -> None:
        definition = TableDefinition(
            name="events",
            columns=[
                ColumnDefinition("id", int, nullable=False),
                ColumnDefinition("kind", str, nullable=False),
            ],
        )
        table = DataTable(definition, row_group_size=2)
        # Appending past one batch boundary should create extra row groups automatically.
        table.append_rows([
            {"id": 1, "kind": "a"},
            {"id": 2, "kind": "b"},
            {"id": 3, "kind": "c"},
            {"id": 4, "kind": "d"},
            {"id": 5, "kind": "e"},
        ])

        self.assertEqual(len(table.row_groups.row_groups.nodes), 3)
        self.assertEqual(table.row_groups.total_rows(), 5)

    def test_scan_rows_crosses_row_group_boundaries(self) -> None:
        definition = TableDefinition(
            name="events",
            columns=[
                ColumnDefinition("id", int, nullable=False),
                ColumnDefinition("kind", str, nullable=False),
            ],
        )
        table = DataTable(definition, row_group_size=2)
        table.append_rows([
            {"id": 1, "kind": "a"},
            {"id": 2, "kind": "b"},
            {"id": 3, "kind": "c"},
            {"id": 4, "kind": "d"},
        ])

        # Pagination should stay seamless even when the requested slice crosses group boundaries.
        rows = table.scan_rows(1, 3)

        self.assertEqual(rows, [
            {"id": 2, "kind": "b"},
            {"id": 3, "kind": "c"},
            {"id": 4, "kind": "d"},
        ])


if __name__ == "__main__":
    unittest.main()
