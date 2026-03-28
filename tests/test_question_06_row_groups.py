import unittest

from columnar_storage.catalog import ColumnDefinition, TableDefinition
from columnar_storage.storage import RowGroup


class RowGroupQuestionTests(unittest.TestCase):
    """Question 6: row groups and column data."""

    def setUp(self) -> None:
        self.definition = TableDefinition(
            name="events",
            columns=[
                ColumnDefinition("id", int, nullable=False),
                ColumnDefinition("kind", str, nullable=False),
                ColumnDefinition("value", int, nullable=True),
            ],
        )

    def test_append_rows_splits_data_by_column(self) -> None:
        row_group = RowGroup(self.definition, start=0, max_rows=10)
        row_group.append_rows([
            {"id": 1, "kind": "click", "value": 10},
            {"id": 2, "kind": "view", "value": None},
        ])

        self.assertEqual(row_group.count, 2)
        self.assertEqual(row_group.columns["id"].scan(0, 2), [1, 2])
        self.assertEqual(row_group.columns["kind"].scan(0, 2), ["click", "view"])

    def test_scan_rows_reconstructs_row_dictionaries(self) -> None:
        row_group = RowGroup(self.definition, start=0, max_rows=10)
        row_group.append_rows([
            {"id": 1, "kind": "click", "value": 10},
            {"id": 2, "kind": "view", "value": None},
            {"id": 3, "kind": "buy", "value": 30},
        ])

        # Query results must be rebuilt into row-shaped records even though storage is columnar.
        rows = row_group.scan_rows(1, 2)

        self.assertEqual(rows, [
            {"id": 2, "kind": "view", "value": None},
            {"id": 3, "kind": "buy", "value": 30},
        ])

    def test_delete_row_hides_it_from_scans(self) -> None:
        row_group = RowGroup(self.definition, start=0, max_rows=10)
        row_group.append_rows([
            {"id": 1, "kind": "click", "value": 10},
            {"id": 2, "kind": "view", "value": None},
            {"id": 3, "kind": "buy", "value": 30},
        ])
        # Deletes stay logical so later reads skip tombstoned rows without reshuffling storage.
        row_group.delete_row(1)

        rows = row_group.scan_rows(0, 3)

        self.assertEqual(rows, [
            {"id": 1, "kind": "click", "value": 10},
            {"id": 3, "kind": "buy", "value": 30},
        ])


if __name__ == "__main__":
    unittest.main()
