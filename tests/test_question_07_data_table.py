import unittest

from columnar_storage.catalog import ColumnDefinition, TableDefinition
from columnar_storage.storage import DataTable


class DataTableQuestionTests(unittest.TestCase):
    """Question 7: full table hierarchy."""

    def make_table(self, row_group_size: int = 2) -> DataTable:
        definition = TableDefinition(
            name="events",
            columns=[
                ColumnDefinition("id", int, nullable=False),
                ColumnDefinition("kind", str, nullable=False),
            ],
        )
        return DataTable(definition, row_group_size=row_group_size)

    def test_append_rows_creates_multiple_row_groups(self) -> None:
        table = self.make_table(row_group_size=2)
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
        table = self.make_table(row_group_size=2)
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

    def test_scan_rows_supports_overlapping_row_starts_around_group_edges(self) -> None:
        table = self.make_table(row_group_size=2)
        table.append_rows([
            {"id": 1, "kind": "a"},
            {"id": 2, "kind": "b"},
            {"id": 3, "kind": "c"},
            {"id": 4, "kind": "d"},
            {"id": 5, "kind": "e"},
        ])

        # Neighboring scans with overlapping starts should stay aligned even when one starts
        # inside a row group and the next starts exactly on the following group's boundary.
        self.assertEqual(table.scan_rows(1, 3), [
            {"id": 2, "kind": "b"},
            {"id": 3, "kind": "c"},
            {"id": 4, "kind": "d"},
        ])
        self.assertEqual(table.scan_rows(2, 3), [
            {"id": 3, "kind": "c"},
            {"id": 4, "kind": "d"},
            {"id": 5, "kind": "e"},
        ])

    def test_append_rows_supports_repeated_actions_without_duplication(self) -> None:
        table = self.make_table(row_group_size=2)

        table.append_rows([{"id": 1, "kind": "a"}])
        table.append_rows([])
        table.append_rows([{"id": 2, "kind": "b"}])
        table.append_rows([])
        table.append_rows([
            {"id": 3, "kind": "c"},
            {"id": 4, "kind": "d"},
        ])

        first_scan = table.scan_rows(0, 4)
        second_scan = table.scan_rows(0, 4)

        self.assertEqual(first_scan, [
            {"id": 1, "kind": "a"},
            {"id": 2, "kind": "b"},
            {"id": 3, "kind": "c"},
            {"id": 4, "kind": "d"},
        ])
        self.assertEqual(second_scan, first_scan)
        self.assertEqual(table.row_groups.total_rows(), 4)

    def test_append_rows_spills_overflow_into_later_row_groups(self) -> None:
        table = self.make_table(row_group_size=3)
        table.append_rows([
            {"id": 1, "kind": "a"},
            {"id": 2, "kind": "b"},
        ])

        # When the active row group has only one slot left, the remaining rows should spill
        # into a fresh row group instead of failing or dropping rows.
        table.append_rows([
            {"id": 3, "kind": "c"},
            {"id": 4, "kind": "d"},
            {"id": 5, "kind": "e"},
            {"id": 6, "kind": "f"},
        ])

        self.assertEqual(
            [(row_group.start, row_group.count) for row_group in table.row_groups.row_groups.nodes],
            [(0, 3), (3, 3)],
        )
        self.assertEqual(table.scan_rows(0, 6), [
            {"id": 1, "kind": "a"},
            {"id": 2, "kind": "b"},
            {"id": 3, "kind": "c"},
            {"id": 4, "kind": "d"},
            {"id": 5, "kind": "e"},
            {"id": 6, "kind": "f"},
        ])

    def test_append_rows_raises_for_invalid_rows_without_mutating_existing_data(self) -> None:
        table = self.make_table(row_group_size=2)
        table.append_rows([
            {"id": 1, "kind": "a"},
            {"id": 2, "kind": "b"},
        ])

        # Table-level appends should surface validation errors from lower layers and leave the
        # already stored rows intact.
        with self.assertRaises(ValueError):
            table.append_rows([
                {"id": 3, "kind": None},
            ])

        self.assertEqual(table.row_groups.total_rows(), 2)
        self.assertEqual(table.scan_rows(0, 10), [
            {"id": 1, "kind": "a"},
            {"id": 2, "kind": "b"},
        ])

    def test_scan_rows_returns_empty_for_past_end_ranges_and_zero_count(self) -> None:
        table = self.make_table(row_group_size=2)
        table.append_rows([
            {"id": 1, "kind": "a"},
            {"id": 2, "kind": "b"},
            {"id": 3, "kind": "c"},
        ])

        self.assertEqual(table.scan_rows(3, 2), [])
        self.assertEqual(table.scan_rows(1, 0), [])


if __name__ == "__main__":
    unittest.main()
