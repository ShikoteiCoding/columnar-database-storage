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
        row_group.append_rows(
            [
                {"id": 1, "kind": "click", "value": 10},
                {"id": 2, "kind": "view", "value": None},
            ]
        )

        self.assertEqual(row_group.count, 2)
        self.assertEqual(row_group.columns["id"].scan(0, 2), [1, 2])
        self.assertEqual(row_group.columns["kind"].scan(0, 2), ["click", "view"])

    def test_append_rows_supports_chained_batches_until_capacity(self) -> None:
        row_group = RowGroup(self.definition, start=0, max_rows=4)

        row_group.append_rows(
            [
                {"id": 1, "kind": "click", "value": 10},
                {"id": 2, "kind": "view", "value": None},
            ]
        )
        row_group.append_rows(
            [
                {"id": 3, "kind": "buy", "value": 30},
                {"id": 4, "kind": "share", "value": 40},
            ]
        )

        self.assertEqual(row_group.count, 4)
        self.assertTrue(row_group.is_full())
        self.assertEqual(
            row_group.scan_rows(0, 4),
            [
                {"id": 1, "kind": "click", "value": 10},
                {"id": 2, "kind": "view", "value": None},
                {"id": 3, "kind": "buy", "value": 30},
                {"id": 4, "kind": "share", "value": 40},
            ],
        )

    def test_append_rows_rejects_overflow_without_partial_mutation(self) -> None:
        row_group = RowGroup(self.definition, start=0, max_rows=3)
        row_group.append_rows(
            [
                {"id": 1, "kind": "click", "value": 10},
                {"id": 2, "kind": "view", "value": None},
            ]
        )

        # Row-group overflow should be handled by the table layer, not by silently spilling here.
        with self.assertRaises(ValueError):
            row_group.append_rows(
                [
                    {"id": 3, "kind": "buy", "value": 30},
                    {"id": 4, "kind": "share", "value": 40},
                ]
            )

        self.assertEqual(row_group.count, 2)
        self.assertEqual(
            row_group.scan_rows(0, 10),
            [
                {"id": 1, "kind": "click", "value": 10},
                {"id": 2, "kind": "view", "value": None},
            ],
        )

    def test_append_rows_validates_row_shape_and_nullability(self) -> None:
        row_group = RowGroup(self.definition, start=0, max_rows=10)

        invalid_rows = [
            {"id": 1, "kind": "click"},
            {"id": 2, "kind": "view", "value": None, "extra": "ignored?"},
            {"id": None, "kind": "buy", "value": 30},
        ]

        for row in invalid_rows:
            with self.subTest(row=row):
                with self.assertRaises(ValueError):
                    row_group.append_rows([row])

        self.assertEqual(row_group.count, 0)
        self.assertEqual(row_group.scan_rows(0, 10), [])

    def test_append_rows_with_empty_batch_is_a_noop(self) -> None:
        row_group = RowGroup(self.definition, start=0, max_rows=10)

        row_group.append_rows([])

        self.assertEqual(row_group.count, 0)
        self.assertFalse(row_group.is_full())
        self.assertEqual(row_group.scan_rows(0, 10), [])

    def test_scan_rows_reconstructs_row_dictionaries(self) -> None:
        row_group = RowGroup(self.definition, start=0, max_rows=10)
        row_group.append_rows(
            [
                {"id": 1, "kind": "click", "value": 10},
                {"id": 2, "kind": "view", "value": None},
                {"id": 3, "kind": "buy", "value": 30},
            ]
        )

        # Query results must be rebuilt into row-shaped records even though storage is columnar.
        rows = row_group.scan_rows(1, 2)

        self.assertEqual(
            rows,
            [
                {"id": 2, "kind": "view", "value": None},
                {"id": 3, "kind": "buy", "value": 30},
            ],
        )

    def test_scan_rows_clips_past_end_and_handles_empty_tail_ranges(self) -> None:
        row_group = RowGroup(self.definition, start=0, max_rows=10)
        row_group.append_rows(
            [
                {"id": 1, "kind": "click", "value": 10},
                {"id": 2, "kind": "view", "value": None},
                {"id": 3, "kind": "buy", "value": 30},
            ]
        )

        self.assertEqual(
            row_group.scan_rows(2, 10),
            [
                {"id": 3, "kind": "buy", "value": 30},
            ],
        )
        self.assertEqual(row_group.scan_rows(3, 2), [])

    def test_scan_rows_reconstructs_across_multiple_column_segments(self) -> None:
        row_group = RowGroup(self.definition, start=0, max_rows=10)
        for column in row_group.columns.values():
            column.segment_size = 2

        row_group.append_rows(
            [
                {"id": 1, "kind": "click", "value": 10},
                {"id": 2, "kind": "view", "value": None},
                {"id": 3, "kind": "buy", "value": 30},
                {"id": 4, "kind": "share", "value": 40},
                {"id": 5, "kind": "refund", "value": 50},
            ]
        )

        # Column data may spill into several segments internally, but row reconstruction stays seamless.
        self.assertEqual(len(row_group.columns["id"].segment_tree.nodes), 3)
        self.assertEqual(
            row_group.scan_rows(1, 4),
            [
                {"id": 2, "kind": "view", "value": None},
                {"id": 3, "kind": "buy", "value": 30},
                {"id": 4, "kind": "share", "value": 40},
                {"id": 5, "kind": "refund", "value": 50},
            ],
        )

    def test_delete_row_hides_it_from_scans(self) -> None:
        row_group = RowGroup(self.definition, start=0, max_rows=10)
        row_group.append_rows(
            [
                {"id": 1, "kind": "click", "value": 10},
                {"id": 2, "kind": "view", "value": None},
                {"id": 3, "kind": "buy", "value": 30},
            ]
        )
        # Deletes stay logical so later reads skip tombstoned rows without reshuffling storage.
        row_group.delete_row(1)

        rows = row_group.scan_rows(0, 3)

        self.assertEqual(
            rows,
            [
                {"id": 1, "kind": "click", "value": 10},
                {"id": 3, "kind": "buy", "value": 30},
            ],
        )

    def test_delete_row_is_idempotent_for_repeated_logical_deletes(self) -> None:
        row_group = RowGroup(self.definition, start=0, max_rows=10)
        row_group.append_rows(
            [
                {"id": 1, "kind": "click", "value": 10},
                {"id": 2, "kind": "view", "value": None},
                {"id": 3, "kind": "buy", "value": 30},
            ]
        )

        row_group.delete_row(1)
        row_group.delete_row(1)

        self.assertEqual(row_group.version_info.deleted_row_ids, {1})
        self.assertEqual(
            row_group.scan_rows(0, 3),
            [
                {"id": 1, "kind": "click", "value": 10},
                {"id": 3, "kind": "buy", "value": 30},
            ],
        )

    def test_delete_row_rejects_out_of_range_absolute_row_ids(self) -> None:
        row_group = RowGroup(self.definition, start=10, max_rows=10)
        row_group.append_rows(
            [
                {"id": 1, "kind": "click", "value": 10},
                {"id": 2, "kind": "view", "value": None},
            ]
        )

        with self.assertRaises(KeyError):
            row_group.delete_row(9)

        with self.assertRaises(KeyError):
            row_group.delete_row(12)


if __name__ == "__main__":
    unittest.main()
