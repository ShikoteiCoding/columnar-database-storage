import unittest

from columnar_storage.catalog import ColumnDefinition
from columnar_storage.database import MiniDatabaseEngine


class DatabaseFacadeQuestionTests(unittest.TestCase):
    """Question 9: high-level database facade."""

    def make_engine(self) -> MiniDatabaseEngine:
        engine = MiniDatabaseEngine("exercise_db")
        engine.create_schema("analytics")
        return engine

    def create_events_table(self, engine: MiniDatabaseEngine) -> None:
        engine.create_table(
            "analytics",
            "events",
            columns=[
                ColumnDefinition("id", int, nullable=False),
                ColumnDefinition("kind", str, nullable=False),
            ],
        )

    def test_create_table_insert_scan_and_checkpoint(self) -> None:
        engine = MiniDatabaseEngine("exercise_db")
        # This mirrors the lifecycle an application uses through the public database API.
        engine.create_schema("analytics")
        self.create_events_table(engine)
        engine.insert_rows(
            "analytics",
            "events",
            [
                {"id": 1, "kind": "a"},
                {"id": 2, "kind": "b"},
                {"id": 3, "kind": "c"},
            ],
        )

        # Reads and checkpoints should compose cleanly because both happen in normal request flows.
        rows = engine.scan_rows("analytics", "events", 1, 2)
        checkpoint = engine.checkpoint_table("analytics", "events")

        self.assertEqual(rows, [
            {"id": 2, "kind": "b"},
            {"id": 3, "kind": "c"},
        ])
        self.assertEqual(checkpoint["table_name"], "events")
        self.assertEqual(checkpoint["total_rows"], 3)

    def test_scan_rows_handles_overlapping_row_start_windows(self) -> None:
        engine = self.make_engine()
        self.create_events_table(engine)
        engine.insert_rows(
            "analytics",
            "events",
            [
                {"id": 1, "kind": "a"},
                {"id": 2, "kind": "b"},
                {"id": 3, "kind": "c"},
                {"id": 4, "kind": "d"},
                {"id": 5, "kind": "e"},
            ],
        )

        # Repeated scans with overlapping row starts should stay stateless and deterministic.
        first_window = engine.scan_rows("analytics", "events", 1, 3)
        second_window = engine.scan_rows("analytics", "events", 2, 3)
        clipped_tail = engine.scan_rows("analytics", "events", 4, 3)

        self.assertEqual(first_window, [
            {"id": 2, "kind": "b"},
            {"id": 3, "kind": "c"},
            {"id": 4, "kind": "d"},
        ])
        self.assertEqual(second_window, [
            {"id": 3, "kind": "c"},
            {"id": 4, "kind": "d"},
            {"id": 5, "kind": "e"},
        ])
        self.assertEqual(clipped_tail, [
            {"id": 5, "kind": "e"},
        ])

    def test_repeated_actions_append_and_checkpoint_without_resetting_state(self) -> None:
        engine = self.make_engine()
        self.create_events_table(engine)

        engine.insert_rows(
            "analytics",
            "events",
            [
                {"id": 1, "kind": "a"},
                {"id": 2, "kind": "b"},
            ],
        )
        first_checkpoint = engine.checkpoint_table("analytics", "events")

        engine.insert_rows(
            "analytics",
            "events",
            [
                {"id": 3, "kind": "c"},
                {"id": 4, "kind": "d"},
            ],
        )
        second_checkpoint = engine.checkpoint_table("analytics", "events")
        rows_after_repeated_actions = engine.scan_rows("analytics", "events", 0, 10)

        self.assertEqual(first_checkpoint["total_rows"], 2)
        self.assertEqual(second_checkpoint["total_rows"], 4)
        self.assertEqual(rows_after_repeated_actions, [
            {"id": 1, "kind": "a"},
            {"id": 2, "kind": "b"},
            {"id": 3, "kind": "c"},
            {"id": 4, "kind": "d"},
        ])

    def test_insert_rows_spills_across_multiple_row_groups_when_capacity_is_small(self) -> None:
        engine = self.make_engine()
        self.create_events_table(engine)

        schema = engine.database.get_catalog().get_schema("analytics")
        assert schema is not None
        table_entry = schema.get_table("events")
        assert table_entry is not None
        table_entry.data_table.row_groups.row_group_size = 2

        engine.insert_rows(
            "analytics",
            "events",
            [
                {"id": 1, "kind": "a"},
                {"id": 2, "kind": "b"},
                {"id": 3, "kind": "c"},
                {"id": 4, "kind": "d"},
                {"id": 5, "kind": "e"},
            ],
        )

        self.assertEqual(len(table_entry.data_table.row_groups.row_groups.nodes), 3)
        self.assertEqual(engine.scan_rows("analytics", "events", 0, 10), [
            {"id": 1, "kind": "a"},
            {"id": 2, "kind": "b"},
            {"id": 3, "kind": "c"},
            {"id": 4, "kind": "d"},
            {"id": 5, "kind": "e"},
        ])

    def test_facade_raises_errors_for_duplicate_tables_and_invalid_rows(self) -> None:
        engine = self.make_engine()
        self.create_events_table(engine)

        # Duplicate catalog actions should fail clearly instead of silently replacing the table.
        with self.assertRaises(ValueError):
            self.create_events_table(engine)

        # Row validation errors from lower storage layers should surface through the facade.
        with self.assertRaises(ValueError):
            engine.insert_rows(
                "analytics",
                "events",
                [
                    {"id": 1},
                ],
            )

        self.assertEqual(engine.scan_rows("analytics", "events", 0, 10), [])


if __name__ == "__main__":
    unittest.main()
