import unittest

import main
from columnar_storage.catalog import ColumnDefinition
from columnar_storage.database import MiniDatabaseEngine


class MainDemoQuestionTests(unittest.TestCase):
    """Question 10: final integration demo."""

    def make_engine(self, row_group_size: int = 2) -> MiniDatabaseEngine:
        """Build an engine configured for small row groups so integration edge cases are easy to hit."""
        engine = MiniDatabaseEngine("exercise_db")
        engine.create_schema("analytics")
        engine.create_table(
            "analytics",
            "events",
            columns=[
                ColumnDefinition("event_id", int, nullable=False),
                ColumnDefinition("category", str, nullable=False),
                ColumnDefinition("value", int, nullable=True),
            ],
        )
        schema = engine.database.get_catalog().get_schema("analytics")
        assert schema is not None
        table_entry = schema.get_table("events")
        assert table_entry is not None
        table_entry.data_table.row_groups.row_group_size = row_group_size
        return engine

    def test_run_demo_returns_checkpoint_and_visible_rows(self) -> None:
        # The demo should tell the full story an end user would see after running the program.
        payload = main.run_demo()

        self.assertEqual(payload["database"], "exercise_db")
        self.assertEqual(payload["schema"], "analytics")
        self.assertEqual(payload["table"], "events")
        self.assertEqual(
            payload["visible_rows"],
            [
                {"event_id": 1, "category": "click", "value": 10},
                {"event_id": 3, "category": "purchase", "value": None},
                {"event_id": 4, "category": "view", "value": 20},
            ],
        )
        self.assertEqual(payload["checkpoint"]["table_name"], "events")
        self.assertEqual(payload["checkpoint"]["total_rows"], 4)

    def test_engine_scan_rows_supports_overlapping_row_starts(self) -> None:
        engine = self.make_engine(row_group_size=2)
        engine.insert_rows(
            "analytics",
            "events",
            [
                {"event_id": 1, "category": "click", "value": 10},
                {"event_id": 2, "category": "view", "value": 20},
                {"event_id": 3, "category": "purchase", "value": None},
                {"event_id": 4, "category": "view", "value": 30},
                {"event_id": 5, "category": "signup", "value": 40},
            ],
        )

        # Neighboring scans should stay aligned when one starts before and the other starts on
        # the next row-group boundary.
        self.assertEqual(
            engine.scan_rows("analytics", "events", 1, 3),
            [
                {"event_id": 2, "category": "view", "value": 20},
                {"event_id": 3, "category": "purchase", "value": None},
                {"event_id": 4, "category": "view", "value": 30},
            ],
        )
        self.assertEqual(
            engine.scan_rows("analytics", "events", 2, 3),
            [
                {"event_id": 3, "category": "purchase", "value": None},
                {"event_id": 4, "category": "view", "value": 30},
                {"event_id": 5, "category": "signup", "value": 40},
            ],
        )

    def test_engine_repeated_actions_keep_rows_and_checkpoints_stable(self) -> None:
        engine = self.make_engine(row_group_size=2)

        engine.insert_rows("analytics", "events", [])
        engine.insert_rows(
            "analytics",
            "events",
            [{"event_id": 1, "category": "click", "value": 10}],
        )
        engine.insert_rows("analytics", "events", [])
        engine.insert_rows(
            "analytics",
            "events",
            [
                {"event_id": 2, "category": "view", "value": 20},
                {"event_id": 3, "category": "purchase", "value": None},
            ],
        )

        first_scan = engine.scan_rows("analytics", "events", 0, 10)
        second_scan = engine.scan_rows("analytics", "events", 0, 10)
        first_checkpoint = engine.checkpoint_table("analytics", "events")
        second_checkpoint = engine.checkpoint_table("analytics", "events")

        self.assertEqual(
            first_scan,
            [
                {"event_id": 1, "category": "click", "value": 10},
                {"event_id": 2, "category": "view", "value": 20},
                {"event_id": 3, "category": "purchase", "value": None},
            ],
        )
        self.assertEqual(second_scan, first_scan)
        self.assertEqual(first_checkpoint["total_rows"], 3)
        self.assertEqual(second_checkpoint["total_rows"], 3)
        self.assertEqual(
            first_checkpoint["row_groups"], second_checkpoint["row_groups"]
        )

    def test_engine_insert_rows_spills_overflow_into_later_row_groups(self) -> None:
        engine = self.make_engine(row_group_size=3)
        engine.insert_rows(
            "analytics",
            "events",
            [
                {"event_id": 1, "category": "click", "value": 10},
                {"event_id": 2, "category": "view", "value": 20},
            ],
        )
        engine.insert_rows(
            "analytics",
            "events",
            [
                {"event_id": 3, "category": "purchase", "value": None},
                {"event_id": 4, "category": "view", "value": 30},
                {"event_id": 5, "category": "signup", "value": 40},
                {"event_id": 6, "category": "refund", "value": 50},
            ],
        )

        schema = engine.database.get_catalog().get_schema("analytics")
        assert schema is not None
        table_entry = schema.get_table("events")
        assert table_entry is not None
        row_groups = table_entry.data_table.row_groups.row_groups.nodes

        self.assertEqual(
            [(row_group.start, row_group.count) for row_group in row_groups],
            [(0, 3), (3, 3)],
        )
        self.assertEqual(
            engine.scan_rows("analytics", "events", 0, 10),
            [
                {"event_id": 1, "category": "click", "value": 10},
                {"event_id": 2, "category": "view", "value": 20},
                {"event_id": 3, "category": "purchase", "value": None},
                {"event_id": 4, "category": "view", "value": 30},
                {"event_id": 5, "category": "signup", "value": 40},
                {"event_id": 6, "category": "refund", "value": 50},
            ],
        )

    def test_engine_insert_rows_raises_errors_without_mutating_existing_rows(
        self,
    ) -> None:
        engine = self.make_engine(row_group_size=2)
        engine.insert_rows(
            "analytics",
            "events",
            [
                {"event_id": 1, "category": "click", "value": 10},
                {"event_id": 2, "category": "view", "value": 20},
            ],
        )

        with self.assertRaises(ValueError):
            engine.insert_rows(
                "analytics",
                "events",
                [{"event_id": 3, "category": None, "value": 30}],
            )

        self.assertEqual(
            engine.scan_rows("analytics", "events", 0, 10),
            [
                {"event_id": 1, "category": "click", "value": 10},
                {"event_id": 2, "category": "view", "value": 20},
            ],
        )
        self.assertEqual(
            engine.checkpoint_table("analytics", "events")["total_rows"], 2
        )


if __name__ == "__main__":
    unittest.main()
