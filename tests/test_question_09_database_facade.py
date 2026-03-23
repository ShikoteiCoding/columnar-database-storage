import unittest

from columnar_storage.catalog import ColumnDefinition
from columnar_storage.database import MiniDatabaseEngine


class DatabaseFacadeQuestionTests(unittest.TestCase):
    """Question 9: high-level database facade."""

    def test_create_table_insert_scan_and_checkpoint(self) -> None:
        engine = MiniDatabaseEngine("exercise_db")
        engine.create_schema("analytics")
        engine.create_table(
            "analytics",
            "events",
            columns=[
                ColumnDefinition("id", int, nullable=False),
                ColumnDefinition("kind", str, nullable=False),
            ],
        )
        engine.insert_rows(
            "analytics",
            "events",
            [
                {"id": 1, "kind": "a"},
                {"id": 2, "kind": "b"},
                {"id": 3, "kind": "c"},
            ],
        )

        rows = engine.scan_rows("analytics", "events", 1, 2)
        checkpoint = engine.checkpoint_table("analytics", "events")

        self.assertEqual(rows, [
            {"id": 2, "kind": "b"},
            {"id": 3, "kind": "c"},
        ])
        self.assertEqual(checkpoint["table_name"], "events")
        self.assertEqual(checkpoint["total_rows"], 3)


if __name__ == "__main__":
    unittest.main()
