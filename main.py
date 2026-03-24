"""End-to-end demo for the columnar storage exercise.

This script should run once all exercise questions are implemented.
"""

from __future__ import annotations

from pprint import pprint
from typing import Any

from columnar_storage.catalog import ColumnDefinition
from columnar_storage.database import MiniDatabaseEngine


def run_demo() -> dict[str, Any]:
    """Run a small end-to-end scenario.

    Expected finished behavior:
    - create a database and schema
    - create a table
    - append rows
    - delete one row through the underlying row group
    - checkpoint table metadata
    - scan rows back
    """

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

    rows = [
        {"event_id": 1, "category": "click", "value": 10},
        {"event_id": 2, "category": "view", "value": 20},
        {"event_id": 3, "category": "purchase", "value": None},
        {"event_id": 4, "category": "view", "value": 20},
    ]
    engine.insert_rows("analytics", "events", rows)

    table_entry = engine.database.get_catalog().get_schema("analytics").get_table("events")
    first_row_group = table_entry.data_table.row_groups.row_groups.nodes[0]
    first_row_group.delete_row(1)

    checkpoint_metadata = engine.checkpoint_table("analytics", "events")
    visible_rows = engine.scan_rows("analytics", "events", 0, 10)

    return {
        "database": engine.database.name,
        "schema": "analytics",
        "table": "events",
        "checkpoint": checkpoint_metadata,
        "visible_rows": visible_rows,
    }


if __name__ == "__main__":
    pprint(run_demo())
