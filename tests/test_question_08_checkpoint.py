import unittest

from columnar_storage.catalog import ColumnDefinition, TableDefinition
from columnar_storage.checkpoint import MetadataWriter, SingleFileTableDataWriter
from columnar_storage.storage import DataTable


class CheckpointQuestionTests(unittest.TestCase):
    """Question 8: checkpoint state flow."""

    def setUp(self) -> None:
        self.definition = TableDefinition(
            name="events",
            columns=[
                ColumnDefinition("id", int, nullable=False),
                ColumnDefinition("kind", str, nullable=False),
                ColumnDefinition("value", int, nullable=True),
            ],
        )

    def test_metadata_writer_round_trip(self) -> None:
        writer = MetadataWriter()
        pointer = writer.write_payload({"hello": "world"})

        self.assertEqual(pointer, {"index": 0})
        self.assertEqual(writer.read_payload(pointer), {"hello": "world"})

    def test_table_checkpoint_returns_row_group_metadata(self) -> None:
        table = DataTable(self.definition, row_group_size=2)
        table.append_rows([
            {"id": 1, "kind": "a", "value": 10},
            {"id": 2, "kind": "b", "value": None},
            {"id": 3, "kind": "c", "value": 30},
        ])
        table.row_groups.row_groups.nodes[0].delete_row(1)

        payload = table.checkpoint()

        self.assertEqual(payload["table_name"], "events")
        self.assertEqual(payload["total_rows"], 3)
        self.assertEqual(len(payload["row_groups"]), 2)
        self.assertIn("table_pointer", payload)
        self.assertEqual(payload["row_groups"][0]["row_start"], 0)
        self.assertEqual(payload["row_groups"][0]["delete_pointers"][0]["deleted_row_ids"], [1])

    def test_single_file_table_writer_builds_catalog_facing_payload(self) -> None:
        writer = MetadataWriter()
        table_writer = SingleFileTableDataWriter(writer)

        payload = table_writer.finalize_table(
            table_name="events",
            table_statistics={"row_count": 3},
            row_group_pointers=[],
        )

        self.assertEqual(payload["table_name"], "events")
        self.assertEqual(payload["table_pointer"], {"index": 0})
        self.assertEqual(payload["total_rows"], 0)
        self.assertEqual(writer.read_payload({"index": 0})["table_statistics"], {"row_count": 3})


if __name__ == "__main__":
    unittest.main()
