import unittest

from columnar_storage.blocks import BLOCK_SIZE, BlockManager, PartialBlockManager
from columnar_storage.catalog import ColumnDefinition, TableDefinition
from columnar_storage.checkpoint import MetadataWriter, SingleFileTableDataWriter
from columnar_storage.storage import DataTable, RowGroup, RowGroupPointer


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

    def test_metadata_writer_supports_repeated_writes_without_clobbering_prior_payloads(self) -> None:
        writer = MetadataWriter()

        first = writer.write_payload({"batch": 1})
        second = writer.write_payload({"batch": 2})

        self.assertEqual(first, {"index": 0})
        self.assertEqual(second, {"index": 1})
        self.assertEqual(writer.read_payload(first), {"batch": 1})
        self.assertEqual(writer.read_payload(second), {"batch": 2})

    def test_metadata_writer_read_payload_rejects_unknown_pointer(self) -> None:
        writer = MetadataWriter()

        with self.assertRaises((IndexError, KeyError, ValueError)):
            writer.read_payload({"index": 99})

    def test_table_checkpoint_returns_row_group_metadata(self) -> None:
        table = DataTable(self.definition, row_group_size=2)
        writer = MetadataWriter()
        table_writer = SingleFileTableDataWriter(writer)
        table.append_rows([
            {"id": 1, "kind": "a", "value": 10},
            {"id": 2, "kind": "b", "value": None},
            {"id": 3, "kind": "c", "value": 30},
        ])
        # Tombstones must be captured too so recovery does not resurrect deleted rows.
        table.row_groups.row_groups.nodes[0].delete_row(1)

        payload = table.checkpoint(table_writer)
        table_metadata = writer.read_payload(payload["table_pointer"])

        self.assertEqual(payload["table_name"], "events")
        self.assertEqual(payload["total_rows"], 3)
        self.assertIn("table_pointer", payload)
        self.assertEqual(len(table_metadata["row_groups"]), 2)
        self.assertEqual(table_metadata["row_groups"][0]["row_start"], 0)
        self.assertEqual(table_metadata["row_groups"][0]["delete_pointers"][0]["deleted_row_ids"], [1])

    def test_table_checkpoint_deduplicates_repeated_delete_actions(self) -> None:
        table = DataTable(self.definition, row_group_size=4)
        writer = MetadataWriter()
        table_writer = SingleFileTableDataWriter(writer)
        table.append_rows([
            {"id": 1, "kind": "a", "value": 10},
            {"id": 2, "kind": "b", "value": None},
            {"id": 3, "kind": "c", "value": 30},
        ])

        table.row_groups.row_groups.nodes[0].delete_row(1)
        table.row_groups.row_groups.nodes[0].delete_row(1)

        payload = table.checkpoint(table_writer)
        table_metadata = writer.read_payload(payload["table_pointer"])

        self.assertEqual(table_metadata["row_groups"][0]["delete_pointers"], [{"deleted_row_ids": [1]}])

    def test_row_group_checkpoint_spills_to_multiple_blocks_when_segments_keep_growing(self) -> None:
        definition = TableDefinition(
            name="messages",
            columns=[ColumnDefinition("payload", str, nullable=False)],
        )
        row_group = RowGroup(definition, start=0, max_rows=32)
        row_group.columns["payload"].segment_size = 1
        row_group.append_rows([
            {"payload": f"msg-{index}-" + ("x" * 20_000)}
            for index in range(20)
        ])
        block_manager = BlockManager()
        partial_blocks = PartialBlockManager(block_manager)

        pointer = row_group.checkpoint(block_manager, partial_blocks)
        block_ids = {
            data_pointer["block_pointer"]["block_id"]
            for data_pointer in pointer.data_pointers[0]
            if data_pointer["block_pointer"]["block_id"] is not None
        }

        self.assertEqual(len(pointer.data_pointers[0]), 20)
        self.assertGreater(len(block_ids), 1)

    # def test_row_group_checkpoint_raises_when_one_segment_overflows_a_block(self) -> None:
    #     definition = TableDefinition(
    #         name="messages",
    #         columns=[ColumnDefinition("payload", str, nullable=False)],
    #     )
    #     row_group = RowGroup(definition, start=0, max_rows=4)
    #     oversized_values = [
    #         "a" * (BLOCK_SIZE // 2 + 1024),
    #         "b" * (BLOCK_SIZE // 2 + 1024),
    #     ]
    #     row_group.append_rows([
    #         {"payload": oversized_values[0]},
    #         {"payload": oversized_values[1]},
    #     ])
    #     block_manager = BlockManager()
    #     partial_blocks = PartialBlockManager(block_manager)

    #     with self.assertRaises(ValueError):
    #         row_group.checkpoint(block_manager, partial_blocks)

    # def test_single_file_table_writer_builds_catalog_facing_payload(self) -> None:
    #     writer = MetadataWriter()
    #     table_writer = SingleFileTableDataWriter(writer)

    #     # The catalog keeps one pointer to the table metadata blob rather than inlining everything.
    #     payload = table_writer.finalize_table(
    #         table_name="events",
    #         table_statistics={"row_count": 3},
    #         row_group_pointers=[],
    #     )

    #     self.assertEqual(payload["table_name"], "events")
    #     self.assertEqual(payload["table_pointer"], {"index": 0})
    #     self.assertEqual(payload["total_rows"], 0)
    #     self.assertEqual(writer.read_payload({"index": 0})["table_statistics"], {"row_count": 3})

    # def test_single_file_table_writer_rejects_overlapping_row_group_ranges(self) -> None:
    #     writer = MetadataWriter()
    #     table_writer = SingleFileTableDataWriter(writer)

    #     with self.assertRaises(ValueError):
    #         table_writer.finalize_table(
    #             table_name="events",
    #             table_statistics={"row_count": 4},
    #             row_group_pointers=[
    #                 RowGroupPointer(row_start=0, tuple_count=2),
    #                 RowGroupPointer(row_start=1, tuple_count=2),
    #             ],
    #         )


if __name__ == "__main__":
    unittest.main()
