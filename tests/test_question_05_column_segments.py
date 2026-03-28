import unittest

from columnar_storage.blocks import BlockPointer
from columnar_storage.storage import ColumnSegment


class ColumnSegmentQuestionTests(unittest.TestCase):
    """Question 5: column segment behavior."""

    def test_append_updates_count_and_statistics(self) -> None:
        segment = ColumnSegment(start=0, column_name="value", max_values=4)
        segment.append([10, 20, None])

        self.assertEqual(segment.count, 3)
        self.assertEqual(segment.statistics.min_value, 10)
        self.assertEqual(segment.statistics.max_value, 20)
        self.assertEqual(segment.statistics.null_count, 1)

    def test_is_full_after_reaching_capacity(self) -> None:
        segment = ColumnSegment(start=0, column_name="value", max_values=2)
        segment.append([1, 2])

        self.assertTrue(segment.is_full())

    def test_scan_returns_requested_slice(self) -> None:
        segment = ColumnSegment(start=100, column_name="value", max_values=10)
        segment.append([5, 6, 7, 8])

        self.assertEqual(segment.scan(local_offset=1, count=2), [6, 7])

    def test_to_pointer_uses_block_pointer(self) -> None:
        segment = ColumnSegment(start=20, column_name="value", max_values=10)
        segment.append([5, 6, 7])

        # Checkpoint turns an in-memory segment into a durable address plus summary metadata.
        pointer = segment.to_pointer(BlockPointer(block_id=9, offset=12))

        self.assertEqual(pointer.row_start, 20)
        self.assertEqual(pointer.tuple_count, 3)
        self.assertEqual(pointer.block_pointer.block_id, 9)

    def test_constant_segment_marks_constant_value(self) -> None:
        segment = ColumnSegment(start=0, column_name="value", max_values=10)
        segment.append([7, 7, 7])

        # Constant runs can be restored from metadata without paying to store duplicate values.
        pointer = segment.to_pointer(BlockPointer(block_id=None, offset=0))

        self.assertEqual(pointer.constant_value, 7)
        self.assertTrue(pointer.statistics.is_constant())


if __name__ == "__main__":
    unittest.main()
