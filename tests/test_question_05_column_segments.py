import unittest

from columnar_storage.blocks import BlockPointer
from columnar_storage.storage import ColumnSegment


class ColumnSegmentQuestionTests(unittest.TestCase):
    """Question 5: column segment behavior."""

    def test_segment_keeps_column_metadata_for_later_storage_decisions(self) -> None:
        segment = ColumnSegment(start=0, column_name="value", column_type=int, max_values=4)

        # The segment already knows which logical column it belongs to, and keeping the
        # declared type nearby makes later serialization and size heuristics easier to explain.
        self.assertEqual(segment.column_name, "value")
        self.assertIs(segment.column_type, int)
        self.assertEqual(segment.max_values, 4)

    def test_append_updates_count_and_statistics(self) -> None:
        segment = ColumnSegment(start=0, column_name="value", max_values=4)
        segment.append([10, 20, None])

        self.assertEqual(segment.count, 3)
        self.assertEqual(segment.statistics.min_value, 10)
        self.assertEqual(segment.statistics.max_value, 20)
        self.assertEqual(segment.statistics.null_count, 1)

    def test_append_supports_chained_batches_until_capacity(self) -> None:
        segment = ColumnSegment(start=0, column_name="value", max_values=5)

        segment.append([1, 2])
        segment.append([3, None, 5])

        self.assertEqual(segment.count, 5)
        self.assertEqual(segment.scan(), [1, 2, 3, None, 5])
        self.assertEqual(segment.statistics.min_value, 1)
        self.assertEqual(segment.statistics.max_value, 5)
        self.assertEqual(segment.statistics.null_count, 1)
        self.assertTrue(segment.is_full())

    def test_is_full_after_reaching_capacity(self) -> None:
        segment = ColumnSegment(start=0, column_name="value", max_values=2)
        segment.append([1, 2])

        self.assertTrue(segment.is_full())

    def test_append_rejects_batches_that_overflow_capacity(self) -> None:
        segment = ColumnSegment(start=0, column_name="value", max_values=3)
        segment.append([1, 2])

        # Segments are bounded row-range slices, so an append that would cross the limit should fail.
        with self.assertRaises(ValueError):
            segment.append([3, 4])

        self.assertEqual(segment.count, 2)
        self.assertEqual(segment.scan(), [1, 2])

    def test_scan_returns_requested_slice(self) -> None:
        segment = ColumnSegment(start=100, column_name="value", max_values=10)
        segment.append([5, 6, 7, 8])

        self.assertEqual(segment.scan(local_offset=1, count=2), [6, 7])

    def test_scan_defaults_to_remaining_values_and_clips_past_end(self) -> None:
        segment = ColumnSegment(start=100, column_name="value", max_values=10)
        segment.append([5, 6, 7, 8])

        self.assertEqual(segment.scan(local_offset=2), [7, 8])
        self.assertEqual(segment.scan(local_offset=1, count=10), [6, 7, 8])
        self.assertEqual(segment.scan(local_offset=4, count=2), [])

    def test_scan_iteration_can_reconstruct_segment_in_small_windows(self) -> None:
        segment = ColumnSegment(start=0, column_name="value", max_values=8)
        segment.append(list(range(8)))

        # Repeated small scans model vectorized execution reading a segment window by window.
        windows = [segment.scan(local_offset=offset, count=2) for offset in range(0, 8, 2)]

        self.assertEqual(windows, [[0, 1], [2, 3], [4, 5], [6, 7]])

    def test_estimate_size_bytes_is_stable_for_empty_segment_and_non_decreasing_after_appends(self) -> None:
        segment = ColumnSegment(start=0, column_name="value", max_values=10)

        # `estimate_size_bytes()` is not meant to be exact accounting. It is a planning heuristic
        # for checkpoint code that needs to know whether a segment stays tiny or keeps growing.
        empty_size = segment.estimate_size_bytes()
        segment.append([1, 2])
        small_size = segment.estimate_size_bytes()
        segment.append([300, 4000])
        larger_size = segment.estimate_size_bytes()

        self.assertIsInstance(empty_size, int)
        self.assertIsInstance(small_size, int)
        self.assertIsInstance(larger_size, int)
        self.assertGreaterEqual(empty_size, 0)
        self.assertGreaterEqual(small_size, empty_size)
        self.assertGreaterEqual(larger_size, small_size)

    def test_to_pointer_uses_block_pointer(self) -> None:
        segment = ColumnSegment(start=20, column_name="value", max_values=10)
        segment.append([5, 6, 7])

        # Checkpoint turns an in-memory segment into a durable address plus summary metadata.
        pointer = segment.to_pointer(BlockPointer(block_id=9, offset=12))

        self.assertEqual(pointer.row_start, 20)
        self.assertEqual(pointer.tuple_count, 3)
        self.assertEqual(pointer.block_pointer.block_id, 9)

    def test_to_pointer_for_non_constant_segment_keeps_explicit_block_location(self) -> None:
        segment = ColumnSegment(start=20, column_name="value", max_values=10)
        segment.append([5, 6])
        segment.append([7, 8])

        pointer = segment.to_pointer(BlockPointer(block_id=11, offset=32))

        self.assertEqual(pointer.row_start, 20)
        self.assertEqual(pointer.tuple_count, 4)
        self.assertEqual(pointer.block_pointer.block_id, 11)
        self.assertEqual(pointer.block_pointer.offset, 32)
        self.assertFalse(pointer.statistics.is_constant())
        self.assertIsNone(pointer.constant_value)

    def test_constant_segment_marks_constant_value(self) -> None:
        segment = ColumnSegment(start=0, column_name="value", max_values=10)
        segment.append([7, 7, 7])

        # Constant runs can be restored from metadata without paying to store duplicate values.
        pointer = segment.to_pointer(BlockPointer(block_id=None, offset=0))

        self.assertEqual(pointer.constant_value, 7)
        self.assertTrue(pointer.statistics.is_constant())

    def test_constant_segment_detection_survives_chained_equal_appends(self) -> None:
        segment = ColumnSegment(start=0, column_name="value", max_values=10)
        for batch in ([7], [7, 7], [7]):
            segment.append(batch)

        # Constant compression should still apply when the same value arrives through multiple appends.
        pointer = segment.to_pointer(BlockPointer(block_id=None, offset=0))

        self.assertEqual(segment.count, 4)
        self.assertEqual(pointer.constant_value, 7)
        self.assertTrue(pointer.statistics.is_constant())


if __name__ == "__main__":
    unittest.main()
