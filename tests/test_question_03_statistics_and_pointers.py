import unittest

from columnar_storage.blocks import BlockPointer
from columnar_storage.stats import BaseStatistics
from columnar_storage.storage import DataPointer


class StatisticsAndPointersQuestionTests(unittest.TestCase):
    """Question 3: statistics and persistent pointers."""

    def test_statistics_update_tracks_min_max_nulls_and_rows(self) -> None:
        stats = BaseStatistics()
        stats.update([3, None, 9, 3])

        self.assertEqual(stats.min_value, 3)
        self.assertEqual(stats.max_value, 9)
        self.assertEqual(stats.null_count, 1)
        self.assertEqual(stats.row_count, 4)
        self.assertFalse(stats.is_constant())

    def test_statistics_support_multiple_chained_updates(self) -> None:
        stats = BaseStatistics()

        stats.update([4, 4])
        self.assertTrue(stats.is_constant())
        self.assertEqual(stats.constant_value, 4)

        stats.update([None, 2, 8])

        self.assertEqual(stats.min_value, 2)
        self.assertEqual(stats.max_value, 8)
        self.assertEqual(stats.null_count, 1)
        self.assertEqual(stats.row_count, 5)
        self.assertFalse(stats.is_constant())

    def test_statistics_detect_constant_streams(self) -> None:
        stats = BaseStatistics()
        stats.update([7, 7, 7])

        self.assertTrue(stats.is_constant())
        self.assertEqual(stats.constant_value, 7)

    def test_statistics_handle_null_only_batches(self) -> None:
        stats = BaseStatistics()
        stats.update([None, None, None])

        self.assertIsNone(stats.min_value)
        self.assertIsNone(stats.max_value)
        self.assertEqual(stats.null_count, 3)
        self.assertEqual(stats.row_count, 3)

    def test_statistics_serialize_round_trip_preserves_constant_state(self) -> None:
        stats = BaseStatistics()
        stats.update([7, 7, 7])

        restored = BaseStatistics.deserialize(stats.serialize())

        self.assertEqual(restored.min_value, 7)
        self.assertEqual(restored.max_value, 7)
        self.assertEqual(restored.null_count, 0)
        self.assertEqual(restored.row_count, 3)
        self.assertTrue(restored.is_constant())
        self.assertEqual(restored.constant_value, 7)

    def test_statistics_merge_combines_two_segments(self) -> None:
        left = BaseStatistics()
        right = BaseStatistics()
        left.update([1, 2, None])
        right.update([5, 9])

        left.merge(right)

        self.assertEqual(left.min_value, 1)
        self.assertEqual(left.max_value, 9)
        self.assertEqual(left.null_count, 1)
        self.assertEqual(left.row_count, 5)

    def test_statistics_merge_breaks_constant_when_values_disagree(self) -> None:
        left = BaseStatistics()
        right = BaseStatistics()
        left.update([7, 7])
        right.update([7, 9])

        left.merge(right)

        self.assertEqual(left.min_value, 7)
        self.assertEqual(left.max_value, 9)
        self.assertEqual(left.row_count, 4)
        self.assertFalse(left.is_constant())

    def test_block_pointer_round_trip(self) -> None:
        pointer = BlockPointer(block_id=11, offset=128)

        restored = BlockPointer.deserialize(pointer.serialize())

        self.assertEqual(restored, pointer)

    def test_data_pointer_round_trip(self) -> None:
        stats = BaseStatistics()
        stats.update([10, 12, 14])
        pointer = DataPointer(
            row_start=20,
            tuple_count=3,
            block_pointer=BlockPointer(block_id=7, offset=64),
            statistics=stats,
            compression_type="uncompressed",
        )

        restored = DataPointer.deserialize(pointer.serialize())

        self.assertEqual(restored.row_start, 20)
        self.assertEqual(restored.tuple_count, 3)
        self.assertEqual(restored.block_pointer.block_id, 7)
        self.assertEqual(restored.statistics.max_value, 14)

    def test_data_pointer_round_trip_for_constant_segment_without_block(self) -> None:
        stats = BaseStatistics()
        stats.update([7, 7, 7])
        pointer = DataPointer(
            row_start=0,
            tuple_count=3,
            block_pointer=BlockPointer(block_id=None, offset=0),
            statistics=stats,
            compression_type="constant",
            constant_value=7,
        )

        restored = DataPointer.deserialize(pointer.serialize())

        self.assertIsNone(restored.block_pointer.block_id)
        self.assertEqual(restored.constant_value, 7)
        self.assertEqual(restored.statistics.constant_value, 7)
        self.assertTrue(restored.statistics.is_constant())


if __name__ == "__main__":
    unittest.main()
