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

    def test_statistics_detect_constant_streams(self) -> None:
        stats = BaseStatistics()
        stats.update([7, 7, 7])

        self.assertTrue(stats.is_constant())
        self.assertEqual(stats.constant_value, 7)

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

    def test_block_pointer_round_trip(self) -> None:
        pointer = BlockPointer(block_id=11, offset=128)

        restored = BlockPointer.from_dict(pointer.to_dict())

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

        restored = DataPointer.from_dict(pointer.to_dict())

        self.assertEqual(restored.row_start, 20)
        self.assertEqual(restored.tuple_count, 3)
        self.assertEqual(restored.block_pointer.block_id, 7)
        self.assertEqual(restored.statistics.max_value, 14)


if __name__ == "__main__":
    unittest.main()
