import unittest

from columnar_storage.segment_tree import SegmentBase, SegmentTree


class SegmentTreeQuestionTests(unittest.TestCase):
    """Question 2: segment tree lookup."""

    class TrackingList(list[SegmentBase]):
        """Count how many segment nodes a lookup inspects."""

        def __init__(self, items: list[SegmentBase]) -> None:
            super().__init__(items)
            self.access_count = 0

        def __getitem__(self, item):
            result = super().__getitem__(item)
            if isinstance(item, slice):
                self.access_count += len(result)
            else:
                self.access_count += 1
            return result

        def __iter__(self):
            for item in super().__iter__():
                self.access_count += 1
                yield item

    def test_segment_contains_row(self) -> None:
        segment = SegmentBase(start=10, count=5)

        self.assertTrue(segment.contains_row(10))
        self.assertTrue(segment.contains_row(14))
        self.assertFalse(segment.contains_row(15))
        self.assertFalse(segment.contains_row(9))

    def test_locate_index_uses_row_ranges(self) -> None:
        tree = SegmentTree()
        tree.append(SegmentBase(start=0, count=4))
        tree.append(SegmentBase(start=4, count=4))
        tree.append(SegmentBase(start=8, count=2))

        self.assertEqual(tree.locate_index(0), 0)
        self.assertEqual(tree.locate_index(6), 1)
        self.assertEqual(tree.locate_index(9), 2)

    def test_locate_index_performs_sublinear_lookup(self) -> None:
        tree = SegmentTree()
        for row_id in range(1024):
            tree.append(SegmentBase(start=row_id, count=1))

        tracked_nodes = self.TrackingList(tree.nodes)
        tree.nodes = tracked_nodes

        self.assertEqual(tree.locate_index(1023), 1023)
        self.assertLess(
            tracked_nodes.access_count,
            40,
            "locate_index() should inspect only a logarithmic number of segments",
        )

    def test_locate_returns_the_matching_node(self) -> None:
        tree = SegmentTree()
        first = SegmentBase(start=0, count=4)
        second = SegmentBase(start=4, count=4)
        tree.append(first)
        tree.append(second)

        self.assertIs(tree.locate(5), second)

    def test_row_ranges_returns_sorted_ranges(self) -> None:
        tree = SegmentTree()
        tree.append(SegmentBase(start=0, count=4))
        tree.append(SegmentBase(start=4, count=3))

        self.assertEqual(tree.row_ranges(), [(0, 4), (4, 3)])

    def test_missing_row_raises_key_error(self) -> None:
        tree = SegmentTree()
        tree.append(SegmentBase(start=0, count=2))

        with self.assertRaises(KeyError):
            tree.locate_index(4)


if __name__ == "__main__":
    unittest.main()
