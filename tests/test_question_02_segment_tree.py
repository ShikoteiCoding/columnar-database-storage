import unittest

from columnar_storage.segment_tree import SegmentBase, SegmentTree


class SegmentTreeQuestionTests(unittest.TestCase):
    """Question 2: segment tree lookup."""

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
