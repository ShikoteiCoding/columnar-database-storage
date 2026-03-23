"""Segment tree skeleton.

In DuckDB both row groups and column segments are tracked by ordered structures
that map row ranges to nodes. For the exercise, represent that structure as an
ordered list plus binary search.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Generic, TypeVar


@dataclass
class SegmentBase:
    """Base range node.

    Goal for the candidate:
    - store the start row id
    - store the number of rows covered by the node
    - provide a containment helper for binary search
    """

    start: int
    count: int

    def contains_row(self, row_id: int) -> bool:
        """Return whether this segment covers `row_id`."""
        raise NotImplementedError("Question 2: implement SegmentBase.contains_row()")


T = TypeVar("T", bound=SegmentBase)


class SegmentTree(Generic[T]):
    """Ordered row-range index.

    Goal for the candidate:
    - append segments in row order
    - keep nodes sorted by `start`
    - locate a segment index with binary search
    - return the actual node through `locate()`
    """

    def __init__(self, *, supports_lazy_loading: bool = False) -> None:
        self.supports_lazy_loading = supports_lazy_loading
        self.nodes: list[T] = []
        self.finished_loading = True

    def append(self, node: T) -> None:
        """Register a new segment node."""
        raise NotImplementedError("Question 2: implement SegmentTree.append()")

    def locate_index(self, row_id: int) -> int:
        """Return the index of the node covering `row_id`."""
        raise NotImplementedError("Question 2: implement SegmentTree.locate_index()")

    def locate(self, row_id: int) -> T:
        """Return the node covering `row_id`."""
        raise NotImplementedError("Question 2: implement SegmentTree.locate()")

    def row_ranges(self) -> list[tuple[int, int]]:
        """Return `(start, count)` for every segment."""
        raise NotImplementedError("Question 2: implement SegmentTree.row_ranges()")
