"""Storage-layer skeleton.

This module mirrors the educational hierarchy:

`DataTable -> RowGroupCollection -> RowGroup -> ColumnData -> ColumnSegment`
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any

from .blocks import BlockManager, BlockPointer, PartialBlockManager
from .catalog import ColumnDefinition, TableDefinition
from .segment_tree import SegmentBase, SegmentTree
from .stats import BaseStatistics


@dataclass
class DataPointer:
    """Persistent metadata for one column segment.

    Goal for the candidate:
    - capture row range, block pointer, compression tag, and statistics
    - support serialization for checkpoint metadata
    - represent constant segments without a block id
    """

    row_start: int
    tuple_count: int
    block_pointer: BlockPointer
    statistics: BaseStatistics
    compression_type: str = "uncompressed"
    constant_value: Any = None

    def serialize(self) -> dict[str, Any]:
        """Serialize the data pointer."""
        return self.__dict__

    @classmethod
    def deserialize(cls, payload: dict[str, Any]) -> DataPointer:
        """Deserialize the data pointer."""
        return DataPointer(**payload)


class VersionInfo:
    """Deletion metadata for a row group.

    Goal for the candidate:
    - track deleted row ids
    - keep delete metadata serializable
    - let scans skip deleted rows
    """

    def __init__(self) -> None:
        self.deleted_row_ids: set[int] = set()

    def mark_deleted(self, row_id: int) -> None:
        """Mark one absolute row id as deleted."""
        raise NotImplementedError("Question 6: implement VersionInfo.mark_deleted()")

    def is_deleted(self, row_id: int) -> bool:
        """Return whether the row id is deleted."""
        raise NotImplementedError("Question 6: implement VersionInfo.is_deleted()")

    def serialize(self) -> dict[str, Any]:
        """Serialize delete metadata."""
        raise NotImplementedError("Question 8: implement VersionInfo.serialize()")


class ColumnSegment(SegmentBase):
    """Contiguous slice of values for one column.

    Goal for the candidate:
    - append values into the segment
    - retain lightweight column metadata such as the declared Python type
    - keep statistics up to date
    - estimate how large the serialized payload would be before allocating a block
    - build `DataPointer` metadata
    """

    def __init__(
        self,
        start: int,
        column_name: str,
        max_values: int = 2048,
        column_type: type | None = None,
    ) -> None:
        super().__init__(start=start, count=0)
        self.column_name = column_name
        self.column_type = column_type
        self.max_values = max_values
        self.values: list[Any] = []
        self.statistics = BaseStatistics()
        self._payload_size_bytes = 0

    def append(self, values: list[Any]) -> None:
        """Append values into the segment."""
        if len(values) + len(self.values) > self.max_values:
            raise ValueError(f"Not able to append to column segment")
        self._payload_size_bytes += self._estimate_batch_payload_bytes(values)
        self.statistics.update(values)
        self.values.extend(values)
        self.count += len(values)

    def is_full(self) -> bool:
        """Return whether the segment reached its logical capacity."""
        return len(self.values) == self.max_values

    def estimate_size_bytes(self) -> int:
        """Return a rough serialized payload size for checkpoint planning.

        The estimate does not need to be exact. It exists so later checkpoint code
        can decide whether a segment is tiny, whether it should be packed into a
        partial block, or whether constant-segment metadata alone is sufficient.
        """
        if self.count == 0:
            return 0

        null_bitmap_bytes = (self.count + 7) // 8

        # Constant segments are cheap to persist as metadata only.
        if self.statistics.is_constant():
            return null_bitmap_bytes + self._estimate_single_value_size(self.statistics.constant_value)

        return null_bitmap_bytes + self._payload_size_bytes

    def _estimate_batch_payload_bytes(self, values: list[Any]) -> int:
        """Estimate bytes contributed by one appended batch.

        This keeps `estimate_size_bytes()` cheap by doing any necessary work once
        at append time instead of rescanning the whole segment later.
        """
        non_null_values = [value for value in values if value is not None]

        if not non_null_values:
            return 0

        if self.column_type is bool:
            return len(non_null_values)

        if self.column_type in (int, float):
            return 8 * len(non_null_values)

        if self.column_type is str:
            return sum(len(value.encode("utf-8")) for value in non_null_values)

        return sum(self._estimate_single_value_size(value) for value in non_null_values)

    def _estimate_single_value_size(self, value: Any) -> int:
        """Estimate bytes for one logical value in persisted form."""
        if value is None:
            return 0

        if self.column_type is bool or isinstance(value, bool):
            return 1

        if self.column_type in (int, float):
            return 8

        if self.column_type is str or isinstance(value, str):
            return len(value.encode("utf-8"))

        if isinstance(value, (int, float)):
            return 8

        return len(str(value).encode("utf-8"))

    def scan(self, local_offset: int = 0, count: int | None = None) -> list[Any]:
        """Read a slice of values from the segment."""
        if not count:
            return self.values[local_offset:]
        return self.values[local_offset:local_offset+count]

    def to_pointer(self, block_pointer: BlockPointer) -> DataPointer:
        """Create a `DataPointer` for this segment."""
        return DataPointer(
            self.start,
            self.count,
            block_pointer,
            self.statistics,
            constant_value=self.statistics.constant_value
        )


class ColumnData:
    """All segment data for one logical column inside one row group.

    Goal for the candidate:
    - own a `SegmentTree` of `ColumnSegment` nodes
    - append values while creating new segments when needed
    - scan values by absolute row range
    - checkpoint into `DataPointer` metadata
    """

    def __init__(self, definition: ColumnDefinition, row_group_start: int, segment_size: int = 2048) -> None:
        self.definition = definition
        self.row_group_start = row_group_start
        self.segment_size = segment_size
        self.segment_tree: SegmentTree[ColumnSegment] = SegmentTree()

    def append(self, values: list[Any]) -> None:
        """Append a batch of values for this column."""
        segment = ColumnSegment(
            # not sure at all here, i don't know where is row_id is tracked
            # should I use self.row_group_start ?
            0, 
            self.definition.name,
            column_type=self.definition.python_type
        )
        self.segment_tree.append(segment)

    def scan(self, row_start: int, count: int) -> list[Any]:
        """Return values for the requested absolute row range."""
        start = self.segment_tree.locate_index(row_start)
        end = self.segment_tree.locate_index(row_start + count)
        pass

    def checkpoint(self, block_manager: BlockManager, partial_blocks: PartialBlockManager) -> list[DataPointer]:
        """Persist segments and return metadata pointers."""
        raise NotImplementedError("Question 8: implement ColumnData.checkpoint()")


@dataclass
class RowGroupPointer:
    """Persistent metadata for one row group."""

    row_start: int
    tuple_count: int
    data_pointers: list[list[dict[str, Any]]] = field(default_factory=list)
    delete_pointers: list[dict[str, Any]] = field(default_factory=list)

    def serialize(self) -> dict[str, Any]:
        """Serialize the row group pointer."""
        raise NotImplementedError("Question 8: implement RowGroupPointer.serialize()")


class RowGroup(SegmentBase):
    """Horizontal partition of a table.

    Goal for the candidate:
    - own one `ColumnData` per table column
    - append rows in columnar form
    - expose scans that reconstruct row dictionaries
    - keep deletion metadata in `VersionInfo`
    """

    def __init__(self, definition: TableDefinition, start: int, max_rows: int = 122_880) -> None:
        super().__init__(start=start, count=0)
        self.definition = definition
        self.max_rows = max_rows
        self.version_info = VersionInfo()
        self.columns = {
            column.name: ColumnData(column, row_group_start=start)
            for column in definition.columns
        }

    def append_rows(self, rows: list[dict[str, Any]]) -> None:
        """Append row dictionaries into the row group."""
        self.count += len(rows)
        col_to_list = defaultdict(list)

        # pivot as columns
        for row in rows:
            for col_name, value in row.items():
                col_to_list[col_name].append(value)

        for col_name, values in col_to_list.items():
            self.columns[col_name].append(values)


    def is_full(self) -> bool:
        """Return whether the row group reached capacity."""
        return self.count == self.max_rows

    def scan_rows(self, row_start: int, count: int) -> list[dict[str, Any]]:
        """Reconstruct rows from columnar storage."""
        raise NotImplementedError("Question 6: implement RowGroup.scan_rows()")

    def delete_row(self, row_id: int) -> None:
        """Mark one absolute row id as deleted."""
        raise NotImplementedError("Question 6: implement RowGroup.delete_row()")

    def checkpoint(self, block_manager: BlockManager, partial_blocks: PartialBlockManager) -> RowGroupPointer:
        """Checkpoint this row group into row-group metadata."""
        raise NotImplementedError("Question 8: implement RowGroup.checkpoint()")


class RowGroupCollection:
    """Collection of row groups for one table.

    Goal for the candidate:
    - create new row groups when appending beyond capacity
    - use a `SegmentTree` to locate the owning row group for scans
    - coordinate row-group checkpointing
    """

    def __init__(self, definition: TableDefinition, row_group_size: int = 122_880) -> None:
        self.definition = definition
        self.row_group_size = row_group_size
        self.row_groups: SegmentTree[RowGroup] = SegmentTree()

    def append_rows(self, rows: list[dict[str, Any]]) -> None:
        """Append rows across one or more row groups."""
        raise NotImplementedError("Question 7: implement RowGroupCollection.append_rows()")

    def scan_rows(self, row_start: int, count: int) -> list[dict[str, Any]]:
        """Read rows across row-group boundaries."""
        raise NotImplementedError("Question 7: implement RowGroupCollection.scan_rows()")

    def total_rows(self) -> int:
        """Return the total number of visible rows including deleted ones."""
        raise NotImplementedError("Question 7: implement RowGroupCollection.total_rows()")

    def checkpoint(self, block_manager: BlockManager, partial_blocks: PartialBlockManager) -> list[RowGroupPointer]:
        """Checkpoint all row groups."""
        raise NotImplementedError("Question 8: implement RowGroupCollection.checkpoint()")


class DataTable:
    """Physical table storage entry point.

    Goal for the candidate:
    - own a `RowGroupCollection`
    - append rows
    - scan rows
    - produce final table checkpoint metadata
    """

    def __init__(self, definition: TableDefinition, row_group_size: int = 122_880) -> None:
        self.definition = definition
        self.row_groups = RowGroupCollection(definition, row_group_size=row_group_size)
        self.block_manager = BlockManager()
        self.partial_blocks = PartialBlockManager(self.block_manager)

    def append_rows(self, rows: list[dict[str, Any]]) -> None:
        """Append rows into the table."""
        raise NotImplementedError("Question 7: implement DataTable.append_rows()")

    def scan_rows(self, row_start: int, count: int) -> list[dict[str, Any]]:
        """Read rows from the table."""
        raise NotImplementedError("Question 7: implement DataTable.scan_rows()")

    def checkpoint(self) -> dict[str, Any]:
        """Return a simplified table metadata payload."""
        raise NotImplementedError("Question 8: implement DataTable.checkpoint()")
