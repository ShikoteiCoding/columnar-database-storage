"""Storage-layer skeleton.

This module mirrors the educational hierarchy:

`DataTable -> RowGroupCollection -> RowGroup -> ColumnData -> ColumnSegment`
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from .blocks import BlockManager, BlockPointer, PartialBlockManager
from .catalog import ColumnDefinition, TableDefinition
from .segment_tree import SegmentBase, SegmentTree
from .stats import BaseStatistics

if TYPE_CHECKING:
    from .checkpoint import SingleFileTableDataWriter


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
        raise NotImplementedError("Question 3: implement DataPointer.serialize()")

    @classmethod
    def deserialize(cls, payload: dict[str, Any]) -> "DataPointer":
        """Deserialize the data pointer."""
        raise NotImplementedError("Question 3: implement DataPointer.deserialize()")


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

    def append(self, values: list[Any]) -> None:
        """Append values into the segment."""
        raise NotImplementedError("Question 5: implement ColumnSegment.append()")

    def is_full(self) -> bool:
        """Return whether the segment reached its logical capacity."""
        raise NotImplementedError("Question 5: implement ColumnSegment.is_full()")

    def estimate_size_bytes(self) -> int:
        """Return a rough serialized payload size for checkpoint planning.

        The estimate does not need to be exact. It exists so later checkpoint code
        can decide whether a segment is tiny, whether it should be packed into a
        partial block, or whether constant-segment metadata alone is sufficient.
        """
        raise NotImplementedError("Question 5: implement ColumnSegment.estimate_size_bytes()")

    def scan(self, local_offset: int = 0, count: int | None = None) -> list[Any]:
        """Read a slice of values from the segment."""
        raise NotImplementedError("Question 5: implement ColumnSegment.scan()")

    def to_pointer(self, block_pointer: BlockPointer | None = None) -> DataPointer:
        """Create a `DataPointer` for this segment."""
        raise NotImplementedError("Question 5: implement ColumnSegment.to_pointer()")


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
        raise NotImplementedError("Question 6: implement ColumnData.append()")

    def scan(self, row_start: int, count: int) -> list[Any]:
        """Return values for the requested absolute row range."""
        raise NotImplementedError("Question 6: implement ColumnData.scan()")

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
        raise NotImplementedError("Question 6: implement RowGroup.append_rows()")

    def is_full(self) -> bool:
        """Return whether the row group reached capacity."""
        raise NotImplementedError("Question 6: implement RowGroup.is_full()")

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
    - delegate final table metadata writing to a provided checkpoint writer
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

    def checkpoint(self, table_data_writer: "SingleFileTableDataWriter") -> dict[str, Any]:
        """Checkpoint row groups and delegate final metadata writing."""
        raise NotImplementedError("Question 8: implement DataTable.checkpoint()")
