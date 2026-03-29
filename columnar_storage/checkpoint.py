"""Checkpoint skeleton.

DuckDB pushes pointers upward during checkpointing. The exercise mirrors that by
introducing explicit checkpoint state objects and an in-memory metadata writer.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .stats import BaseStatistics
from .storage import RowGroupPointer


@dataclass
class ColumnCheckpointState:
    """Intermediate checkpoint state for one column.

    Goal for the candidate:
    - collect `DataPointer` objects for one column
    - keep column-level statistics ready for parent merges
    """

    data_pointers: list[dict[str, Any]] = field(default_factory=list)
    statistics: BaseStatistics = field(default_factory=BaseStatistics)


@dataclass
class RowGroupWriteData:
    """Intermediate checkpoint state for one row group."""

    states: list[ColumnCheckpointState] = field(default_factory=list)
    statistics: list[BaseStatistics] = field(default_factory=list)


@dataclass
class CollectionCheckpointState:
    """Intermediate checkpoint state for a row-group collection."""

    row_group_pointers: list[RowGroupPointer] = field(default_factory=list)


class MetadataWriter:
    """In-memory metadata writer.

    Goal for the candidate:
    - act like a stand-in for a meta block list
    - allocate deterministic logical pointers
    - store arbitrary serializable payloads
    """

    def __init__(self) -> None:
        self.payloads: list[dict[str, Any]] = []

    def get_meta_block_pointer(self) -> dict[str, int]:
        """Return a pointer to the next metadata slot."""
        raise NotImplementedError("Question 8: implement MetadataWriter.get_meta_block_pointer()")

    def write_payload(self, payload: dict[str, Any]) -> dict[str, int]:
        """Store a payload and return its pointer."""
        raise NotImplementedError("Question 8: implement MetadataWriter.write_payload()")

    def read_payload(self, pointer: dict[str, int]) -> dict[str, Any]:
        """Read a payload by pointer."""
        raise NotImplementedError("Question 8: implement MetadataWriter.read_payload()")


class SingleFileTableDataWriter:
    """Educational stand-in for DuckDB's table metadata writer.

    Goal for the candidate:
    - write table-level summary metadata into a referenced metadata blob
    - write row-group metadata into that same referenced blob
    - return a smaller catalog-facing payload with a pointer to table metadata

    There are two metadata layers during checkpointing:
    - the catalog-facing payload returned from `finalize_table()`
    - the referenced table metadata blob written through `MetadataWriter`

    In this exercise, `table_statistics` belong to the referenced table
    metadata blob as logical table-level summaries. Lower-level physical block
    details stay attached to row groups and `DataPointer` objects.
    """

    def __init__(self, metadata_writer: MetadataWriter) -> None:
        self.metadata_writer = metadata_writer

    def finalize_table(
        self,
        *,
        table_name: str,
        table_statistics: dict[str, Any],
        row_group_pointers: list[RowGroupPointer],
    ) -> dict[str, Any]:
        """Build the final table metadata payload.

        `total_rows` in the catalog-facing payload should be derived from the
        serialized row-group pointers included in this write, while
        `table_statistics` remain stored inside the metadata blob referenced by
        `table_pointer`.
        """
        raise NotImplementedError("Question 8: implement SingleFileTableDataWriter.finalize_table()")
