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
        return {"index": len(self.payloads)}

    def write_payload(self, payload: dict[str, Any]) -> dict[str, int]:
        """Store a payload and return its pointer."""
        self.payloads.append(payload)
        return {"index": len(self.payloads) - 1}
        

    def read_payload(self, pointer: dict[str, int]) -> dict[str, Any]:
        """Read a payload by pointer."""
        index = pointer["index"]
        if index < 0 or index >= len(self.payloads):
            raise IndexError(f"pointer '{pointer}' out of range")
        return self.payloads[pointer["index"]]


class SingleFileTableDataWriter:
    """Educational stand-in for DuckDB's table metadata writer.

    Goal for the candidate:
    - write table statistics
    - write row-group metadata
    - return a catalog-facing payload with a pointer to table metadata
    """

    def __init__(self, metadata_writer: MetadataWriter) -> None:
        self.metadata_writer = metadata_writer

    def _validate_row_group_pointers(self, row_group_pointers: list[RowGroupPointer]) -> None:
        """Reject overlapping logical row ranges before writing table metadata."""
        if not row_group_pointers:
            return

        ordered_pointers = sorted(row_group_pointers, key=lambda pointer: pointer.row_start)
        previous_end = ordered_pointers[0].row_start + ordered_pointers[0].tuple_count

        for pointer in ordered_pointers[1:]:
            if pointer.row_start < previous_end:
                raise ValueError("row group pointers must not overlap")
            previous_end = pointer.row_start + pointer.tuple_count

    def finalize_table(
        self,
        *,
        table_name: str,
        table_statistics: dict[str, Any],
        row_group_pointers: list[RowGroupPointer],
    ) -> dict[str, Any]:
        """Build the final table metadata payload."""

        self._validate_row_group_pointers(row_group_pointers)

        table_pointer = self.metadata_writer.write_payload(
            {
                "table_statistics": table_statistics,
                "row_groups": [pointer.serialize() for pointer in row_group_pointers],
            }
        )

        return {
            "table_name": table_name,
            "total_rows": sum(pointer.tuple_count for pointer in row_group_pointers),
            "table_pointer": table_pointer,
        }