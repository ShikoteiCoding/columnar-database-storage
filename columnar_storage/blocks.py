"""Block management skeleton.

The learning goal is to model how column segments become block-backed storage
through fixed-size allocations and partial block reuse.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


BLOCK_SIZE = 256 * 1024


@dataclass(frozen=True)
class BlockPointer:
    """Pointer to a payload inside a block.

    Goal for the candidate:
    - store the block id
    - store the byte offset inside the block
    - serialize symmetrically for checkpoint metadata
    """

    block_id: int | None
    offset: int = 0

    def to_dict(self) -> dict[str, Any]:
        """Serialize the pointer."""
        raise NotImplementedError("Question 3: implement BlockPointer.to_dict()")

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "BlockPointer":
        """Deserialize the pointer."""
        raise NotImplementedError("Question 3: implement BlockPointer.from_dict()")


class DataBlock:
    """One fixed-size block used for data payloads.

    Goal for the candidate:
    - append bytes into the block
    - track free space
    - return offsets for appended payloads
    """

    def __init__(self, block_id: int, capacity: int = BLOCK_SIZE) -> None:
        self.block_id = block_id
        self.capacity = capacity
        self.data = bytearray(capacity)
        self.used = 0

    def remaining_capacity(self) -> int:
        """Return remaining capacity in bytes."""
        raise NotImplementedError("Question 4: implement DataBlock.remaining_capacity()")

    def write(self, payload: bytes) -> int:
        """Write bytes and return the starting offset."""
        raise NotImplementedError("Question 4: implement DataBlock.write()")

    def read(self, offset: int, size: int) -> bytes:
        """Read `size` bytes from `offset`."""
        raise NotImplementedError("Question 4: implement DataBlock.read()")


class BlockManager:
    """Allocator and registry for data blocks.

    Goal for the candidate:
    - allocate monotonically increasing block ids
    - expose blocks by id
    - track modified blocks that could be reclaimed
    """

    def __init__(self) -> None:
        self.blocks: dict[int, DataBlock] = {}
        self.modified_blocks: set[int] = set()
        self._next_block_id = 1

    def allocate_block(self) -> DataBlock:
        """Allocate and return a new `DataBlock`."""
        raise NotImplementedError("Question 4: implement BlockManager.allocate_block()")

    def get_block(self, block_id: int) -> DataBlock:
        """Return a block by id."""
        raise NotImplementedError("Question 4: implement BlockManager.get_block()")

    def mark_block_as_modified(self, block_id: int) -> None:
        """Track a block as modified or reclaimable."""
        raise NotImplementedError("Question 4: implement BlockManager.mark_block_as_modified()")


@dataclass
class PartialBlockAllocation:
    """Result of a partial block allocation request."""

    pointer: BlockPointer
    block: DataBlock


class PartialBlockManager:
    """Pack smaller payloads into partially filled blocks.

    Goal for the candidate:
    - try to reuse existing blocks first
    - allocate a new block when needed
    - expose a simple `allocate()` API that returns a `BlockPointer`
    """

    def __init__(self, block_manager: BlockManager) -> None:
        self.block_manager = block_manager
        self.partial_blocks: list[DataBlock] = []

    def allocate(self, payload_size: int) -> PartialBlockAllocation:
        """Return a block allocation that can fit `payload_size` bytes."""
        raise NotImplementedError("Question 4: implement PartialBlockManager.allocate()")

    def register_block(self, block: DataBlock) -> None:
        """Register a block for future partial reuse."""
        raise NotImplementedError("Question 4: implement PartialBlockManager.register_block()")
