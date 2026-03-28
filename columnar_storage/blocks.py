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

    def serialize(self) -> dict[str, Any]:
        """Serialize the pointer."""
        return self.__dict__

    @classmethod
    def deserialize(cls, payload: dict[str, Any]) -> BlockPointer:
        """Deserialize the pointer."""
        return BlockPointer(**payload)


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
        self.data = bytearray(capacity) # pre allocated data "empty"
        self.used = 0

    def remaining_capacity(self) -> int:
        """Return remaining capacity in bytes."""
        return self.capacity - self.used

    def write(self, payload: bytes) -> int:
        """Write bytes and return the starting offset."""
        prev_offset = self.used
        size = len(payload)
        
        if size > self.remaining_capacity():
            raise ValueError(f"Block is full")

        self.data[prev_offset: prev_offset + size] = payload
        self.used += len(payload)
        return prev_offset

    def read(self, offset: int, size: int) -> bytes:
        """Read `size` bytes from `offset`."""
        return self.data[offset:offset+size]


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
        new_data_block = DataBlock(self._next_block_id)
        self.blocks[self._next_block_id] = new_data_block
        self._next_block_id += 1
        return new_data_block

    def get_block(self, block_id: int) -> DataBlock:
        """Return a block by id."""
        return self.blocks[block_id]

    def mark_block_as_modified(self, block_id: int) -> None:
        """Track a block as modified or reclaimable."""
        self.modified_blocks.add(block_id)


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
        for block in self.partial_blocks:
            if block.remaining_capacity() >= payload_size:
                block_pointer = BlockPointer(block.block_id, block.used)
                return PartialBlockAllocation(block_pointer, block)
        
        new_block = self.block_manager.allocate_block()
        new_block_pointer = BlockPointer(new_block.block_id, new_block.used)
        return PartialBlockAllocation(new_block_pointer, new_block)

    def register_block(self, block: DataBlock) -> None:
        """Register a block for future partial reuse."""
        self.partial_blocks.append(block)
