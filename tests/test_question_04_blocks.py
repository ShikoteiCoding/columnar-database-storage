import unittest

from columnar_storage.blocks import BLOCK_SIZE, BlockManager, PartialBlockManager


class BlocksQuestionTests(unittest.TestCase):
    """Question 4: data blocks and partial block reuse."""

    def test_data_block_writes_and_reads_payload(self) -> None:
        manager = BlockManager()
        block = manager.allocate_block()
        offset = block.write(b"abc")

        self.assertEqual(offset, 0)
        self.assertEqual(block.read(0, 3), b"abc")
        self.assertEqual(block.remaining_capacity(), BLOCK_SIZE - 3)

    def test_data_block_supports_chained_writes_consistently(self) -> None:
        manager = BlockManager()
        block = manager.allocate_block()

        first_offset = block.write(b"ab")
        second_offset = block.write(b"cdef")
        third_offset = block.write(b"ghi")

        self.assertEqual(first_offset, 0)
        self.assertEqual(second_offset, 2)
        self.assertEqual(third_offset, 6)
        self.assertEqual(block.read(first_offset, 2), b"ab")
        self.assertEqual(block.read(second_offset, 4), b"cdef")
        self.assertEqual(block.read(third_offset, 3), b"ghi")
        self.assertEqual(block.read(0, 9), b"abcdefghi")
        self.assertEqual(block.remaining_capacity(), BLOCK_SIZE - 9)

    def test_data_block_reserve_tracks_offsets_without_requiring_a_payload(self) -> None:
        manager = BlockManager()
        block = manager.allocate_block()

        first_offset = block.reserve(3)
        second_offset = block.reserve(5)

        self.assertEqual(first_offset, 0)
        self.assertEqual(second_offset, 3)
        self.assertEqual(block.remaining_capacity(), BLOCK_SIZE - 8)

    def test_data_block_rejects_overflowing_writes(self) -> None:
        manager = BlockManager()
        block = manager.allocate_block()

        block.write(b"x" * BLOCK_SIZE)

        with self.assertRaises(ValueError):
            block.write(b"!")

    def test_data_block_rejects_overflowing_reservations(self) -> None:
        manager = BlockManager()
        block = manager.allocate_block()

        block.reserve(BLOCK_SIZE)

        with self.assertRaises(ValueError):
            block.reserve(1)

    def test_block_manager_allocates_monotonic_ids(self) -> None:
        manager = BlockManager()
        first = manager.allocate_block()
        second = manager.allocate_block()

        self.assertEqual(first.block_id, 1)
        self.assertEqual(second.block_id, 2)
        self.assertIs(manager.get_block(2), second)

    def test_partial_block_manager_reuses_existing_partial_block(self) -> None:
        manager = BlockManager()
        partials = PartialBlockManager(manager)

        # Reserving a payload consumes only the requested prefix and keeps the tail available for reuse.
        first = partials.reserve(8)

        # The next write reuses that known tail to avoid wasting the rest of the same block.
        second = partials.reserve(4)

        self.assertEqual(second.block_id, first.block_id)
        self.assertEqual(second.offset, 8)

    def test_partial_block_manager_chains_reuse_consistently(self) -> None:
        manager = BlockManager()
        partials = PartialBlockManager(manager)

        first = partials.reserve(5)
        # This models multiple small column segments being packed back-to-back inside one block.
        second = partials.reserve(4)

        # Reusing the same block keeps free space contiguous instead of fragmenting it across many blocks.
        third = partials.reserve(3)

        self.assertEqual(second.block_id, first.block_id)
        self.assertEqual(second.offset, 5)
        self.assertEqual(third.block_id, first.block_id)
        self.assertEqual(third.offset, 9)
        self.assertEqual(len(partials.partial_blocks), 1)

    def test_partial_block_allocation_reserve_consumes_tentative_slot(self) -> None:
        manager = BlockManager()
        partials = PartialBlockManager(manager)

        allocation = partials.allocate(8)
        pointer = allocation.reserve(8)

        self.assertEqual(pointer, allocation.pointer)
        self.assertEqual(pointer.offset, 0)
        self.assertEqual(allocation.block.remaining_capacity(), BLOCK_SIZE - 8)

    def test_large_payload_can_force_new_block(self) -> None:
        manager = BlockManager()
        partials = PartialBlockManager(manager)

        first = partials.reserve(BLOCK_SIZE - 2)
        # Only the tiny tail is reusable, so a larger payload must go elsewhere.

        # A nearly full block should not be reused for a payload that no longer fits safely.
        second = partials.reserve(16)

        self.assertNotEqual(second.block_id, first.block_id)

    def test_partial_block_manager_allocates_new_block_after_reuse_overflow(self) -> None:
        manager = BlockManager()
        partials = PartialBlockManager(manager)

        first = partials.reserve(BLOCK_SIZE - 4)

        # The second allocation consumes the exact remaining tail in that block.
        second = partials.reserve(4)

        # Once reuse consumes the last bytes, the following write must spill into a fresh block.
        third = partials.reserve(1)

        first_block = manager.get_block(first.block_id)

        self.assertEqual(second.block_id, first.block_id)
        self.assertEqual(second.offset, BLOCK_SIZE - 4)
        self.assertNotEqual(third.block_id, first.block_id)
        self.assertEqual(first_block.remaining_capacity(), 0)

    def test_partial_block_manager_rejects_one_payload_larger_than_a_block(self) -> None:
        manager = BlockManager()
        partials = PartialBlockManager(manager)

        # At this stage the candidate only knows about block-sized payload packing,
        # so a single payload larger than one block should fail immediately.
        with self.assertRaises(ValueError):
            partials.reserve(BLOCK_SIZE + 1)

    def test_register_block_skips_duplicates_and_full_blocks(self) -> None:
        manager = BlockManager()
        partials = PartialBlockManager(manager)
        block = manager.allocate_block()

        block.write(b"abc")
        partials.register_block(block)
        partials.register_block(block)

        self.assertEqual(partials.partial_blocks.count(block), 1)

        block.write(b"x" * (BLOCK_SIZE - 3))
        partials.register_block(block)

        self.assertNotIn(block, partials.partial_blocks)

    def test_mark_block_as_modified_tracks_reclaim_candidates(self) -> None:
        manager = BlockManager()
        block = manager.allocate_block()

        # Update-heavy systems track dirty blocks so checkpoint or vacuum can revisit them later.
        manager.mark_block_as_modified(block.block_id)

        self.assertIn(block.block_id, manager.modified_blocks)


if __name__ == "__main__":
    unittest.main()
