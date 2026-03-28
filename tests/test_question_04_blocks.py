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

    def test_data_block_rejects_overflowing_writes(self) -> None:
        manager = BlockManager()
        block = manager.allocate_block()

        block.write(b"x" * BLOCK_SIZE)

        with self.assertRaises(ValueError):
            block.write(b"!")

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

        # The allocator returns a precise `(block_id, offset)` slot, so the bytes are addressable rather than random.
        first = partials.allocate(8)
        first.pointer.block_id
        first.block.write(b"12345678")
        # Registering the block advertises only its contiguous unused tail for later packing.
        partials.register_block(first.block)

        # The next write reuses that known tail to avoid wasting the rest of the same block.
        second = partials.allocate(4)

        self.assertEqual(second.pointer.block_id, first.pointer.block_id)
        self.assertEqual(second.pointer.offset, 8)

    def test_partial_block_manager_chains_reuse_consistently(self) -> None:
        manager = BlockManager()
        partials = PartialBlockManager(manager)

        first = partials.allocate(5)
        first.block.write(b"hello")
        # This models multiple small column segments being packed back-to-back inside one block.
        partials.register_block(first.block)

        second = partials.allocate(4)
        second.block.write(b"data")
        partials.register_block(second.block)

        # Reusing the same block keeps free space contiguous instead of fragmenting it across many blocks.
        third = partials.allocate(3)

        self.assertEqual(second.pointer.block_id, first.pointer.block_id)
        self.assertEqual(second.pointer.offset, 5)
        self.assertEqual(third.pointer.block_id, first.pointer.block_id)
        self.assertEqual(third.pointer.offset, 9)
        self.assertEqual(first.block.read(0, 9), b"hellodata")

    def test_large_payload_can_force_new_block(self) -> None:
        manager = BlockManager()
        partials = PartialBlockManager(manager)

        first = partials.allocate(BLOCK_SIZE - 2)
        first.block.write(b"x" * (BLOCK_SIZE - 2))
        # Only the tiny tail is reusable, so a larger payload must go elsewhere.
        partials.register_block(first.block)

        # A nearly full block should not be reused for a payload that no longer fits safely.
        second = partials.allocate(16)

        self.assertNotEqual(second.pointer.block_id, first.pointer.block_id)

    def test_partial_block_manager_allocates_new_block_after_reuse_overflow(self) -> None:
        manager = BlockManager()
        partials = PartialBlockManager(manager)

        first = partials.allocate(BLOCK_SIZE - 4)
        first.block.write(b"x" * (BLOCK_SIZE - 4))
        partials.register_block(first.block)

        # The second allocation consumes the exact remaining tail in that block.
        second = partials.allocate(4)
        second.block.write(b"yyyy")
        partials.register_block(second.block)

        # Once reuse consumes the last bytes, the following write must spill into a fresh block.
        third = partials.allocate(1)

        self.assertEqual(second.pointer.block_id, first.pointer.block_id)
        self.assertEqual(second.pointer.offset, BLOCK_SIZE - 4)
        self.assertNotEqual(third.pointer.block_id, first.pointer.block_id)
        self.assertEqual(second.block.remaining_capacity(), 0)

    def test_mark_block_as_modified_tracks_reclaim_candidates(self) -> None:
        manager = BlockManager()
        block = manager.allocate_block()

        # Update-heavy systems track dirty blocks so checkpoint or vacuum can revisit them later.
        manager.mark_block_as_modified(block.block_id)

        self.assertIn(block.block_id, manager.modified_blocks)


if __name__ == "__main__":
    unittest.main()
