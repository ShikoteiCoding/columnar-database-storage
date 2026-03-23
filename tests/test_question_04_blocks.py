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

        first = partials.allocate(8)
        first.pointer.block_id
        first.block.write(b"12345678")
        partials.register_block(first.block)

        second = partials.allocate(4)

        self.assertEqual(second.pointer.block_id, first.pointer.block_id)
        self.assertEqual(second.pointer.offset, 8)

    def test_large_payload_can_force_new_block(self) -> None:
        manager = BlockManager()
        partials = PartialBlockManager(manager)

        first = partials.allocate(BLOCK_SIZE - 2)
        first.block.write(b"x" * (BLOCK_SIZE - 2))
        partials.register_block(first.block)

        second = partials.allocate(16)

        self.assertNotEqual(second.pointer.block_id, first.pointer.block_id)

    def test_mark_block_as_modified_tracks_reclaim_candidates(self) -> None:
        manager = BlockManager()
        block = manager.allocate_block()

        manager.mark_block_as_modified(block.block_id)

        self.assertIn(block.block_id, manager.modified_blocks)


if __name__ == "__main__":
    unittest.main()
