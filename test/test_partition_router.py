import unittest

from common.partition_router import PartitionRouter


class TestPartitionRouter(unittest.TestCase):
    def test_partition_for_known_key(self) -> None:

        router = PartitionRouter(queue_prefix="input_batcher.input", partition_count=4)

        partition = router.partition_for("execution-123")

        self.assertEqual(partition, 3)

    def test_queue_for_includes_partition(self) -> None:

        router = PartitionRouter(queue_prefix="input_batcher.input", partition_count=4)

        queue = router.queue_for("execution-123")

        self.assertEqual(queue, "input_batcher.input.3")

    def test_rejects_non_positive_partition_count(self) -> None:

        for partition_count in (0, -1):
            with self.subTest(partition_count=partition_count):
                with self.assertRaises(ValueError):
                    PartitionRouter(
                        queue_prefix="input_batcher.input",
                        partition_count=partition_count,
                    )


if __name__ == "__main__":
    unittest.main()
