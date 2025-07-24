import unittest
from excercises.count_highest import count

class TestCounter(unittest.TestCase):
    def test_count_basic(self):
        self.assertEqual(count([3,4,5,2,3,8,6,4,5]), 4)

    def test_count_empty(self):
        self.assertEqual(count([]), 0)

    def test_count_all_increasing(self):
        self.assertEqual(count([1,2,3,4,5]), 5)

    def test_count_all_same(self):
        self.assertEqual(count([2,2,2,2]), 1)

    def test_count_decreasing(self):
        self.assertEqual(count([5,4,3,2,1]), 1)

if __name__ == "__main__":
    unittest.main()
