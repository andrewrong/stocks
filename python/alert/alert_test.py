import unittest
from alert.alert_func import threshold, direction_threshold


class TestThreshold(unittest.TestCase):
    def test_threshold_true(self):
        a = [1, 2, 3]
        b = [0, 1, 2]
        exp = 'A > B'
        result, idx = threshold(a, b, exp)
        self.assertTrue(result)
        self.assertEqual(idx, 0)

    def test_threshold_false(self):
        a = [1, 2, 3]
        b = [0, 1, 2]
        exp = 'A < B'
        result, idx = threshold(a, b, exp)
        self.assertFalse(result)
        self.assertEqual(idx, 0)

        a = [1, -2, 3]
        b = [0, 1, 2]
        exp = 'A < B'
        result, idx = threshold(a, b, exp)
        self.assertTrue(result)
        self.assertEqual(idx, 1)

    def test_threshold_equal(self):
        a = [1, 2, 3]
        b = [0, 2, 2]
        exp = 'A <= B'
        result, idx = threshold(a, b, exp)
        self.assertTrue(result)
        self.assertEqual(idx, 1)

    def test_threshold_index(self):
        a = [1, 2, 3]
        b = [2, 3, 2]
        exp = 'A >= B'
        _, index = threshold(a, b, exp)
        self.assertEqual(index, 2)


class TestDirectionThreshold(unittest.TestCase):
    def test_direction_threshold_a_down_b(self):
        a = [3, 2, 1]
        b = [1, 2, 3]
        exp = 'A down B'
        result, index = direction_threshold(a, b, exp)
        self.assertTrue(result)
        self.assertEqual(index, 1)

    def test_direction_threshold_b_down_a(self):
        a = [1, 2, 3]
        b = [3, 2, 1]
        exp = 'A down B'
        result, index = direction_threshold(a, b, exp)
        self.assertFalse(result)
        self.assertEqual(index, 0)

    def test_direction_threshold_equal(self):
        a = [1, 2, 3]
        b = [1, 2, 3]
        exp = 'A down B'
        result, index = direction_threshold(a, b, exp)
        self.assertFalse(result)
        self.assertEqual(index, 0)

    def test_direction_threshold_equal_2(self):
        a = [2, 3, 4]
        b = [1, 2, 3]
        exp = 'A down B'
        result, index = direction_threshold(a, b, exp)
        self.assertFalse(result)
        self.assertEqual(index, 0)

    def test_direction_threshold_empty_lists(self):
        a = []
        b = []
        exp = 'A down B'
        result, index = direction_threshold(a, b, exp)
        self.assertFalse(result)
        self.assertEqual(index, 0)


if __name__ == '__main__':
    unittest.main()
