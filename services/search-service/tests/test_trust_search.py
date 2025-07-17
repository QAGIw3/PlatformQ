import unittest

class TestTrustSearch(unittest.TestCase):
    def test_search(self):
        self.assertTrue(True)

    res = trust_search('test', 0.5)
    self.assertIsInstance(res, list) 