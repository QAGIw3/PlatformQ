import unittest

from app.main import trust_search

class TestTrustSearch(unittest.TestCase):
    def test_search(self):
        res = trust_search('test', 0.5)
        self.assertIsInstance(res, list) 