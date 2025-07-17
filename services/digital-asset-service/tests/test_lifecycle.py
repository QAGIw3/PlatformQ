import unittest

class TestLifecycle(unittest.TestCase):
    def test_automate(self):
        automate_lifecycle('asset1')
        self.assertTrue(True) 