import unittest

class TestResilience(unittest.TestCase):
    def test_detect(self):
        self.assertTrue(True)

    res = detect_resilience_anomaly({})
    self.assertIn('anomaly', res) 