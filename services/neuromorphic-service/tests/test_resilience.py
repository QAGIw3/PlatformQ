import unittest

from app.main import detect_resilience_anomaly

class TestResilience(unittest.TestCase):
    def test_detect(self):
        res = detect_resilience_anomaly({})
        self.assertIn('anomaly', res) 