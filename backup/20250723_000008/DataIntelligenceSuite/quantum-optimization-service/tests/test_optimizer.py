import unittest

from app.main import optimize_pipeline

class TestOptimizer(unittest.TestCase):
    def test_optimize(self):
        result = optimize_pipeline({})
        self.assertIn('optimized', result) 