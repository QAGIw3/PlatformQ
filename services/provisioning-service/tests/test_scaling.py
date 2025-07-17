import unittest
from app.scaling import AdaptiveScaler
from pyignite import Client as IgniteClient

class TestAdaptiveScaler(unittest.TestCase):
    def setUp(self):
        self.ignite = IgniteClient()
        self.ignite.connect('localhost:10800')
        self.cache = self.ignite.get_or_create_cache('test_resources')
        self.scaler = AdaptiveScaler(self.ignite, self.cache)

    def test_initialize(self):
        self.scaler.initialize_tenant_resources('test_tenant')
        state = self.cache.get('test_tenant')
        self.assertEqual(state['scale_factor'], 1.0)

    def test_handle_anomaly(self):
        anomaly = {'severity': 0.6, 'details': {'tenant_id': 'test_tenant'}}
        self.scaler.handle_anomaly(anomaly)
        state = self.cache.get('test_tenant')
        self.assertGreater(state['scale_factor'], 1.0) 