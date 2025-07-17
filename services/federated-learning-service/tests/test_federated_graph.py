import unittest

class TestFederatedGraph(unittest.TestCase):
    def test_aggregation(self):
        # Mock updates
        updates = [{'embeddings': [1.0, 2.0]}, {'embeddings': [3.0, 4.0]}]
        aggregated = federated_graph_averaging(updates)  # Assuming function
        self.assertEqual(aggregated, [2.0, 3.0]) 