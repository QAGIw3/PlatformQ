import unittest

class TestCollaboration(unittest.TestCase):
    def test_crdt_edit(self):
        manager = MockCRDTManager()
        manager.apply_edit('sim1', {'op': 'add'})
        self.assertEqual(manager.get_state('sim1'), {'ops': 1})

class MockCRDTManager:
    def __init__(self):
        self.states = {}
    def apply_edit(self, id, edit):
        self.states[id] = {'ops': (self.states.get(id, {'ops': 0})['ops'] + 1)}
    def get_state(self, id):
        return self.states.get(id, {})

manager = MockCRDTManager()
manager.apply_edit('sim1', {'op': 'add'})
self.assertEqual(manager.get_state('sim1'), {'ops': 1}) 