import unittest
from avro.schema import parse
from avro.io import DatumWriter, BinaryEncoder
import io
import os

class TestAvroSerialization(unittest.TestCase):
    def test_resource_anomaly_serialization(self):
        event = {
            'anomaly_id': 'test_id',
            'timestamp': 1234567890,
            'service_name': 'test_service',
            'resource_type': 'cpu',
            'severity': 0.8,
            'details': {'info': 'high usage'}
        }
        schema_path = os.path.join(os.path.dirname(__file__), '../../../libs/platformq-events/src/platformq/events/schemas/resource_anomaly.avsc')
        schema = parse(open(schema_path).read())
        writer = DatumWriter(schema)
        bytes_writer = io.BytesIO()
        encoder = BinaryEncoder(bytes_writer)
        writer.write(event, encoder)
        serialized = bytes_writer.getvalue()
        self.assertTrue(len(serialized) > 0) 