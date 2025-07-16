# PlatformQ Events

This package contains the shared Avro event schemas for the PlatformQ ecosystem.

## Usage

The schemas can be loaded and parsed using the `avro` library.

```python
import avro.schema
from pkg_resources import resource_stream

def load_schema(schema_name):
    with resource_stream('platformq.events', f'schemas/{schema_name}.avsc') as schema_file:
        return avro.schema.parse(schema_file.read().decode('utf-8'))

# Load a schema
user_created_schema = load_schema('user_created')
``` 