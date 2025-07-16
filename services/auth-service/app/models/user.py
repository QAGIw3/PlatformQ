import uuid
from datetime import datetime
from cassandra.cqlengine import columns
from cassandra.cqlengine.models import Model

class User(Model):
    __keyspace__ = 'auth_keyspace'
    __table_name__ = 'users'
    id = columns.UUID(primary_key=True, default=uuid.uuid4)
    username = columns.Text(index=True)
    email = columns.Text(index=True)
    hashed_password = columns.Text()
    full_name = columns.Text()
    disabled = columns.Boolean(default=False)
    created_at = columns.DateTime(default=datetime.utcnow)
    updated_at = columns.DateTime(default=datetime.utcnow)
    role = columns.Text()
    bio = columns.Text()
    profile_picture_url = columns.Text()
    did = columns.Text()
    wallet_address = columns.Text()
    storage_backend = columns.Text()
    storage_config = columns.Text()