from pulsar.schema import *
import uuid
import time

class UserCreatedEvent(Record):
    tenant_id = String(required=True)
    user_id = String(required=True)
    email = String(required=True)
    full_name = String()
    event_timestamp = Long(required=True, default=lambda: int(time.time() * 1000))

class SubscriptionChangedEvent(Record):
    tenant_id = String(required=True)
    user_id = String(required=True)
    subscription_id = String(required=True)
    new_tier = String(required=True)
    new_status = String(required=True)
    event_timestamp = Long(required=True, default=lambda: int(time.time() * 1000))

# ... (and so on for all other event types) 