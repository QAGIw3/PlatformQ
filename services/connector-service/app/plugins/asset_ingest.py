from platformq_shared.event_publisher import EventPublisher

event_publisher = EventPublisher()

def publish_event(event_type: str, data: Dict):
    event_publisher.publish(event_type, data) 