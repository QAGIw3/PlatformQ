import pulsar
import httpx
import json

from ..knative_client import KnativeClient
from ..core.config import settings

def run_pulsar_knative_bridge():
    """
    Runs the Pulsar-to-Knative bridge.
    """
    client = pulsar.Client(settings.pulsar_url)
    consumer = client.subscribe(
        topic='knative-function-invocations',
        subscription_name='knative-bridge-subscription'
    )
    
    knative_client = KnativeClient()

    while True:
        msg = consumer.receive()
        try:
            data = json.loads(msg.data().decode('utf-8'))
            function_name = data.get("function_name")
            if function_name:
                knative_client.invoke_function(function_name)
            consumer.acknowledge(msg)
        except Exception as e:
            consumer.negative_acknowledge(msg)
            print(f"Failed to process message: {e}") 