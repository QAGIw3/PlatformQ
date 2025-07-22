from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.pulsar import FlinkPulsarSource
from pyflink.common.serialization import SimpleStringSchema
import json
import logging
import requests

logger = logging.getLogger(__name__)

def define_lineage_ingestion_job():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    # Pulsar source for lineage events
    pulsar_source = FlinkPulsarSource.builder() \
        .set_service_url('pulsar://pulsar:6650') \
        .set_admin_url('http://pulsar:8080') \
        .set_subscription_name('lineage-ingestion-subscription') \
        .set_topics('lineage-events') \
        .set_deserialization_schema(SimpleStringSchema()) \
        .build()

    lineage_stream = env.add_source(pulsar_source, "Pulsar Lineage Source")

    def process_lineage_event(lineage_json):
        try:
            lineage_data = json.loads(lineage_json)
            
            # Call the graph-intelligence-service to update the graph
            graph_service_url = "http://graph-intelligence-service:8000"
            response = requests.post(
                f"{graph_service_url}/internal/graph/lineage",
                json=lineage_data
            )
            response.raise_for_status()
            logger.info(f"Successfully processed lineage event for processor {lineage_data.get('processor_name')}")
        except Exception as e:
            logger.error(f"Failed to process lineage event: {e}")

    lineage_stream.map(process_lineage_event)

    env.execute("Lineage Ingestion Job")

if __name__ == '__main__':
    define_lineage_ingestion_job() 