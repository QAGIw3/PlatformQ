from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.pulsar import FlinkPulsarSource, FlinkPulsarSink
from pyflink.common.serialization import SimpleStringSchema
import json
import logging

logger = logging.getLogger(__name__)

def define_predictive_alerting_job():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    # Pulsar source for forecasts
    pulsar_source = FlinkPulsarSource.builder() \
        .set_service_url('pulsar://pulsar:6650') \
        .set_admin_url('http://pulsar:8080') \
        .set_subscription_name('predictive-alerting-subscription') \
        .set_topics('simulation-forecasts') \
        .set_deserialization_schema(SimpleStringSchema()) \
        .build()

    # Pulsar sink for alerts
    pulsar_sink = FlinkPulsarSink.builder() \
        .set_service_url('pulsar://pulsar:6650') \
        .set_admin_url('http://pulsar:8080') \
        .set_topic('proactive-alerts') \
        .set_serialization_schema(SimpleStringSchema()) \
        .build()

    forecast_stream = env.add_source(pulsar_source, "Pulsar Forecast Source")

    def process_forecast(forecast_json):
        forecast_data = json.loads(forecast_json)
        simulation_id = forecast_data.get("simulation_id")
        forecast = forecast_data.get("forecast", {})
        
        # Simple rule: if the upper bound of the forecast exceeds a threshold, fire an alert.
        upper_bound = forecast.get("upper_bound", [])
        if any(value > 0.9 for value in upper_bound): # Example threshold
            alert = {
                "alert_type": "PREDICTIVE_ALERT",
                "simulation_id": simulation_id,
                "reason": "Forecasted metric exceeds threshold.",
                "details": {
                    "forecast": forecast
                },
                "severity": "HIGH"
            }
            return json.dumps(alert)
        return None

    alerts = forecast_stream.map(process_forecast).filter(lambda x: x is not None)
    
    alerts.sink_to(pulsar_sink)

    env.execute("Predictive Alerting Job")

if __name__ == '__main__':
    define_predictive_alerting_job() 