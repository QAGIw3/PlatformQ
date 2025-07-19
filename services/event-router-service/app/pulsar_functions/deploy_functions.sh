#!/bin/bash

# Pulsar Functions Deployment Script
# Deploys all Pulsar Functions for event processing

set -e

# Configuration
PULSAR_ADMIN_URL="${PULSAR_ADMIN_URL:-http://localhost:8080}"
PULSAR_SERVICE_URL="${PULSAR_SERVICE_URL:-pulsar://localhost:6650}"
FUNCTION_NAMESPACE="${FUNCTION_NAMESPACE:-platformq/functions}"

echo "========================================"
echo "Pulsar Functions Deployment"
echo "========================================"
echo "Admin URL: $PULSAR_ADMIN_URL"
echo "Service URL: $PULSAR_SERVICE_URL"
echo "Namespace: $FUNCTION_NAMESPACE"
echo ""

# Check if pulsar-admin is available
if ! command -v pulsar-admin &> /dev/null; then
    echo "Error: pulsar-admin command not found. Please install Pulsar client tools."
    exit 1
fi

# Create namespace if it doesn't exist
echo "Creating namespace if needed..."
pulsar-admin namespaces create $FUNCTION_NAMESPACE 2>/dev/null || true

# Deploy Event Enricher Function
echo ""
echo "Deploying Event Enricher Function..."
pulsar-admin functions create \
  --name event-enricher \
  --py /app/pulsar_functions/event_enricher.py \
  --classname event_enricher.EventEnricherFunction \
  --inputs persistent://platformq/raw/events \
  --output persistent://platformq/enriched/events \
  --log-topic persistent://platformq/functions/logs \
  --processing-guarantees ATLEAST_ONCE \
  --parallelism 4 \
  --cpu 0.5 \
  --ram 536870912 \
  --disk 1073741824 \
  --user-config '{"cache_ttl_seconds": 300, "max_cache_size": 1000}' \
  || pulsar-admin functions update \
    --name event-enricher \
    --py /app/pulsar_functions/event_enricher.py \
    --classname event_enricher.EventEnricherFunction \
    --parallelism 4

# Deploy Event Validator Function
echo ""
echo "Deploying Event Validator Function..."
pulsar-admin functions create \
  --name event-validator \
  --py /app/pulsar_functions/event_validator.py \
  --classname event_validator.EventValidatorFunction \
  --inputs persistent://platformq/incoming/events \
  --output persistent://platformq/validated/events \
  --log-topic persistent://platformq/functions/logs \
  --processing-guarantees ATLEAST_ONCE \
  --parallelism 2 \
  --cpu 0.25 \
  --ram 268435456 \
  --disk 536870912 \
  --user-config '{"strict_mode": false, "filter_test_events": true}' \
  || pulsar-admin functions update \
    --name event-validator \
    --py /app/pulsar_functions/event_validator.py \
    --classname event_validator.EventValidatorFunction \
    --parallelism 2

# Deploy Event Router Function
echo ""
echo "Deploying Event Router Function..."
pulsar-admin functions create \
  --name event-router \
  --py /app/pulsar_functions/event_router.py \
  --classname event_router.EventRouterFunction \
  --inputs persistent://platformq/validated/events \
  --log-topic persistent://platformq/functions/logs \
  --processing-guarantees ATLEAST_ONCE \
  --parallelism 3 \
  --cpu 0.5 \
  --ram 536870912 \
  --disk 536870912 \
  --user-config '{"routing_rules_topic": "persistent://platformq/config/routing-rules"}' \
  || pulsar-admin functions update \
    --name event-router \
    --py /app/pulsar_functions/event_router.py \
    --classname event_router.EventRouterFunction \
    --parallelism 3

# Deploy Metric Aggregator Function
echo ""
echo "Deploying Metric Aggregator Function..."
pulsar-admin functions create \
  --name metric-aggregator \
  --py /app/pulsar_functions/metric_aggregator.py \
  --classname metric_aggregator.MetricAggregatorFunction \
  --inputs persistent://platformq/metrics/raw \
  --output persistent://platformq/metrics/aggregated \
  --log-topic persistent://platformq/functions/logs \
  --processing-guarantees EFFECTIVELY_ONCE \
  --parallelism 2 \
  --cpu 0.5 \
  --ram 536870912 \
  --disk 536870912 \
  --window-config '{"windowLengthCount": 100, "slidingIntervalCount": 10}' \
  || pulsar-admin functions update \
    --name metric-aggregator \
    --py /app/pulsar_functions/metric_aggregator.py \
    --classname metric_aggregator.MetricAggregatorFunction \
    --parallelism 2

# Deploy Data Masking Function
echo ""
echo "Deploying Data Masking Function..."
pulsar-admin functions create \
  --name data-masker \
  --py /app/pulsar_functions/data_masker.py \
  --classname data_masker.DataMaskingFunction \
  --inputs persistent://platformq/sensitive/events \
  --output persistent://platformq/masked/events \
  --log-topic persistent://platformq/functions/logs \
  --processing-guarantees ATLEAST_ONCE \
  --parallelism 2 \
  --cpu 0.25 \
  --ram 268435456 \
  --disk 268435456 \
  --user-config '{"masking_rules": {"email": "hash", "phone": "partial", "ssn": "full"}}' \
  || pulsar-admin functions update \
    --name data-masker \
    --py /app/pulsar_functions/data_masker.py \
    --classname data_masker.DataMaskingFunction \
    --parallelism 2

# List deployed functions
echo ""
echo "========================================"
echo "Deployed Functions:"
echo "========================================"
pulsar-admin functions list --namespace $FUNCTION_NAMESPACE

# Check function status
echo ""
echo "========================================"
echo "Function Status:"
echo "========================================"
for func in event-enricher event-validator event-router metric-aggregator data-masker; do
    echo ""
    echo "Status of $func:"
    pulsar-admin functions status --name $func || echo "Function $func not found"
done

echo ""
echo "========================================"
echo "Deployment Complete!"
echo "========================================"
echo ""
echo "To monitor functions:"
echo "  pulsar-admin functions stats --name <function-name>"
echo ""
echo "To view logs:"
echo "  pulsar-admin functions logs --name <function-name>"
echo ""
echo "To restart a function:"
echo "  pulsar-admin functions restart --name <function-name>" 