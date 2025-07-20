#!/bin/bash

# Deploy Pulsar Functions for Event Router Service

PULSAR_ADMIN_URL=${PULSAR_ADMIN_URL:-"http://localhost:8080"}
NAMESPACE=${NAMESPACE:-"platformq/event-router"}

echo "Deploying Pulsar Functions to namespace: $NAMESPACE"

# Create namespace if it doesn't exist
pulsar-admin namespaces create $NAMESPACE 2>/dev/null || echo "Namespace already exists"

# Deploy Event Validator Function
echo "Deploying Event Validator Function..."
pulsar-admin functions create \
  --name event-validator \
  --namespace $NAMESPACE \
  --py event_validator.py \
  --classname event_validator.EventValidatorFunction \
  --inputs persistent://platformq/raw/events \
  --output persistent://platformq/validated/events \
  --log-topic persistent://platformq/functions/logs

# Deploy Event Enricher Function
echo "Deploying Event Enricher Function..."
pulsar-admin functions create \
  --name event-enricher \
  --namespace $NAMESPACE \
  --py event_enricher.py \
  --classname event_enricher.EventEnricherFunction \
  --inputs persistent://platformq/validated/events \
  --output persistent://platformq/enriched/events \
  --log-topic persistent://platformq/functions/logs

# Deploy Trade Enricher Function
echo "Deploying Trade Enricher Function..."
pulsar-admin functions create \
  --name trade-enricher \
  --namespace $NAMESPACE \
  --py trade_enricher.py \
  --classname trade_enricher.TradeEnricherFunction \
  --inputs persistent://platformq/trading/trade-events \
  --output persistent://platformq/trading/enriched-trades \
  --parallelism 4 \
  --cpu 2 \
  --ram 2147483648 \
  --log-topic persistent://platformq/functions/logs \
  --user-config '{"cache_ttl": 300, "batch_size": 100}'

echo "Deploying ML Model Enricher Function..."
pulsar-admin functions create \
    --name ml-model-enricher \
    --py /pulsar/functions/ml_model_enricher.py \
    --classname ml_model_enricher.MLModelEnricher \
    --tenant public \
    --namespace default \
    --inputs persistent://public/default/ml-events-raw \
    --output persistent://public/default/ml-events-enriched \
    --log-topic persistent://public/default/ml-enricher-logs \
    --cpu 2 \
    --ram 2147483648 \
    --parallelism 4 \
    --processing-guarantees EFFECTIVELY_ONCE

echo "All functions deployed successfully!"

# List deployed functions
echo -e "\nDeployed functions:"
pulsar-admin functions list --namespace $NAMESPACE 