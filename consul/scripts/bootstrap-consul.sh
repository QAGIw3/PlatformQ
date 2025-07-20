#!/bin/bash
# Bootstrap script for Consul cluster initialization

set -e

echo "Bootstrapping Consul cluster..."

# Wait for Consul to be ready
echo "Waiting for Consul to be ready..."
until consul members &>/dev/null; do
  echo "Consul is not ready yet. Waiting..."
  sleep 2
done

echo "Consul is ready!"

# Bootstrap ACL system
echo "Bootstrapping ACL system..."
BOOTSTRAP_OUTPUT=$(consul acl bootstrap -format=json 2>/dev/null || echo "{}")

if [ "$BOOTSTRAP_OUTPUT" != "{}" ]; then
  # Extract the bootstrap token
  CONSUL_TOKEN=$(echo $BOOTSTRAP_OUTPUT | jq -r '.SecretID')
  echo "ACL system bootstrapped successfully!"
  echo "Bootstrap Token: $CONSUL_TOKEN"
  echo "IMPORTANT: Save this token securely. It will not be shown again."
  
  # Export for use in this script
  export CONSUL_HTTP_TOKEN=$CONSUL_TOKEN
  
  # Save to a secure location (in production, use a secrets manager)
  echo $CONSUL_TOKEN > /tmp/consul-bootstrap-token
  chmod 600 /tmp/consul-bootstrap-token
else
  echo "ACL system already bootstrapped or bootstrap failed."
  # Try to read existing token
  if [ -f /tmp/consul-bootstrap-token ]; then
    export CONSUL_HTTP_TOKEN=$(cat /tmp/consul-bootstrap-token)
    echo "Using existing bootstrap token."
  else
    echo "ERROR: No bootstrap token available. Please provide CONSUL_HTTP_TOKEN."
    exit 1
  fi
fi

# Create node policy
echo "Creating node policy..."
consul acl policy create -name "node-policy" -rules @- <<EOF
node_prefix "" {
  policy = "write"
}
EOF

# Create agent policy
echo "Creating agent policy..."
consul acl policy create -name "agent-policy" -rules @- <<EOF
node_prefix "" {
  policy = "write"
}
agent_prefix "" {
  policy = "write"
}
EOF

# Create service policies
echo "Creating service policies..."

# Market Data Service
consul acl policy create -name "market-data-service-policy" -rules @- <<EOF
service "market-data-service" {
  policy = "write"
}
service "market-data-service-sidecar-proxy" {
  policy = "write"
}
service_prefix "" {
  policy = "read"
}
node_prefix "" {
  policy = "read"
}
agent_prefix "" {
  policy = "read"
}
key_prefix "config/market-data-service/" {
  policy = "write"
}
EOF

# AMM Service
consul acl policy create -name "amm-service-policy" -rules @- <<EOF
service "amm-service" {
  policy = "write"
}
service "amm-service-sidecar-proxy" {
  policy = "write"
}
service_prefix "" {
  policy = "read"
}
node_prefix "" {
  policy = "read"
}
agent_prefix "" {
  policy = "read"
}
key_prefix "config/amm-service/" {
  policy = "write"
}
EOF

# Social Trading Service
consul acl policy create -name "social-trading-service-policy" -rules @- <<EOF
service "social-trading-service" {
  policy = "write"
}
service "social-trading-service-sidecar-proxy" {
  policy = "write"
}
service_prefix "" {
  policy = "read"
}
node_prefix "" {
  policy = "read"
}
agent_prefix "" {
  policy = "read"
}
key_prefix "config/social-trading-service/" {
  policy = "write"
}
EOF

# Create service tokens
echo "Creating service tokens..."

# Market Data Service Token
MARKET_DATA_TOKEN=$(consul acl token create \
  -description "Token for market-data-service" \
  -policy-name "market-data-service-policy" \
  -format=json | jq -r '.SecretID')
echo "Market Data Service Token: $MARKET_DATA_TOKEN"

# AMM Service Token
AMM_TOKEN=$(consul acl token create \
  -description "Token for amm-service" \
  -policy-name "amm-service-policy" \
  -format=json | jq -r '.SecretID')
echo "AMM Service Token: $AMM_TOKEN"

# Social Trading Service Token
SOCIAL_TOKEN=$(consul acl token create \
  -description "Token for social-trading-service" \
  -policy-name "social-trading-service-policy" \
  -format=json | jq -r '.SecretID')
echo "Social Trading Service Token: $SOCIAL_TOKEN"

# Create agent tokens
echo "Creating agent tokens..."
AGENT_TOKEN=$(consul acl token create \
  -description "Agent token" \
  -policy-name "agent-policy" \
  -format=json | jq -r '.SecretID')

# Set default agent token
consul acl set-agent-token agent $AGENT_TOKEN

# Create service intentions
echo "Setting up service intentions..."

# Allow market-data-service to access options and futures
consul intention create -allow market-data-service options-service
consul intention create -allow market-data-service futures-service

# Allow AMM service to access dependencies
consul intention create -allow amm-service options-service
consul intention create -allow amm-service futures-service
consul intention create -allow amm-service oracle-service

# Allow social trading to access dependencies
consul intention create -allow social-trading-service order-matching-service
consul intention create -allow social-trading-service risk-service
consul intention create -allow social-trading-service blockchain-gateway
consul intention create -allow social-trading-service graph-intelligence

# Allow all services to access infrastructure
consul intention create -allow '*' ignite-cache
consul intention create -allow '*' pulsar
consul intention create -allow amm-service cassandra
consul intention create -allow social-trading-service cassandra
consul intention create -allow social-trading-service janusgraph

# Default deny all
consul intention create -deny '*' '*'

# Load initial configuration
echo "Loading initial configuration into KV store..."

# AMM Service Configuration
consul kv put config/amm-service/settings @- <<EOF
{
  "base_fee_bps": 30,
  "min_fee_bps": 1,
  "max_fee_bps": 100,
  "fee_update_interval": 300,
  "concentrated_tick_spacing": 60,
  "volume_fee_tiers": [
    [10000, 0.1],
    [50000, 0.2],
    [100000, 0.3]
  ],
  "imbalance_threshold": 0.05,
  "imbalance_fee_multiplier": 1.5,
  "stableswap_amplification": 100,
  "stableswap_fee_bps": 4
}
EOF

# Market Data Service Configuration
consul kv put config/market-data-service/settings @- <<EOF
{
  "websocket_heartbeat_interval": 30,
  "market_data_cache_ttl": 5,
  "orderbook_depth": 20,
  "price_aggregation_interval": 1,
  "max_subscriptions_per_client": 100
}
EOF

# Social Trading Service Configuration
consul kv put config/social-trading-service/settings @- <<EOF
{
  "min_copy_amount": 100,
  "max_copy_amount": 100000,
  "max_followers_per_leader": 1000,
  "reputation_update_interval": 3600,
  "performance_window_days": 30,
  "social_feed_page_size": 20,
  "leaderboard_size": 100
}
EOF

echo "Consul cluster bootstrap complete!"
echo ""
echo "Service Tokens (save these securely):"
echo "====================================="
echo "Bootstrap Token: $CONSUL_TOKEN"
echo "Market Data Service: $MARKET_DATA_TOKEN"
echo "AMM Service: $AMM_TOKEN"
echo "Social Trading Service: $SOCIAL_TOKEN"
echo ""
echo "To use these tokens, set CONSUL_HTTP_TOKEN environment variable:"
echo "export CONSUL_HTTP_TOKEN=<token>"
echo ""
echo "Access Consul UI at: http://localhost:8500"
echo "Use the bootstrap token to log in." 