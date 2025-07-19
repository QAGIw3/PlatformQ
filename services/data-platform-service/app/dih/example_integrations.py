"""
Example DIH Integrations

Shows how to configure and use the Digital Integration Hub for various scenarios.
"""

import logging
from typing import Dict, Any, List
from datetime import datetime, timedelta

from .digital_integration_hub import (
    DigitalIntegrationHub,
    CacheRegion,
    DataSourceConfig,
    APIEndpoint,
    DataSource,
    CacheStrategy,
    ConsistencyLevel
)

logger = logging.getLogger(__name__)


async def setup_user_session_cache(dih: DigitalIntegrationHub):
    """
    Example: User Session Management
    
    Creates a replicated cache for user sessions with automatic expiry
    and synchronization from PostgreSQL auth service.
    """
    logger.info("Setting up user session cache...")
    
    # Create cache region for sessions
    await dih.create_cache_region(CacheRegion(
        name="user_sessions",
        cache_mode="REPLICATED",  # Replicated for fast reads
        atomicity_mode="ATOMIC",  # Don't need transactions
        cache_strategy=CacheStrategy.WRITE_THROUGH,
        expiry_policy={
            "create": 3600,    # 1 hour after creation
            "access": 3600     # 1 hour after last access
        },
        indexes=[
            ("user_id", "STRING"),
            ("tenant_id", "STRING"),
            ("session_token", "STRING")
        ]
    ))
    
    # Register PostgreSQL data source
    await dih.register_data_source(
        source_name="auth_postgres",
        config=DataSourceConfig(
            source_type=DataSource.POSTGRESQL,
            connection_params={
                "host": "postgresql",
                "port": 5432,
                "database": "auth_db",
                "user": "auth_user",
                "password": "auth_pass"
            },
            sync_interval_seconds=300,  # Sync every 5 minutes
            batch_size=5000,
            consistency_level=ConsistencyLevel.STRONG
        ),
        target_regions=["user_sessions"]
    )
    
    # Register API endpoint for session lookup
    await dih.register_api_endpoint(APIEndpoint(
        path="session/validate",
        cache_regions=["user_sessions"],
        query_template="SELECT * FROM user_sessions WHERE session_token = '{token}' AND expires_at > CURRENT_TIMESTAMP",
        cache_key_pattern="session:{token}",
        ttl_seconds=60  # Cache validation result for 1 minute
    ))


async def setup_asset_metadata_cache(dih: DigitalIntegrationHub):
    """
    Example: Digital Asset Metadata Cache
    
    Creates a partitioned cache for asset metadata with read-through
    from Cassandra and Elasticsearch.
    """
    logger.info("Setting up asset metadata cache...")
    
    # Create cache region
    await dih.create_cache_region(CacheRegion(
        name="asset_metadata",
        cache_mode="PARTITIONED",
        backups=2,  # 2 backups for reliability
        cache_strategy=CacheStrategy.READ_THROUGH,
        eviction_policy="LRU",
        eviction_max_size=50000000,  # 50M entries
        indexes=[
            ("asset_id", "STRING"),
            ("asset_type", "STRING"),
            ("owner_id", "STRING"),
            ("tenant_id", "STRING"),
            ("created_at", "TIMESTAMP")
        ]
    ))
    
    # Register Cassandra source for core metadata
    await dih.register_data_source(
        source_name="asset_cassandra",
        config=DataSourceConfig(
            source_type=DataSource.CASSANDRA,
            connection_params={
                "hosts": ["cassandra-0", "cassandra-1", "cassandra-2"],
                "keyspace": "digital_assets"
            },
            sync_interval_seconds=None,  # On-demand only
            consistency_level=ConsistencyLevel.EVENTUAL
        ),
        target_regions=["asset_metadata"]
    )
    
    # Register Elasticsearch source for search metadata
    await dih.register_data_source(
        source_name="asset_elasticsearch",
        config=DataSourceConfig(
            source_type=DataSource.ELASTICSEARCH,
            connection_params={
                "hosts": ["elasticsearch:9200"],
                "options": {
                    "verify_certs": False
                }
            },
            sync_interval_seconds=600,  # Sync every 10 minutes
            batch_size=10000,
            consistency_level=ConsistencyLevel.EVENTUAL
        ),
        target_regions=["asset_metadata"]
    )
    
    # API endpoints
    await dih.register_api_endpoint(APIEndpoint(
        path="asset/get",
        cache_regions=["asset_metadata"],
        query_template="SELECT * FROM asset_metadata WHERE asset_id = '{asset_id}'",
        ttl_seconds=1800  # 30 minutes
    ))
    
    await dih.register_api_endpoint(APIEndpoint(
        path="asset/search",
        cache_regions=["asset_metadata"],
        query_template="""
            SELECT * FROM asset_metadata 
            WHERE tenant_id = '{tenant_id}' 
            AND asset_type = '{asset_type}'
            AND created_at >= '{start_date}'
            ORDER BY created_at DESC
            LIMIT {limit}
        """,
        ttl_seconds=300  # 5 minutes for search results
    ))


async def setup_realtime_metrics_cache(dih: DigitalIntegrationHub):
    """
    Example: Real-time Metrics Aggregation
    
    Creates a cache for real-time metrics with write-behind to time-series DB
    and automatic aggregation.
    """
    logger.info("Setting up real-time metrics cache...")
    
    # Create cache region
    await dih.create_cache_region(CacheRegion(
        name="realtime_metrics",
        cache_mode="PARTITIONED",
        backups=1,
        atomicity_mode="ATOMIC",
        cache_strategy=CacheStrategy.WRITE_BEHIND,  # Async write to DB
        expiry_policy={
            "create": 300  # 5 minute TTL for metrics
        },
        indexes=[
            ("metric_name", "STRING"),
            ("resource_id", "STRING"),
            ("timestamp", "LONG")
        ]
    ))
    
    # Create aggregated metrics cache
    await dih.create_cache_region(CacheRegion(
        name="metrics_aggregates",
        cache_mode="REPLICATED",
        cache_strategy=CacheStrategy.WRITE_THROUGH,
        expiry_policy={
            "create": 3600  # 1 hour TTL
        },
        indexes=[
            ("metric_name", "STRING"),
            ("aggregation_type", "STRING"),
            ("time_bucket", "TIMESTAMP")
        ]
    ))
    
    # API endpoints for metrics
    await dih.register_api_endpoint(APIEndpoint(
        path="metrics/current",
        cache_regions=["realtime_metrics"],
        query_template="""
            SELECT metric_name, resource_id, value, timestamp
            FROM realtime_metrics
            WHERE metric_name = '{metric_name}'
            AND timestamp > {start_time}
            ORDER BY timestamp DESC
        """,
        ttl_seconds=10  # Very short TTL for real-time data
    ))
    
    await dih.register_api_endpoint(APIEndpoint(
        path="metrics/aggregate",
        cache_regions=["metrics_aggregates"],
        query_template="""
            SELECT time_bucket, aggregation_type, value
            FROM metrics_aggregates
            WHERE metric_name = '{metric_name}'
            AND aggregation_type = '{agg_type}'
            AND time_bucket >= '{start_time}'
            AND time_bucket <= '{end_time}'
            ORDER BY time_bucket
        """,
        ttl_seconds=60  # 1 minute for aggregates
    ))


async def setup_transaction_cache(dih: DigitalIntegrationHub):
    """
    Example: Financial Transaction Processing
    
    Creates a transactional cache for ACID-compliant financial operations
    with strong consistency.
    """
    logger.info("Setting up transaction cache...")
    
    # Create cache region
    await dih.create_cache_region(CacheRegion(
        name="financial_transactions",
        cache_mode="PARTITIONED",
        backups=2,
        atomicity_mode="TRANSACTIONAL",  # Full ACID support
        cache_strategy=CacheStrategy.WRITE_THROUGH,
        indexes=[
            ("tx_id", "STRING"),
            ("user_id", "STRING"),
            ("account_id", "STRING"),
            ("status", "STRING"),
            ("created_at", "TIMESTAMP")
        ]
    ))
    
    # Account balances cache
    await dih.create_cache_region(CacheRegion(
        name="account_balances",
        cache_mode="PARTITIONED",
        backups=2,
        atomicity_mode="TRANSACTIONAL",
        cache_strategy=CacheStrategy.WRITE_THROUGH,
        indexes=[
            ("account_id", "STRING"),
            ("currency", "STRING")
        ]
    ))
    
    # Register PostgreSQL for persistence
    await dih.register_data_source(
        source_name="finance_postgres",
        config=DataSourceConfig(
            source_type=DataSource.POSTGRESQL,
            connection_params={
                "host": "postgresql",
                "port": 5432,
                "database": "finance_db",
                "user": "finance_user",
                "password": "finance_pass"
            },
            consistency_level=ConsistencyLevel.STRONG
        ),
        target_regions=["financial_transactions", "account_balances"]
    )
    
    # Transaction processing endpoint
    await dih.register_api_endpoint(APIEndpoint(
        path="transaction/status",
        cache_regions=["financial_transactions"],
        query_template="SELECT * FROM financial_transactions WHERE tx_id = '{tx_id}'",
        ttl_seconds=300
    ))


async def setup_ml_feature_cache(dih: DigitalIntegrationHub):
    """
    Example: ML Feature Store Cache
    
    Creates a cache for ML features with low-latency serving
    and batch updates from the data lake.
    """
    logger.info("Setting up ML feature cache...")
    
    # Online feature cache
    await dih.create_cache_region(CacheRegion(
        name="ml_features_online",
        cache_mode="PARTITIONED",
        backups=1,
        atomicity_mode="ATOMIC",
        cache_strategy=CacheStrategy.READ_THROUGH,
        eviction_policy="LRU",
        eviction_max_size=100000000,  # 100M features
        indexes=[
            ("entity_id", "STRING"),
            ("feature_name", "STRING"),
            ("version", "INT")
        ]
    ))
    
    # Feature statistics cache
    await dih.create_cache_region(CacheRegion(
        name="feature_statistics",
        cache_mode="REPLICATED",
        cache_strategy=CacheStrategy.WRITE_THROUGH,
        expiry_policy={
            "create": 86400  # 24 hour TTL
        },
        indexes=[
            ("feature_name", "STRING"),
            ("version", "INT")
        ]
    ))
    
    # API endpoints
    await dih.register_api_endpoint(APIEndpoint(
        path="features/get",
        cache_regions=["ml_features_online"],
        query_template="""
            SELECT feature_value
            FROM ml_features_online
            WHERE entity_id = '{entity_id}'
            AND feature_name IN ({feature_names})
            AND version = {version}
        """,
        ttl_seconds=600  # 10 minutes
    ))
    
    await dih.register_api_endpoint(APIEndpoint(
        path="features/batch",
        cache_regions=["ml_features_online"],
        query_template="""
            SELECT entity_id, feature_name, feature_value
            FROM ml_features_online
            WHERE entity_id IN ({entity_ids})
            AND feature_name IN ({feature_names})
            AND version = {version}
        """,
        ttl_seconds=300  # 5 minutes for batch
    ))


async def example_transaction_processing(dih: DigitalIntegrationHub):
    """
    Example: ACID Transaction Processing
    
    Shows how to use DIH for complex multi-step transactions.
    """
    
    # Example: Transfer funds between accounts
    operations = [
        # Debit from source account
        ("account_balances", "update", {
            "key": "account_123",
            "updates": {
                "balance": -100.00,
                "last_updated": datetime.utcnow()
            }
        }),
        
        # Credit to destination account
        ("account_balances", "update", {
            "key": "account_456",
            "updates": {
                "balance": 100.00,
                "last_updated": datetime.utcnow()
            }
        }),
        
        # Record transaction
        ("financial_transactions", "put", {
            "key": "tx_789",
            "value": {
                "tx_id": "tx_789",
                "from_account": "account_123",
                "to_account": "account_456",
                "amount": 100.00,
                "status": "completed",
                "created_at": datetime.utcnow()
            }
        })
    ]
    
    # Execute as ACID transaction
    success = await dih.execute_transaction(
        operations,
        isolation="SERIALIZABLE",  # Highest isolation
        concurrency="PESSIMISTIC",  # Lock-based
        timeout=5000  # 5 second timeout
    )
    
    if success:
        logger.info("Transaction completed successfully")
    else:
        logger.error("Transaction failed and was rolled back")


async def example_continuous_query(dih: DigitalIntegrationHub):
    """
    Example: Continuous Query for Real-time Updates
    
    Shows how to set up continuous queries for real-time notifications.
    """
    
    # Define callback for new high-value transactions
    async def on_high_value_transaction(event: Dict[str, Any]):
        logger.info(f"High value transaction detected: {event}")
        # Send alert, update dashboard, etc.
    
    # Create continuous query
    query_id = await dih.create_continuous_query(
        region_name="financial_transactions",
        filter_sql="amount > 10000 AND status = 'pending'",
        callback=on_high_value_transaction,
        initial_query=True  # Process existing matches
    )
    
    logger.info(f"Continuous query created: {query_id}")


async def setup_all_examples(dih: DigitalIntegrationHub):
    """Setup all example integrations"""
    await setup_user_session_cache(dih)
    await setup_asset_metadata_cache(dih)
    await setup_realtime_metrics_cache(dih)
    await setup_transaction_cache(dih)
    await setup_ml_feature_cache(dih)
    
    logger.info("All example integrations configured successfully") 