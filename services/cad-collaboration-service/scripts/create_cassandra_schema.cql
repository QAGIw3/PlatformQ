-- Cassandra schema for CAD collaboration service
-- This schema is designed for high-throughput writes and time-series queries

-- Create keyspace if not exists
CREATE KEYSPACE IF NOT EXISTS platformq_cad
  WITH REPLICATION = {
    'class': 'NetworkTopologyStrategy',
    'datacenter1': 3
  }
  AND DURABLE_WRITES = true;

USE platformq_cad;

-- Geometry operation log
-- Stores all geometry operations for replay and analysis
CREATE TABLE IF NOT EXISTS geometry_operations (
    tenant_id UUID,
    session_id UUID,
    operation_timestamp timestamp,
    operation_id UUID,
    user_id text,
    operation_type text,
    vector_clock frozen<map<text, bigint>>,
    target_object_ids list<text>,
    operation_data blob,  -- Compressed operation data
    parent_operations list<UUID>,
    PRIMARY KEY ((tenant_id, session_id), operation_timestamp, operation_id)
) WITH CLUSTERING ORDER BY (operation_timestamp DESC, operation_id ASC)
  AND compaction = {
    'class': 'TimeWindowCompactionStrategy',
    'compaction_window_unit': 'DAYS',
    'compaction_window_size': 1
  }
  AND gc_grace_seconds = 86400;  -- 1 day

-- Geometry version history
-- Stores version snapshots with efficient diffing
CREATE TABLE IF NOT EXISTS geometry_versions (
    tenant_id UUID,
    asset_id UUID,
    version_number int,
    created_at timestamp,
    created_by UUID,
    parent_version_id UUID,
    operation_log_start UUID,
    operation_log_end UUID,
    operation_count int,
    geometry_snapshot blob,  -- Compressed full snapshot
    diff_from_previous blob,  -- Compressed diff
    metadata frozen<map<text, text>>,
    vertex_count int,
    edge_count int,
    face_count int,
    file_size_bytes bigint,
    commit_message text,
    PRIMARY KEY ((tenant_id, asset_id), version_number)
) WITH CLUSTERING ORDER BY (version_number DESC)
  AND compression = {
    'sstable_compression': 'LZ4Compressor',
    'chunk_length_kb': 64
  };

-- User activity tracking for CAD sessions
CREATE TABLE IF NOT EXISTS cad_user_activity (
    tenant_id UUID,
    user_id UUID,
    activity_date date,
    session_id UUID,
    activity_timestamp timestamp,
    operation_count counter,
    PRIMARY KEY ((tenant_id, user_id, activity_date), session_id, activity_timestamp)
) WITH CLUSTERING ORDER BY (session_id ASC, activity_timestamp DESC);

-- Session metrics for analytics
CREATE TABLE IF NOT EXISTS session_metrics (
    tenant_id UUID,
    session_id UUID,
    metric_timestamp timestamp,
    active_users set<text>,
    total_operations bigint,
    geometry_stats frozen<map<text, int>>,  -- vertex_count, edge_count, face_count
    crdt_state_size bigint,
    conflict_count int,
    checkpoint_count int,
    PRIMARY KEY ((tenant_id, session_id), metric_timestamp)
) WITH CLUSTERING ORDER BY (metric_timestamp DESC)
  AND default_time_to_live = 2592000;  -- 30 days TTL

-- Mesh optimization results
CREATE TABLE IF NOT EXISTS mesh_optimization_results (
    tenant_id UUID,
    asset_id UUID,
    request_id UUID,
    completed_at timestamp,
    optimization_type text,
    status text,
    original_poly_count bigint,
    optimized_poly_count bigint,
    original_file_size bigint,
    optimized_file_size bigint,
    processing_time_ms bigint,
    optimized_mesh_uri text,
    lod_levels frozen<list<frozen<tuple<float, text, bigint>>>>,  -- (level, uri, poly_count)
    error_message text,
    metadata frozen<map<text, text>>,
    PRIMARY KEY ((tenant_id, asset_id), completed_at, request_id)
) WITH CLUSTERING ORDER BY (completed_at DESC, request_id ASC)
  AND default_time_to_live = 7776000;  -- 90 days TTL

-- Collaboration patterns for analytics
CREATE TABLE IF NOT EXISTS collaboration_patterns (
    tenant_id UUID,
    time_bucket timestamp,  -- Hourly buckets
    asset_type text,
    pattern_type text,  -- 'concurrent_edit', 'conflict_resolution', 'review_cycle'
    user_pairs frozen<set<frozen<tuple<text, text>>>>,
    occurrence_count counter,
    avg_session_duration_seconds int,
    PRIMARY KEY ((tenant_id, time_bucket), asset_type, pattern_type)
) WITH CLUSTERING ORDER BY (asset_type ASC, pattern_type ASC);

-- Create indexes for common queries
CREATE INDEX IF NOT EXISTS operations_by_user ON geometry_operations (user_id);
CREATE INDEX IF NOT EXISTS versions_by_creator ON geometry_versions (created_by);
CREATE INDEX IF NOT EXISTS sessions_by_date ON session_metrics (metric_timestamp);

-- Create materialized view for active sessions
CREATE MATERIALIZED VIEW IF NOT EXISTS active_sessions AS
    SELECT tenant_id, session_id, metric_timestamp, active_users, total_operations
    FROM session_metrics
    WHERE tenant_id IS NOT NULL
      AND session_id IS NOT NULL
      AND metric_timestamp IS NOT NULL
      AND metric_timestamp > dateOf(now()) - 3600000  -- Last hour
    PRIMARY KEY ((tenant_id), metric_timestamp, session_id)
    WITH CLUSTERING ORDER BY (metric_timestamp DESC, session_id ASC); 