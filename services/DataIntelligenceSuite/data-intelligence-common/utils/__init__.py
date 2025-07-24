"""
Common utilities for Data Intelligence Suite.

Provides reusable utility functions and helpers.
"""

# Import from optimized utilities
from .optimized import (
    # String utilities
    to_snake_case,
    to_camel_case,
    
    # URL utilities
    parse_url,
    is_valid_url,
    
    # ID and token generation
    generate_id,
    generate_secure_token,
    
    # Hashing
    hash_string,
    hash_dict,
    
    # Dictionary utilities
    merge_dicts,
    deep_merge,
    
    # Iteration utilities
    chunk_iterable,
    process_in_batches,
    
    # Path utilities
    ensure_dir,
    safe_path_join,
    
    # Async utilities
    run_async_tasks,
    async_timer,
    memoize_async,
    
    # Timing utilities
    timer,
    
    # JSON utilities
    safe_json_loads,
    safe_json_dumps,
    
    # Type conversion
    to_bool,
    to_int,
    to_float,
    
    # Environment utilities
    get_env,
    
    # Validation
    is_valid_email,
    is_valid_uuid,
    
    # Retry utilities
    retry
)

from .converters import (
    DataFormat,
    TypeConverter,
    DataFrameConverter,
    FormatConverter,
    BinaryConverter,
    SchemaConverter,
    format_converter,
    convert_data
)

from .datetime_utils import (
    TimeUnit,
    DateFormat,
    DateTimeParser,
    TimeZoneUtils,
    DateRangeUtils,
    DurationUtils,
    BusinessDateUtils,
    now,
    today,
    parse_datetime,
    format_datetime,
    humanize_duration
)

from .encryption import (
    HashAlgorithm,
    SymmetricEncryption,
    AsymmetricEncryption,
    AESEncryption,
    HashUtils,
    PasswordUtils,
    TokenUtils,
    KeyDerivation,
    encrypt,
    decrypt,
    hash_password,
    verify_password,
    generate_token,
    hash_data
)

from .graph_utils import (
    GraphType,
    GraphMetrics,
    calculate_graph_metrics,
    detect_communities,
    calculate_centrality,
    find_shortest_paths,
    analyze_influence_propagation,
    find_cliques,
    calculate_trust_scores,
    graph_to_adjacency_matrix,
    adjacency_matrix_to_graph
)

from .quantum_utils import (
    BackendType,
    ProblemType,
    QuantumCircuitMetrics,
    qubo_to_ising,
    ising_to_qubo,
    create_max_cut_qubo,
    create_tsp_qubo,
    create_portfolio_qubo,
    decode_solution,
    calculate_circuit_metrics,
    estimate_quantum_advantage
)

# Import deprecated functions from helpers for backward compatibility
from .helpers import (
    retry_async,  # Deprecated
    timeout_async,
    chunk_list,
    flatten_dict,
    unflatten_dict,
    safe_get,
    safe_set,
    memoize,  # Deprecated
    rate_limit,
    sanitize_string,
    truncate_string,
    format_bytes,
    parse_bool,
    deep_get,
    deep_set,
    remove_none_values,
    get_nested_attr,
    set_nested_attr,
    camel_to_snake,
    snake_to_camel,
    slugify,
    calculate_checksum
)

__all__ = [
    # Optimized utilities
    "to_snake_case",
    "to_camel_case",
    "parse_url",
    "is_valid_url",
    "generate_id",
    "generate_secure_token",
    "hash_string",
    "hash_dict",
    "merge_dicts",
    "deep_merge",
    "chunk_iterable",
    "process_in_batches",
    "ensure_dir",
    "safe_path_join",
    "run_async_tasks",
    "async_timer",
    "memoize_async",
    "timer",
    "safe_json_loads",
    "safe_json_dumps",
    "to_bool",
    "to_int",
    "to_float",
    "get_env",
    "is_valid_email",
    "is_valid_uuid",
    "retry",
    
    # Converters
    "DataFormat",
    "TypeConverter",
    "DataFrameConverter",
    "FormatConverter",
    "BinaryConverter",
    "SchemaConverter",
    "format_converter",
    "convert_data",
    
    # DateTime
    "TimeUnit",
    "DateFormat",
    "DateTimeParser",
    "TimeZoneUtils",
    "DateRangeUtils",
    "DurationUtils",
    "BusinessDateUtils",
    "now",
    "today",
    "parse_datetime",
    "format_datetime",
    "humanize_duration",
    
    # Encryption
    "HashAlgorithm",
    "SymmetricEncryption",
    "AsymmetricEncryption",
    "AESEncryption",
    "HashUtils",
    "PasswordUtils",
    "TokenUtils",
    "KeyDerivation",
    "encrypt",
    "decrypt",
    "hash_password",
    "verify_password",
    "generate_token",
    "hash_data",
    
    # Graph utilities
    "GraphType",
    "GraphMetrics",
    "calculate_graph_metrics",
    "detect_communities",
    "calculate_centrality",
    "find_shortest_paths",
    "analyze_influence_propagation",
    "find_cliques",
    "calculate_trust_scores",
    "graph_to_adjacency_matrix",
    "adjacency_matrix_to_graph",
    
    # Quantum utilities
    "BackendType",
    "ProblemType",
    "QuantumCircuitMetrics",
    "qubo_to_ising",
    "ising_to_qubo",
    "create_max_cut_qubo",
    "create_tsp_qubo",
    "create_portfolio_qubo",
    "decode_solution",
    "calculate_circuit_metrics",
    "estimate_quantum_advantage",
    
    # Backward compatibility (deprecated)
    "retry_async",
    "timeout_async",
    "chunk_list",
    "flatten_dict",
    "unflatten_dict",
    "safe_get",
    "safe_set",
    "memoize",
    "rate_limit",
    "sanitize_string",
    "truncate_string",
    "format_bytes",
    "parse_bool",
    "deep_get",
    "deep_set",
    "remove_none_values",
    "get_nested_attr",
    "set_nested_attr",
    "camel_to_snake",
    "snake_to_camel",
    "slugify",
    "calculate_checksum"
] 