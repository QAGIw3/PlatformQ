"""
Cache Utilities for DataIntelligenceSuite

Provides utility functions for cache operations.
"""

import hashlib
import json
import pickle
import zlib
from typing import Any, Union, Optional, Callable
from datetime import timedelta
import re


def generate_cache_key(*args, prefix: Optional[str] = None, **kwargs) -> str:
    """
    Generate a cache key from arguments.
    
    Args:
        *args: Positional arguments to include in key
        prefix: Optional prefix for the key
        **kwargs: Keyword arguments to include in key
        
    Returns:
        Cache key string
    """
    key_parts = []
    
    if prefix:
        key_parts.append(prefix)
        
    # Add positional arguments
    for arg in args:
        key_parts.append(str(arg))
        
    # Add keyword arguments (sorted for consistency)
    for k, v in sorted(kwargs.items()):
        key_parts.append(f"{k}={v}")
        
    key = ":".join(key_parts)
    
    # Hash if too long
    if len(key) > 250:
        key_hash = hashlib.sha256(key.encode()).hexdigest()
        return f"{prefix}:hash:{key_hash}" if prefix else f"hash:{key_hash}"
        
    return key


def hash_key(key: str) -> str:
    """
    Hash a cache key using SHA256.
    
    Args:
        key: Key to hash
        
    Returns:
        Hashed key
    """
    return hashlib.sha256(key.encode()).hexdigest()


def parse_ttl(ttl_str: str) -> timedelta:
    """
    Parse TTL string to timedelta.
    
    Supports formats like:
    - "30s" -> 30 seconds
    - "5m" -> 5 minutes  
    - "2h" -> 2 hours
    - "1d" -> 1 day
    
    Args:
        ttl_str: TTL string
        
    Returns:
        timedelta object
    """
    units = {
        "s": "seconds",
        "m": "minutes",
        "h": "hours",
        "d": "days"
    }
    
    match = re.match(r"(\d+)([smhd])", ttl_str.lower())
    if not match:
        raise ValueError(f"Invalid TTL format: {ttl_str}")
        
    value = int(match.group(1))
    unit = units.get(match.group(2))
    
    if not unit:
        raise ValueError(f"Unknown TTL unit: {match.group(2)}")
        
    return timedelta(**{unit: value})


def serialize_value(value: Any, compress: bool = False) -> bytes:
    """
    Serialize value for cache storage.
    
    Args:
        value: Value to serialize
        compress: Whether to compress the serialized data
        
    Returns:
        Serialized bytes
    """
    data = pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL)
    
    if compress:
        data = zlib.compress(data)
        
    return data


def deserialize_value(data: bytes, compressed: bool = False) -> Any:
    """
    Deserialize value from cache storage.
    
    Args:
        data: Serialized data
        compressed: Whether the data is compressed
        
    Returns:
        Deserialized value
    """
    if compressed:
        data = zlib.decompress(data)
        
    return pickle.loads(data)


def estimate_size(value: Any) -> int:
    """
    Estimate the size of a value in bytes.
    
    Args:
        value: Value to estimate
        
    Returns:
        Estimated size in bytes
    """
    try:
        return len(pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL))
    except:
        # Fallback for non-picklable objects
        return len(str(value).encode())


def is_cache_key_valid(key: str) -> bool:
    """
    Check if a cache key is valid.
    
    Args:
        key: Key to validate
        
    Returns:
        True if valid
    """
    if not key:
        return False
        
    # Check length
    if len(key) > 250:
        return False
        
    # Check for invalid characters
    invalid_chars = ['\n', '\r', '\t', '\0']
    return not any(char in key for char in invalid_chars)


def normalize_cache_key(key: str) -> str:
    """
    Normalize a cache key by removing invalid characters.
    
    Args:
        key: Key to normalize
        
    Returns:
        Normalized key
    """
    # Replace whitespace with underscores
    key = re.sub(r'\s+', '_', key)
    
    # Remove invalid characters
    key = re.sub(r'[^\w\-:.]', '', key)
    
    # Truncate if too long
    if len(key) > 250:
        key = key[:250]
        
    return key


def create_key_pattern(pattern: str) -> re.Pattern:
    """
    Create regex pattern from cache key pattern.
    
    Supports wildcards:
    - * matches any sequence of characters
    - ? matches any single character
    
    Args:
        pattern: Pattern string
        
    Returns:
        Compiled regex pattern
    """
    # Escape special regex characters except * and ?
    pattern = re.escape(pattern)
    
    # Replace wildcards with regex equivalents
    pattern = pattern.replace(r'\*', '.*')
    pattern = pattern.replace(r'\?', '.')
    
    return re.compile(f'^{pattern}$')


def match_keys(keys: list[str], pattern: str) -> list[str]:
    """
    Match keys against a pattern.
    
    Args:
        keys: List of keys to match
        pattern: Pattern to match against
        
    Returns:
        List of matching keys
    """
    regex = create_key_pattern(pattern)
    return [key for key in keys if regex.match(key)]


def chunk_keys(keys: list[str], chunk_size: int = 100) -> list[list[str]]:
    """
    Split keys into chunks for batch operations.
    
    Args:
        keys: List of keys
        chunk_size: Size of each chunk
        
    Returns:
        List of key chunks
    """
    return [keys[i:i + chunk_size] for i in range(0, len(keys), chunk_size)]


def create_cache_key_builder(namespace: str) -> Callable:
    """
    Create a cache key builder function for a namespace.
    
    Args:
        namespace: Namespace for keys
        
    Returns:
        Key builder function
    """
    def build_key(*args, **kwargs) -> str:
        return generate_cache_key(*args, prefix=namespace, **kwargs)
        
    return build_key


class CacheKeyBuilder:
    """
    Fluent cache key builder.
    
    Example:
        key = CacheKeyBuilder()
            .namespace("users")
            .add("profile")
            .add(user_id)
            .param("version", 2)
            .build()
    """
    
    def __init__(self):
        self._parts = []
        self._params = {}
        
    def namespace(self, ns: str) -> 'CacheKeyBuilder':
        """Set namespace"""
        self._namespace = ns
        return self
        
    def add(self, part: Any) -> 'CacheKeyBuilder':
        """Add key part"""
        self._parts.append(str(part))
        return self
        
    def param(self, name: str, value: Any) -> 'CacheKeyBuilder':
        """Add parameter"""
        self._params[name] = value
        return self
        
    def build(self) -> str:
        """Build the cache key"""
        parts = []
        
        if hasattr(self, '_namespace'):
            parts.append(self._namespace)
            
        parts.extend(self._parts)
        
        # Add parameters
        for k, v in sorted(self._params.items()):
            parts.append(f"{k}={v}")
            
        return ":".join(parts)


def calculate_hit_rate(hits: int, misses: int) -> float:
    """
    Calculate cache hit rate.
    
    Args:
        hits: Number of cache hits
        misses: Number of cache misses
        
    Returns:
        Hit rate as percentage (0-100)
    """
    total = hits + misses
    if total == 0:
        return 0.0
        
    return (hits / total) * 100


def format_cache_stats(stats: dict) -> str:
    """
    Format cache statistics for display.
    
    Args:
        stats: Statistics dictionary
        
    Returns:
        Formatted string
    """
    lines = ["Cache Statistics:"]
    lines.append(f"  Hits: {stats.get('hits', 0):,}")
    lines.append(f"  Misses: {stats.get('misses', 0):,}")
    
    hit_rate = calculate_hit_rate(
        stats.get('hits', 0),
        stats.get('misses', 0)
    )
    lines.append(f"  Hit Rate: {hit_rate:.2f}%")
    
    lines.append(f"  Size: {stats.get('size', 0):,}")
    lines.append(f"  Evictions: {stats.get('evictions', 0):,}")
    
    return "\n".join(lines) 