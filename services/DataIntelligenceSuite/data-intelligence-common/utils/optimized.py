"""
Optimized utilities module.

Reduces redundancy and leverages standard libraries where possible.
"""

import asyncio
import hashlib
import json
import uuid
from typing import Any, Dict, List, Optional, TypeVar, Union, Callable, Iterable
from functools import wraps, lru_cache
from pathlib import Path
from datetime import datetime, timezone
from contextlib import asynccontextmanager, contextmanager
import time
import re
from collections import ChainMap
from itertools import islice

# Use standard library where possible
from urllib.parse import urlparse, urljoin, quote, unquote
from base64 import b64encode, b64decode
from secrets import token_urlsafe

T = TypeVar('T')


# String utilities using standard library
def to_snake_case(string: str) -> str:
    """Convert string to snake_case."""
    # Use re.sub instead of custom implementation
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', string)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()


def to_camel_case(string: str, upper_first: bool = True) -> str:
    """Convert string to camelCase or PascalCase."""
    components = string.split('_')
    if upper_first:
        return ''.join(x.title() for x in components)
    else:
        return components[0] + ''.join(x.title() for x in components[1:])


# Use standard library for URL handling
def parse_url(url: str) -> Dict[str, Any]:
    """Parse URL into components."""
    parsed = urlparse(url)
    return {
        'scheme': parsed.scheme,
        'host': parsed.hostname,
        'port': parsed.port,
        'path': parsed.path,
        'query': parsed.query,
        'fragment': parsed.fragment,
        'username': parsed.username,
        'password': parsed.password
    }


# Simplified ID generation
def generate_id(prefix: Optional[str] = None) -> str:
    """Generate a unique ID."""
    # Use uuid4 for uniqueness
    unique_id = uuid.uuid4().hex
    return f"{prefix}_{unique_id}" if prefix else unique_id


def generate_secure_token(length: int = 32) -> str:
    """Generate a secure random token."""
    # Use secrets module for security
    return token_urlsafe(length)


# Simplified hashing
def hash_string(data: str, algorithm: str = 'sha256') -> str:
    """Hash a string using specified algorithm."""
    return hashlib.new(algorithm, data.encode()).hexdigest()


def hash_dict(data: Dict[str, Any], algorithm: str = 'sha256') -> str:
    """Hash a dictionary consistently."""
    # Sort keys for consistent hashing
    json_str = json.dumps(data, sort_keys=True, default=str)
    return hash_string(json_str, algorithm)


# Use ChainMap for dict merging
def merge_dicts(*dicts: Dict[str, Any]) -> Dict[str, Any]:
    """Merge multiple dictionaries."""
    # ChainMap provides a view over multiple dicts
    return dict(ChainMap(*reversed(dicts)))


def deep_merge(base: Dict[str, Any], *updates: Dict[str, Any]) -> Dict[str, Any]:
    """Deep merge dictionaries."""
    result = base.copy()
    
    for update in updates:
        for key, value in update.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = deep_merge(result[key], value)
            else:
                result[key] = value
                
    return result


# Simplified chunking using itertools
def chunk_iterable(iterable: Iterable[T], size: int) -> Iterable[List[T]]:
    """Split an iterable into chunks."""
    it = iter(iterable)
    while True:
        chunk = list(islice(it, size))
        if not chunk:
            break
        yield chunk


# Path utilities using pathlib
def ensure_dir(path: Union[str, Path]) -> Path:
    """Ensure directory exists."""
    path = Path(path)
    path.mkdir(parents=True, exist_ok=True)
    return path


def safe_path_join(*parts: str) -> str:
    """Safely join path components."""
    return str(Path(*parts))


# Async utilities
async def run_async_tasks(tasks: List[Callable], max_concurrent: int = 10) -> List[Any]:
    """Run async tasks with concurrency limit."""
    semaphore = asyncio.Semaphore(max_concurrent)
    
    async def run_with_semaphore(task):
        async with semaphore:
            if asyncio.iscoroutinefunction(task):
                return await task()
            else:
                return await asyncio.to_thread(task)
    
    return await asyncio.gather(*[run_with_semaphore(task) for task in tasks])


@asynccontextmanager
async def async_timer(name: str = "Operation"):
    """Async context manager for timing operations."""
    start = time.perf_counter()
    try:
        yield
    finally:
        elapsed = time.perf_counter() - start
        print(f"{name} took {elapsed:.3f} seconds")


@contextmanager
def timer(name: str = "Operation"):
    """Context manager for timing operations."""
    start = time.perf_counter()
    try:
        yield
    finally:
        elapsed = time.perf_counter() - start
        print(f"{name} took {elapsed:.3f} seconds")


# Caching utilities
def memoize_async(ttl: Optional[int] = None):
    """Async memoization decorator."""
    cache = {}
    
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            key = str((args, sorted(kwargs.items())))
            
            if key in cache:
                value, timestamp = cache[key]
                if ttl is None or time.time() - timestamp < ttl:
                    return value
                    
            result = await func(*args, **kwargs)
            cache[key] = (result, time.time())
            return result
            
        wrapper.clear_cache = cache.clear
        return wrapper
        
    return decorator


# JSON utilities with better error handling
def safe_json_loads(data: Union[str, bytes], default: Any = None) -> Any:
    """Safely parse JSON with default on error."""
    try:
        if isinstance(data, bytes):
            data = data.decode('utf-8')
        return json.loads(data)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return default


def safe_json_dumps(obj: Any, default: Optional[Callable] = None, **kwargs) -> str:
    """Safely serialize to JSON."""
    if default is None:
        default = str
    return json.dumps(obj, default=default, **kwargs)


# Type conversion utilities
def to_bool(value: Any) -> bool:
    """Convert value to boolean."""
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() in ('true', '1', 'yes', 'on')
    return bool(value)


def to_int(value: Any, default: int = 0) -> int:
    """Convert value to int with default."""
    try:
        return int(value)
    except (ValueError, TypeError):
        return default


def to_float(value: Any, default: float = 0.0) -> float:
    """Convert value to float with default."""
    try:
        return float(value)
    except (ValueError, TypeError):
        return default


# Environment utilities
def get_env(key: str, default: Any = None, cast: Optional[type] = None) -> Any:
    """Get environment variable with optional casting."""
    import os
    value = os.getenv(key, default)
    
    if cast is not None and value is not None:
        if cast == bool:
            return to_bool(value)
        elif cast == int:
            return to_int(value, default)
        elif cast == float:
            return to_float(value, default)
        else:
            return cast(value)
            
    return value


# Validation utilities
def is_valid_email(email: str) -> bool:
    """Validate email address."""
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return bool(re.match(pattern, email))


def is_valid_url(url: str) -> bool:
    """Validate URL."""
    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc])
    except Exception:
        return False


def is_valid_uuid(uuid_string: str) -> bool:
    """Validate UUID string."""
    try:
        uuid.UUID(uuid_string)
        return True
    except ValueError:
        return False


# Retry utilities (simplified)
def retry(max_attempts: int = 3, delay: float = 1.0, exceptions: tuple = (Exception,)):
    """Simple retry decorator."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    if attempt < max_attempts - 1:
                        time.sleep(delay * (2 ** attempt))
                        
            raise last_exception
            
        return wrapper
    return decorator


# Batch processing utilities
def process_in_batches(
    items: List[T],
    batch_size: int,
    processor: Callable[[List[T]], Any],
    progress_callback: Optional[Callable[[int, int], None]] = None
) -> List[Any]:
    """Process items in batches with optional progress callback."""
    results = []
    total = len(items)
    
    for i in range(0, total, batch_size):
        batch = items[i:i + batch_size]
        results.extend(processor(batch))
        
        if progress_callback:
            progress_callback(min(i + batch_size, total), total)
            
    return results


# Export all utilities
__all__ = [
    # String utilities
    'to_snake_case', 'to_camel_case',
    
    # URL utilities
    'parse_url', 'is_valid_url',
    
    # ID and token generation
    'generate_id', 'generate_secure_token',
    
    # Hashing
    'hash_string', 'hash_dict',
    
    # Dictionary utilities
    'merge_dicts', 'deep_merge',
    
    # Iteration utilities
    'chunk_iterable', 'process_in_batches',
    
    # Path utilities
    'ensure_dir', 'safe_path_join',
    
    # Async utilities
    'run_async_tasks', 'async_timer', 'memoize_async',
    
    # Timing utilities
    'timer',
    
    # JSON utilities
    'safe_json_loads', 'safe_json_dumps',
    
    # Type conversion
    'to_bool', 'to_int', 'to_float',
    
    # Environment utilities
    'get_env',
    
    # Validation
    'is_valid_email', 'is_valid_uuid',
    
    # Retry utilities
    'retry'
] 