"""
Helper Utilities

Common helper functions for the platform.
"""

import asyncio
import hashlib
import uuid
from typing import Any, Dict, List, Optional, TypeVar, Callable, Union
from functools import wraps
import time
import json
from datetime import datetime


T = TypeVar('T')


# Deprecated - use core.patterns.resilience.retry instead
from ..core.patterns.resilience import retry as retry_async_impl
from ..core.patterns.resilience import RetryConfig

def retry_async(
    max_attempts: int = 3,
    delay: float = 1.0,
    backoff: float = 2.0,
    exceptions: tuple = (Exception,)
):
    """
    Async retry decorator with exponential backoff.
    
    DEPRECATED: Use core.patterns.resilience.retry instead.
    
    Args:
        max_attempts: Maximum number of retry attempts
        delay: Initial delay between retries in seconds
        backoff: Backoff multiplier for each retry
        exceptions: Tuple of exceptions to catch and retry
    """
    import warnings
    warnings.warn(
        "retry_async is deprecated. Use core.patterns.resilience.retry instead.",
        DeprecationWarning,
        stacklevel=2
    )
    
    config = RetryConfig(
        max_attempts=max_attempts,
        initial_delay=delay,
        exponential_base=backoff,
        retry_on=list(exceptions)
    )
    return retry_async_impl(config)


def timeout_async(seconds: float):
    """
    Async timeout decorator.
    
    Args:
        seconds: Timeout in seconds
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def wrapper(*args, **kwargs) -> Any:
            return await asyncio.wait_for(
                func(*args, **kwargs),
                timeout=seconds
            )
        return wrapper
    return decorator


def chunk_list(lst: List[T], chunk_size: int) -> List[List[T]]:
    """
    Split a list into chunks of specified size.
    
    Args:
        lst: List to chunk
        chunk_size: Size of each chunk
        
    Returns:
        List of chunks
    """
    if chunk_size <= 0:
        raise ValueError("Chunk size must be positive")
        
    return [
        lst[i:i + chunk_size]
        for i in range(0, len(lst), chunk_size)
    ]


def flatten_dict(
    d: Dict[str, Any],
    parent_key: str = '',
    sep: str = '.'
) -> Dict[str, Any]:
    """
    Flatten a nested dictionary.
    
    Args:
        d: Dictionary to flatten
        parent_key: Parent key for recursion
        sep: Separator for nested keys
        
    Returns:
        Flattened dictionary
    """
    items = []
    
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        
        if isinstance(v, dict):
            items.extend(
                flatten_dict(v, new_key, sep=sep).items()
            )
        elif isinstance(v, list):
            for i, item in enumerate(v):
                if isinstance(item, dict):
                    items.extend(
                        flatten_dict(
                            item,
                            f"{new_key}[{i}]",
                            sep=sep
                        ).items()
                    )
                else:
                    items.append((f"{new_key}[{i}]", item))
        else:
            items.append((new_key, v))
            
    return dict(items)


def merge_dicts(*dicts: Dict[str, Any], deep: bool = True) -> Dict[str, Any]:
    """
    Merge multiple dictionaries.
    
    Args:
        *dicts: Dictionaries to merge
        deep: Whether to deep merge nested dictionaries
        
    Returns:
        Merged dictionary
    """
    result = {}
    
    for d in dicts:
        if not isinstance(d, dict):
            continue
            
        for key, value in d.items():
            if deep and key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = merge_dicts(result[key], value, deep=True)
            else:
                result[key] = value
                
    return result


def generate_id(prefix: Optional[str] = None) -> str:
    """
    Generate a unique ID.
    
    Args:
        prefix: Optional prefix for the ID
        
    Returns:
        Unique ID string
    """
    unique_id = str(uuid.uuid4())
    
    if prefix:
        return f"{prefix}_{unique_id}"
    else:
        return unique_id


def hash_data(
    data: Union[str, bytes, Dict[str, Any]],
    algorithm: str = 'sha256'
) -> str:
    """
    Hash data using specified algorithm.
    
    Args:
        data: Data to hash
        algorithm: Hash algorithm to use
        
    Returns:
        Hex digest of the hash
    """
    if algorithm not in hashlib.algorithms_available:
        raise ValueError(f"Unsupported hash algorithm: {algorithm}")
        
    hasher = hashlib.new(algorithm)
    
    if isinstance(data, str):
        hasher.update(data.encode('utf-8'))
    elif isinstance(data, bytes):
        hasher.update(data)
    elif isinstance(data, dict):
        # Sort keys for consistent hashing
        json_str = json.dumps(data, sort_keys=True)
        hasher.update(json_str.encode('utf-8'))
    else:
        raise TypeError(f"Unsupported data type: {type(data)}")
        
    return hasher.hexdigest()


def safe_get(
    d: Dict[str, Any],
    path: str,
    default: Any = None,
    sep: str = '.'
) -> Any:
    """
    Safely get a value from a nested dictionary using dot notation.
    
    Args:
        d: Dictionary to get value from
        path: Dot-separated path to the value
        default: Default value if path not found
        sep: Path separator
        
    Returns:
        Value at path or default
    """
    keys = path.split(sep)
    value = d
    
    for key in keys:
        if isinstance(value, dict) and key in value:
            value = value[key]
        else:
            return default
            
    return value


def safe_set(
    d: Dict[str, Any],
    path: str,
    value: Any,
    sep: str = '.'
) -> Dict[str, Any]:
    """
    Safely set a value in a nested dictionary using dot notation.
    
    Args:
        d: Dictionary to set value in
        path: Dot-separated path to the value
        value: Value to set
        sep: Path separator
        
    Returns:
        Modified dictionary
    """
    keys = path.split(sep)
    current = d
    
    for i, key in enumerate(keys[:-1]):
        if key not in current:
            current[key] = {}
        elif not isinstance(current[key], dict):
            raise ValueError(f"Path conflict at key: {key}")
        current = current[key]
        
    current[keys[-1]] = value
    return d


def format_bytes(size_bytes: int) -> str:
    """
    Format bytes into human-readable string.
    
    Args:
        size_bytes: Size in bytes
        
    Returns:
        Formatted string
    """
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024.0
        
    return f"{size_bytes:.2f} PB"


def format_duration(seconds: float) -> str:
    """
    Format duration in seconds to human-readable string.
    
    Args:
        seconds: Duration in seconds
        
    Returns:
        Formatted string
    """
    if seconds < 60:
        return f"{seconds:.2f}s"
    elif seconds < 3600:
        minutes = seconds / 60
        return f"{minutes:.2f}m"
    elif seconds < 86400:
        hours = seconds / 3600
        return f"{hours:.2f}h"
    else:
        days = seconds / 86400
        return f"{days:.2f}d"


def batch_process(
    items: List[T],
    process_func: Callable[[List[T]], Any],
    batch_size: int = 100,
    progress_callback: Optional[Callable[[int, int], None]] = None
) -> List[Any]:
    """
    Process items in batches.
    
    Args:
        items: Items to process
        process_func: Function to process each batch
        batch_size: Size of each batch
        progress_callback: Optional callback for progress updates
        
    Returns:
        List of results from each batch
    """
    results = []
    total_items = len(items)
    
    for i in range(0, total_items, batch_size):
        batch = items[i:i + batch_size]
        result = process_func(batch)
        results.append(result)
        
        if progress_callback:
            progress_callback(i + len(batch), total_items)
            
    return results


async def batch_process_async(
    items: List[T],
    process_func: Callable[[List[T]], Any],
    batch_size: int = 100,
    max_concurrent: int = 10,
    progress_callback: Optional[Callable[[int, int], None]] = None
) -> List[Any]:
    """
    Process items in batches asynchronously with concurrency control.
    
    Args:
        items: Items to process
        process_func: Async function to process each batch
        batch_size: Size of each batch
        max_concurrent: Maximum concurrent batches
        progress_callback: Optional callback for progress updates
        
    Returns:
        List of results from each batch
    """
    semaphore = asyncio.Semaphore(max_concurrent)
    results = []
    total_items = len(items)
    processed = 0
    
    async def process_batch(batch: List[T], index: int) -> Any:
        async with semaphore:
            result = await process_func(batch)
            
            nonlocal processed
            processed += len(batch)
            
            if progress_callback:
                progress_callback(processed, total_items)
                
            return index, result
            
    # Create tasks for all batches
    tasks = []
    for i in range(0, total_items, batch_size):
        batch = items[i:i + batch_size]
        task = process_batch(batch, i // batch_size)
        tasks.append(task)
        
    # Execute all tasks
    batch_results = await asyncio.gather(*tasks)
    
    # Sort results by index to maintain order
    batch_results.sort(key=lambda x: x[0])
    
    return [result for _, result in batch_results]


# Deprecated - use core.caching.memoize instead
from ..core.caching import memoize as memoize_impl

def memoize(func: Callable) -> Callable:
    """
    Simple memoization decorator.
    
    DEPRECATED: Use core.caching.memoize instead, which provides more features
    like maxsize and TTL support.
    
    Args:
        func: Function to memoize
        
    Returns:
        Memoized function
    """
    import warnings
    warnings.warn(
        "memoize is deprecated. Use core.caching.memoize instead.",
        DeprecationWarning,
        stacklevel=2
    )
    
    # Use the new memoize with unlimited cache size
    return memoize_impl(maxsize=None)(func)


def rate_limit(calls: int, period: float):
    """
    Rate limiting decorator.
    
    Args:
        calls: Number of calls allowed
        period: Time period in seconds
    """
    def decorator(func: Callable) -> Callable:
        timestamps = []
        
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            now = time.time()
            
            # Remove old timestamps
            timestamps[:] = [
                t for t in timestamps
                if now - t < period
            ]
            
            if len(timestamps) >= calls:
                sleep_time = period - (now - timestamps[0])
                if sleep_time > 0:
                    await asyncio.sleep(sleep_time)
                    
            timestamps.append(time.time())
            return await func(*args, **kwargs)
            
        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            now = time.time()
            
            # Remove old timestamps
            timestamps[:] = [
                t for t in timestamps
                if now - t < period
            ]
            
            if len(timestamps) >= calls:
                sleep_time = period - (now - timestamps[0])
                if sleep_time > 0:
                    time.sleep(sleep_time)
                    
            timestamps.append(time.time())
            return func(*args, **kwargs)
            
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper
            
    return decorator 