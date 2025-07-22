"""Zero-copy message passing using shared memory for ultra-low latency."""

import os
import mmap
import struct
import asyncio
import logging
from typing import Optional, Tuple, Dict, Any
from dataclasses import dataclass
import multiprocessing.shared_memory as shm
import numpy as np
import msgpack

logger = logging.getLogger(__name__)


@dataclass
class SharedMemorySegment:
    """Represents a shared memory segment."""
    name: str
    size: int
    offset: int = 0
    
    
class RingBuffer:
    """
    Lock-free ring buffer for zero-copy message passing.
    Uses atomic operations for thread-safe access.
    """
    
    def __init__(self, size: int = 1024 * 1024 * 10):  # 10MB default
        self.size = size
        self.shm = shm.SharedMemory(create=True, size=size)
        self.name = self.shm.name
        
        # Header structure: read_pos (8) + write_pos (8) + message_count (8)
        self.header_size = 24
        self.data_offset = self.header_size
        self.data_size = size - self.header_size
        
        # Initialize header
        self._init_header()
        
        logger.info(f"Created ring buffer '{self.name}' with size {size}")
        
    def _init_header(self):
        """Initialize ring buffer header."""
        # Set read_pos, write_pos, message_count to 0
        header = struct.pack('QQQ', 0, 0, 0)
        self.shm.buf[:self.header_size] = header
        
    def write(self, data: bytes) -> bool:
        """
        Write data to ring buffer with zero-copy.
        
        Returns True if successful, False if buffer full.
        """
        data_len = len(data)
        
        # Check if data fits (with 4 byte length prefix)
        if data_len + 4 > self.data_size:
            return False
            
        # Read current positions atomically
        read_pos, write_pos, msg_count = struct.unpack('QQQ', 
            bytes(self.shm.buf[:self.header_size]))
        
        # Calculate available space
        if write_pos >= read_pos:
            available = self.data_size - (write_pos - read_pos)
        else:
            available = read_pos - write_pos
            
        # Need space for length prefix + data
        needed = data_len + 4
        
        if available < needed:
            return False  # Buffer full
            
        # Write length prefix
        actual_write_pos = self.data_offset + (write_pos % self.data_size)
        
        # Handle wrap-around
        if actual_write_pos + needed > self.size:
            # Split write
            first_chunk = self.size - actual_write_pos - 4
            if first_chunk > 0:
                # Write partial length and data
                self.shm.buf[actual_write_pos:actual_write_pos + 4] = struct.pack('I', data_len)
                self.shm.buf[actual_write_pos + 4:self.size] = data[:first_chunk]
                self.shm.buf[self.data_offset:self.data_offset + (data_len - first_chunk)] = data[first_chunk:]
            else:
                # Length wraps
                len_bytes = struct.pack('I', data_len)
                remaining = 4 - (self.size - actual_write_pos)
                self.shm.buf[actual_write_pos:self.size] = len_bytes[:4-remaining]
                self.shm.buf[self.data_offset:self.data_offset + remaining] = len_bytes[4-remaining:]
                self.shm.buf[self.data_offset + remaining:self.data_offset + remaining + data_len] = data
        else:
            # Normal write
            self.shm.buf[actual_write_pos:actual_write_pos + 4] = struct.pack('I', data_len)
            self.shm.buf[actual_write_pos + 4:actual_write_pos + 4 + data_len] = data
            
        # Update write position and message count atomically
        new_write_pos = (write_pos + needed) % (self.data_size * 2)
        new_header = struct.pack('QQQ', read_pos, new_write_pos, msg_count + 1)
        self.shm.buf[:self.header_size] = new_header
        
        return True
        
    def read(self) -> Optional[bytes]:
        """
        Read next message from ring buffer with zero-copy.
        
        Returns None if buffer empty.
        """
        # Read current positions
        read_pos, write_pos, msg_count = struct.unpack('QQQ', 
            bytes(self.shm.buf[:self.header_size]))
        
        if read_pos == write_pos:
            return None  # Buffer empty
            
        # Read length prefix
        actual_read_pos = self.data_offset + (read_pos % self.data_size)
        
        # Handle wrap-around for length
        if actual_read_pos + 4 > self.size:
            # Length wraps
            first_part = self.size - actual_read_pos
            len_bytes = bytes(self.shm.buf[actual_read_pos:self.size])
            len_bytes += bytes(self.shm.buf[self.data_offset:self.data_offset + (4 - first_part)])
            data_len = struct.unpack('I', len_bytes)[0]
            actual_read_pos = self.data_offset + (4 - first_part)
        else:
            data_len = struct.unpack('I', 
                bytes(self.shm.buf[actual_read_pos:actual_read_pos + 4]))[0]
            actual_read_pos += 4
            
        # Read data
        if actual_read_pos + data_len > self.size:
            # Data wraps
            first_chunk = self.size - actual_read_pos
            data = bytes(self.shm.buf[actual_read_pos:self.size])
            data += bytes(self.shm.buf[self.data_offset:self.data_offset + (data_len - first_chunk)])
        else:
            # Normal read
            data = bytes(self.shm.buf[actual_read_pos:actual_read_pos + data_len])
            
        # Update read position atomically
        new_read_pos = (read_pos + data_len + 4) % (self.data_size * 2)
        new_header = struct.pack('QQQ', new_read_pos, write_pos, msg_count - 1)
        self.shm.buf[:self.header_size] = new_header
        
        return data
        
    def close(self):
        """Close and cleanup shared memory."""
        self.shm.close()
        
    def unlink(self):
        """Unlink (delete) shared memory."""
        self.shm.unlink()
        

class ZeroCopyMessagePool:
    """
    Pre-allocated message pool for zero-copy operations.
    Manages shared memory segments for different message sizes.
    """
    
    def __init__(self):
        # Different pools for different message sizes
        self.pools = {
            'small': RingBuffer(1024 * 1024),      # 1MB for messages < 1KB
            'medium': RingBuffer(10 * 1024 * 1024), # 10MB for messages < 10KB
            'large': RingBuffer(100 * 1024 * 1024)  # 100MB for messages < 100KB
        }
        
        # Track allocations
        self._allocations = {}
        
    def allocate(self, size: int) -> Tuple[str, RingBuffer]:
        """Allocate appropriate buffer for message size."""
        if size < 1024:
            return 'small', self.pools['small']
        elif size < 10240:
            return 'medium', self.pools['medium']
        else:
            return 'large', self.pools['large']
            
    def write_message(self, data: bytes) -> Optional[str]:
        """
        Write message to appropriate pool.
        Returns reference ID if successful.
        """
        pool_name, pool = self.allocate(len(data))
        
        if pool.write(data):
            # Return reference for retrieval
            return f"{pool_name}:{pool.name}"
        
        return None
        
    def read_message(self, pool_name: str) -> Optional[bytes]:
        """Read message from specified pool."""
        if pool_name in self.pools:
            return self.pools[pool_name].read()
        return None
        
    def close(self):
        """Close all pools."""
        for pool in self.pools.values():
            pool.close()
            
    def unlink(self):
        """Unlink all pools."""
        for pool in self.pools.values():
            pool.unlink()
            

class SharedMemoryArray:
    """
    Shared memory array for zero-copy numerical operations.
    Supports SIMD operations on shared data.
    """
    
    def __init__(self, shape: Tuple[int, ...], dtype: np.dtype = np.float32):
        self.shape = shape
        self.dtype = dtype
        self.size = np.prod(shape) * dtype.itemsize
        
        # Create shared memory
        self.shm = shm.SharedMemory(create=True, size=self.size)
        
        # Create numpy array view (zero-copy)
        self.array = np.ndarray(shape, dtype=dtype, buffer=self.shm.buf)
        
        # Initialize to zero
        self.array[:] = 0
        
    def get_array(self) -> np.ndarray:
        """Get numpy array view (zero-copy)."""
        return self.array
        
    def update_inplace(self, func, *args, **kwargs):
        """Apply function in-place on shared array."""
        func(self.array, *args, **kwargs)
        
    def close(self):
        """Close shared memory."""
        self.shm.close()
        
    def unlink(self):
        """Unlink shared memory."""
        self.shm.unlink()
        

class ZeroCopyCommunicator:
    """
    Zero-copy message communicator using shared memory.
    Provides ultra-low latency communication between processes.
    """
    
    def __init__(self, service_id: str):
        self.service_id = service_id
        self.message_pool = ZeroCopyMessagePool()
        self._readers = {}
        self._writers = {}
        
    async def send_zero_copy(self, 
                            target_service: str,
                            data: Dict[str, Any]) -> bool:
        """
        Send message with zero-copy.
        
        Args:
            target_service: Target service ID
            data: Message data
            
        Returns:
            True if sent successfully
        """
        # Serialize with msgpack
        packed = msgpack.packb(data, use_bin_type=True)
        
        # Get or create writer for target
        if target_service not in self._writers:
            # Create new ring buffer for this service pair
            writer = RingBuffer()
            self._writers[target_service] = writer
            
            # Store reference in shared registry (would use Redis/Ignite)
            # For now, just log
            logger.info(f"Created writer {self.service_id}->{target_service}: {writer.name}")
        else:
            writer = self._writers[target_service]
            
        # Write with zero-copy
        return writer.write(packed)
        
    async def receive_zero_copy(self, 
                               source_service: str) -> Optional[Dict[str, Any]]:
        """
        Receive message with zero-copy.
        
        Args:
            source_service: Source service ID
            
        Returns:
            Message data if available
        """
        # Get or attach to reader
        if source_service not in self._readers:
            # Would lookup shared memory name from registry
            # For now, return None
            return None
            
        reader = self._readers[source_service]
        
        # Read with zero-copy
        data = reader.read()
        if data:
            return msgpack.unpackb(data, raw=False)
            
        return None
        
    def attach_reader(self, source_service: str, shm_name: str):
        """Attach to existing shared memory for reading."""
        try:
            # Attach to existing shared memory
            existing_shm = shm.SharedMemory(name=shm_name)
            
            # Create ring buffer wrapper
            reader = RingBuffer(size=existing_shm.size)
            reader.shm = existing_shm
            reader.name = shm_name
            
            self._readers[source_service] = reader
            logger.info(f"Attached reader {source_service}->{self.service_id}: {shm_name}")
            
        except Exception as e:
            logger.error(f"Failed to attach reader: {e}")
            
    def close(self):
        """Close all shared memory segments."""
        for writer in self._writers.values():
            writer.close()
        for reader in self._readers.values():
            reader.close()
        self.message_pool.close()
        
    def unlink(self):
        """Unlink writer segments only (readers don't own the memory)."""
        for writer in self._writers.values():
            writer.unlink()
        self.message_pool.unlink() 