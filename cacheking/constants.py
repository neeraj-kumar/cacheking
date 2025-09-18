from __future__ import annotations

from typing import TypeVar

# type for cache keys
KeyT = TypeVar('KeyT')

# type for hash function outputs
HashT = TypeVar('HashT', str, bytes, int)

class CacheNotFound(Exception):
    """Exception raised when a cache key is not found."""
    def __init__(self, key: KeyT):
        super().__init__(f"Cache key '{key}' not found")
        self.key = key
