import hashlib
import json
import os
import tempfile
import time

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Callable

import pytest

from nkpylib.cacher.backends import CacheBackend, MemoryBackend
from nkpylib.cacher.constants import CacheNotFound
from nkpylib.cacher.file_utils import _write_atomic, _read_file
from nkpylib.cacher.formatters import JsonFormatter
from nkpylib.cacher.keyers import (
    TupleKeyer, StringKeyer, HashStringKeyer, HashBytesKeyer
)
from nkpylib.cacher.strategies import CacheStrategy

from .test_functions import (
    ExpensiveClass, fibonacci, fetch_url_size, random_choice
)

@pytest.fixture
def memory_backend():
    """Create a MemoryBackend with JSON formatter."""
    return MemoryBackend(formatter=JsonFormatter())

@pytest.fixture
def test_url():
    """URL that should be stable and relatively quick to fetch."""
    return "https://www.example.com"

@pytest.fixture
def expensive_instance():
    """Create an ExpensiveClass instance."""
    return ExpensiveClass(multiplier=2)

class TestCacheBackend(ABC):
    """Base test class for all cache backends."""

    @pytest.fixture
    @abstractmethod
    def backend(self):
        """Default backend fixture that should be overridden by subclasses."""
        raise NotImplementedError("Subclasses must provide a backend fixture")

    def test_basic_get_set(self, backend: CacheBackend):
        """Test basic get/set operations."""
        backend.set('key1', 'value1')
        assert backend.get('key1') == 'value1'

        # Test overwrite
        backend.set('key1', 'value2')
        assert backend.get('key1') == 'value2'

        # Test missing key
        with pytest.raises(CacheNotFound):
            backend.get('nonexistent')

    def test_delete(self, backend: CacheBackend):
        """Test delete operation."""
        backend.set('key1', 'value1')
        assert backend.get('key1') == 'value1'

        backend.delete('key1')
        with pytest.raises(CacheNotFound):
            backend.get('key1')

        # Delete nonexistent key should not raise
        backend.delete('nonexistent')

    def test_clear(self, backend: CacheBackend):
        """Test clear operation."""
        backend.set('key1', 'value1')
        backend.set('key2', 'value2')

        backend.clear()
        with pytest.raises(CacheNotFound):
            backend.get('key1')
            backend.get('key2')

    def test_stats(self, backend: CacheBackend):
        """Test hit/miss statistics."""
        # Test miss
        with pytest.raises(CacheNotFound):
            backend.get('key1')
        assert backend.get_stats()['misses'] == 1

        # Test hit
        backend.set('key1', 'value1')
        assert backend.get('key1') == 'value1'
        print(backend.get_stats())
        assert backend.get_stats()['misses'] == 1
        assert backend.get_stats()['hits'] == 1

    def test_function_caching(self, backend: CacheBackend):
        """Test caching a pure function."""
        cached_fib = backend.__class__(
            fn=fibonacci,
            formatter=backend.formatter
        )

        # First call should compute
        result1 = cached_fib(10)
        assert result1 == fibonacci(10)
        assert cached_fib.get_stats()['misses'] == 1

        # Second call should hit cache
        result2 = cached_fib(10)
        assert result2 == result1
        assert cached_fib.get_stats()['hits'] == 1

    def test_method_caching(self, backend: CacheBackend, expensive_instance):
        """Test caching an instance method."""
        cached_method = backend.__class__(
            fn=expensive_instance.expensive_method,
            formatter=backend.formatter
        )

        # First call should compute
        result1 = cached_method(3, 4)
        assert result1 == 24  # (3 * 4) * multiplier(2)
        assert cached_method.get_stats()['misses'] == 1

        # Second call should hit cache
        result2 = cached_method(3, 4)
        assert result2 == result1
        assert cached_method.get_stats()['hits'] == 1

    def test_network_caching(self, backend: CacheBackend, test_url):
        """Test caching network requests."""
        cached_fetch = backend.__class__(
            fn=fetch_url_size,
            formatter=backend.formatter
        )

        # First call should fetch
        size1 = cached_fetch(test_url)
        assert isinstance(size1, int)
        assert cached_fetch.get_stats()['misses'] == 1

        # Second call should hit cache
        size2 = cached_fetch(test_url)
        assert size2 == size1
        assert cached_fetch.get_stats()['hits'] == 1

    def test_nondeterministic_caching(self, backend: CacheBackend):
        """Test caching non-deterministic function."""
        items = ['a', 'b', 'c']
        cached_choice = backend.__class__(
            fn=random_choice,
            formatter=backend.formatter
        )

        # First call gets random choice
        result1 = cached_choice(items)
        assert result1 in items
        assert cached_choice.get_stats()['misses'] == 1

        # Subsequent calls should return same value
        for _ in range(10):
            assert cached_choice(items) == result1
        assert cached_choice.get_stats()['hits'] == 10

class TestMemoryBackend(TestCacheBackend):
    """Test MemoryBackend specific functionality."""

    @pytest.fixture
    def backend(self, memory_backend):
        """Provide backend for base class tests."""
        return memory_backend

    def test_memory_persistence(self, memory_backend):
        """Test that MemoryBackend persists only in memory."""
        memory_backend.set('key1', 'value1')

        # New instance should start empty
        new_backend = MemoryBackend(formatter=JsonFormatter())
        with pytest.raises(CacheNotFound):
            new_backend.get('key1')

def test_write_atomic_creates_file():
    """Test that _write_atomic creates a file with the correct contents."""
    with tempfile.TemporaryDirectory() as tmpdir:
        path = Path(tmpdir) / "new_dir" / "test.txt"
        data = b"test data"

        _write_atomic(path, data)

        # Check file exists and has correct contents
        assert path.exists()
        assert path.read_bytes() == data

        # Check no temporary files were left behind
        temp_files = [f for f in os.listdir(tmpdir) if f.startswith("test.txt.")]
        assert not temp_files

def test_write_atomic_overwrites_existing():
    """Test that _write_atomic safely overwrites an existing file."""
    with tempfile.TemporaryDirectory() as tmpdir:
        path = Path(tmpdir) / "test.txt"

        # Create initial file
        path.write_bytes(b"initial data")

        # Overwrite with new data
        new_data = b"new data"
        _write_atomic(path, new_data)

        # Check file has new contents
        assert path.read_bytes() == new_data

def test_read_file_success():
    """Test that _read_file successfully reads existing file."""
    with tempfile.TemporaryDirectory() as tmpdir:
        path = Path(tmpdir) / "test.txt"
        data = b"test data"
        path.write_bytes(data)

        assert _read_file(path) == data

def test_read_file_missing():
    """Test that _read_file returns None for missing file."""
    with tempfile.TemporaryDirectory() as tmpdir:
        path = Path(tmpdir) / "nonexistent.txt"

        assert _read_file(path) is None

def test_read_file_permission_error():
    """Test that _read_file returns None for unreadable file."""
    with tempfile.TemporaryDirectory() as tmpdir:
        path = Path(tmpdir) / "unreadable.txt"
        path.write_bytes(b"secret data")
        path.chmod(0)  # Remove all permissions

        assert _read_file(path) is None

# Keyer Tests
def test_tuple_keyer_basic():
    """Test TupleKeyer with basic types."""
    keyer = TupleKeyer()
    key = keyer.make_key((1, "test"), {"x": 2})
    assert isinstance(key, tuple)
    assert key[0] == 1
    assert key[1] == "test"
    assert isinstance(key[2], frozenset)

def test_tuple_keyer_nested():
    """Test TupleKeyer with nested structures."""
    keyer = TupleKeyer()
    args = ([1, 2], {"a": [3, 4]}, {5, 6})
    kwargs = {"x": [7, 8], "y": {"b": 9}}
    key = keyer.make_key(args, kwargs)

    # Check types are converted
    assert isinstance(key[0], tuple)  # list -> tuple
    assert isinstance(key[1], frozenset)  # dict -> frozenset
    assert isinstance(key[2], frozenset)  # set -> frozenset
    assert isinstance(key[3], frozenset)  # kwargs -> frozenset

def test_tuple_keyer_unhashable():
    """Test TupleKeyer with unhashable objects."""
    class UnhashableObject:
        def __hash__(self):
            raise TypeError("unhashable")

    keyer = TupleKeyer()
    obj = UnhashableObject()
    with pytest.raises(TypeError):
        key = keyer.make_key((obj,), {})

def test_string_keyer():
    """Test StringKeyer produces consistent string keys."""
    keyer = StringKeyer()
    key1 = keyer.make_key((1, "test"), {1: 'a', "x": 2})
    key2 = keyer.make_key((1, "test"), {"x": 2, 1: 'a'})

    assert isinstance(key1, str)
    assert key1 == key2

def test_hash_string_keyer_builtin():
    """Test HashStringKeyer with builtin hash function."""
    keyer = HashStringKeyer('sha256')
    key = keyer.make_key((1, "test"), {"x": 2})

    assert isinstance(key, str)
    assert len(key) == 64  # sha256 hex digest length

def test_hash_string_keyer_custom():
    """Test HashStringKeyer with custom hash function."""
    def custom_hash(s: str) -> str:
        return 'hash_' + s

    keyer = HashStringKeyer(custom_hash)
    key = keyer.make_key((1,), {})
    assert key.startswith('hash_')

def test_hash_bytes_keyer_builtin():
    """Test HashBytesKeyer with builtin hash function."""
    keyer = HashBytesKeyer('sha256')
    key = keyer.make_key((1, "test"), {"x": 2})

    assert isinstance(key, bytes)
    assert len(key) == 32  # sha256 raw digest length

def test_hash_bytes_keyer_custom():
    """Test HashBytesKeyer with custom hash function."""
    def custom_hash(s: str) -> bytes:
        return b'hash_' + s.encode()

    keyer = HashBytesKeyer(custom_hash)
    key = keyer.make_key((1,), {})
    assert key.startswith(b'hash_')

def test_hash_keyer_invalid_algorithm():
    """Test HashKeyers raise error for invalid hash algorithm."""
    with pytest.raises(ValueError):
        HashStringKeyer('invalid_algorithm')

    with pytest.raises(ValueError):
        HashBytesKeyer('invalid_algorithm')

# Formatter Tests
def test_json_formatter_basic():
    """Test basic JSON serialization/deserialization."""
    formatter = JsonFormatter()
    obj = {'a': 1, 'b': [2, 3], 'c': {'d': 4}}

    data = formatter.dumps(obj)
    assert isinstance(data, bytes)

    decoded = formatter.loads(data)
    assert decoded == obj

def test_json_formatter_custom_encoder():
    """Test JSON formatter with custom encoder."""
    class CustomEncoder(json.JSONEncoder):
        def default(self, obj):
            if isinstance(obj, set):
                return list(obj)
            return super().default(obj)

    formatter = JsonFormatter(EncoderCls=CustomEncoder)
    obj = {'a': {1, 2, 3}}  # Sets aren't normally JSON serializable

    data = formatter.dumps(obj)
    decoded = formatter.loads(data)
    assert decoded == {'a': [1, 2, 3]}

def test_json_formatter_custom_decoder():
    """Test JSON formatter with custom decoder."""
    class CustomDecoder(json.JSONDecoder):
        def decode(self, s):
            obj = super().decode(s)
            # Convert all lists to tuples
            if isinstance(obj, list):
                return tuple(obj)
            if isinstance(obj, dict):
                return {k: tuple(v) if isinstance(v, list) else v for k, v in obj.items()}
            return obj

    formatter = JsonFormatter(DecoderCls=CustomDecoder)
    obj = {'a': [1, 2, 3]}

    data = formatter.dumps(obj)
    decoded = formatter.loads(data)
    assert decoded == {'a': (1, 2, 3)}

def test_json_formatter_invalid_input():
    """Test JSON formatter with invalid input."""
    formatter = JsonFormatter()

    # Test invalid JSON bytes
    with pytest.raises(json.JSONDecodeError):
        formatter.loads(b'invalid json')

    # Test non-serializable object
    class UnserializableObject:
        pass

    with pytest.raises(TypeError):
        formatter.dumps(UnserializableObject())
