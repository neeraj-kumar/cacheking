"""Cache strategies: ways to modify caching behavior."""

from __future__ import annotations

import atexit
import queue
import random
import sys
import threading
import time

from abc import ABC
from collections import OrderedDict, defaultdict
from typing import Any, Callable, Generic, Literal, TypeVar, Union

from nkpylib.cacher.constants import KeyT

class CacheStrategy(Generic[KeyT]):
    """Base class for cache strategies.

    Strategies can hook into different stages of cache operations:
    - pre_get: Before retrieving a value
    - post_get: After retrieving a value
    - pre_set: Before setting a value
    - post_set: After setting a value
    - pre_delete: Before deleting a value
    - post_delete: After deleting a value
    """
    def __init__(self, backend=None):
        self._backend = backend
        if backend is not None:
            self.initialize()

    def initialize(self) -> None:
        """Initialize strategy with existing items from backend.

        Called when backend is set, allowing strategies to scan existing items.
        Override this in subclasses that need to track existing items.
        """
        pass

    def pre_get(self, key: KeyT) -> bool:
        """Called before retrieving a value.

        Returns:
            False to skip cache lookup, True to proceed
        """
        return True

    def post_get(self, key: KeyT, value: Any) -> Any:
        """Called after retrieving a value.

        Args:
            key: The cache key
            value: The retrieved value

        Returns:
            Potentially modified value
        """
        return value

    def pre_set(self, key: KeyT, value: Any) -> bool:
        """Called before setting a value.

        Returns:
            False to skip caching, True to proceed
        """
        return True

    def post_set(self, key: KeyT, value: Any) -> None:
        """Called after setting a value."""
        pass

    def pre_delete(self, key: KeyT) -> bool:
        """Called before deleting a value. Returns False to skip deletion, True to proceed"""
        return True

    def post_delete(self, key: KeyT) -> None:
        """Called after deleting a value."""
        pass

    def pre_clear(self) -> bool:
        """Called before clearing all entries. Returns False to skip clearing, True to proceed"""
        return True

    def post_clear(self) -> None:
        """Called after clearing all entries."""
        pass


class RateLimiter(CacheStrategy[KeyT]):
    """Strategy that enforces a global minimum interval between operations.

    This is useful for rate-limiting API calls or other resources that need
    to be accessed with some delay between requests, regardless of which key
    is being accessed.
    """
    def __init__(self, min_interval: float|Callable[[KeyT], float]):
        """Initialize with minimum interval between requests.

        Args:
        - min_interval: Minimum time (in seconds) between any operations, or a function that takes
          in a key and returns the interval to use.
        """
        self.min_interval = min_interval
        self.last_request_time = 0.0

    def pre_get(self, key: KeyT) -> bool:
        now = time.time()
        elapsed = now - self.last_request_time
        min_interval = self.min_interval(key) if callable(self.min_interval) else self.min_interval
        if elapsed < min_interval:
            time.sleep(min_interval - elapsed)
        self.last_request_time = time.time()
        return True


class TTLPolicy(CacheStrategy[KeyT]):
    """Strategy that enforces a time-to-live (TTL) on cached items.

    Items older than the TTL are considered invalid and will be re-fetched.
    """
    def __init__(self, ttl_seconds: float|Callable[[KeyT], float], *, delete_expired: bool = False):
        """Initialize with TTL duration.

        Args:
        - ttl_seconds: Time-to-live in seconds for cached items or a function that takes in a key
          and returns the TTL to use.
        - delete_expired: If True, automatically delete expired items when found. If False, just
          force a cache miss (default).
        """
        super().__init__()
        self.ttl = ttl_seconds
        self.delete_expired = delete_expired
        self.timestamps: dict[Any, float] = {}

    def initialize(self) -> None:
        """Initialize timestamps for existing items in backend."""
        now = time.time()
        for key in self._backend.iter_keys():
            self.timestamps[key] = now

    def pre_get(self, key: KeyT) -> bool:
        if key in self.timestamps:
            age = time.time() - self.timestamps[key]
            ttl = self.ttl(key) if callable(self.ttl) else self.ttl
            if age > ttl:
                if self.delete_expired:
                    # Delete expired item from both timestamp dict and backend
                    del self.timestamps[key]
                    self._backend._delete_value(key)
                return False  # Skip cache lookup, forcing a miss
        return True

    def post_set(self, key: KeyT, value: Any) -> None:
        self.timestamps[key] = time.time()

    def post_delete(self, key: KeyT) -> None:
        self.timestamps.pop(key, None)

    def post_clear(self) -> None:
        self.timestamps.clear()


class DelayedWriteStrategy(CacheStrategy[KeyT]):
    """Strategy that delays cache writes for better performance.

    Two modes are supported:
    - "background": Writes happen in a background thread (good for slow backends)
    - "memory": Writes are cached in memory and flushed periodically (good for fast access)

    You can also configure:
    - batch_size: How many writes to batch together (<=1 means one at a time)
    - flush_interval: How often to flush to backend (<=0 means immediate)
    """
    def __init__(self,
                 mode: str = "background",
                 batch_size: int = 1,
                 flush_interval: float = 0.0):
        """Initialize with write mode and timing settings.

        Args:
            mode: Either "background" (thread) or "memory" (cache)
            batch_size: Number of writes to batch (<=1 means one at a time)
            flush_interval: Seconds between flushes (<=0 means immediate)
        """
        self.mode = mode
        self.batch_size = max(1, batch_size)
        self.flush_interval = max(0.0, flush_interval)

        if mode == "background": # For background mode
            self.write_queue: queue.Queue[tuple[str, KeyT, Any]] = queue.Queue(maxsize=batch_size)
            self.stop_event = threading.Event()
            self.worker = threading.Thread(target=self._worker, daemon=True)
            self.worker.start()
            atexit.register(self.shutdown)
            self._batch: list[tuple[str, KeyT, Any]] = []
        elif mode == 'memory': # For memory mode
            self._cache: dict[KeyT, Any] = {}
            self._dirty = set()
            self._last_flush = time.time()
        else:
            raise NotImplementedError(f"Unsupported mode: {mode}")

    def _worker(self):
        """Background thread that processes writes."""
        batch: list[tuple[str, KeyT, Any]] = []
        last_flush = time.time()
        while not self.stop_event.is_set():
            try:
                # Get item with timeout to check stop_event
                try:
                    op, key, value = self.write_queue.get(timeout=0.1)
                    batch.append((op, key, value))
                    self.write_queue.task_done()
                except queue.Empty:
                    pass

                # Check if we should flush
                should_flush = (
                    len(batch) >= self.batch_size or
                    (self.flush_interval > 0 and time.time() - last_flush >= self.flush_interval)
                )
                if should_flush and batch:
                    # Collect operations into chunks by type
                    chunks: list[tuple[str, list]] = []
                    current_type = None
                    current_chunk = []
                    for op, key, value in batch:
                        if op != current_type:
                            if current_chunk:
                                chunks.append((current_type, current_chunk))
                            current_type = op
                            current_chunk = []
                        if op != 'clear':
                            current_chunk.append((key, value))
                    if current_chunk:
                        chunks.append((current_type, current_chunk))
                    # Process all chunks in order
                    for op_type, chunk in chunks:
                        if op_type == 'set':
                            self._backend.set_many(dict(chunk))
                        elif op_type == 'delete':
                            self._backend.delete_many([k for k,v in chunk])
                        elif op_type == 'clear':
                            self._backend._clear()
                    batch.clear()
                    last_flush = time.time()
            except Exception as e:
                print(f"Error in delayed write worker: {e}")
                batch.clear()

    def pre_get(self, key: KeyT) -> bool:
        """Check memory cache first in memory mode."""
        if self.mode == "memory" and key in self._cache:
            return False
        return True

    def post_get(self, key: KeyT, value: Any) -> Any:
        """Return from memory cache in memory mode."""
        if self.mode == "memory" and key in self._cache:
            return self._cache[key]
        return value

    def pre_set(self, key: KeyT, value: Any) -> bool:
        """Handle write based on mode."""
        if self.mode == "background":
            try:
                self.write_queue.put(('set', key, value), block=False)
                return False
            except queue.Full:
                return True
        else:  # memory mode
            self._cache[key] = value
            self._dirty.add(key)
            self._maybe_flush()
            return False

    def pre_delete(self, key: KeyT) -> bool:
        """Handle delete based on mode."""
        if self.mode == "background":
            try:
                self.write_queue.put(('delete', key, None), block=False)
                return False
            except queue.Full:
                return True
        else:  # memory mode
            if key in self._cache:
                del self._cache[key]
            self._dirty.add(key)
            self._maybe_flush()
            return False

    def pre_clear(self) -> bool:
        """Handle clear based on mode."""
        if self.mode == "background":
            # Clear queue
            while not self.write_queue.empty():
                try:
                    self.write_queue.get_nowait()
                    self.write_queue.task_done()
                except queue.Empty:
                    break
            self.write_queue.put(('clear', None, None), block=False)
            return False
        else:  # memory mode
            self._cache.clear()
            self._dirty.clear()
            self._backend._clear()
            self._last_flush = time.time()
            return False

    def _maybe_flush(self):
        """Check if we should flush memory cache."""
        if not self.flush_interval:
            self.flush()
        elif time.time() - self._last_flush >= self.flush_interval:
            self.flush()

    def flush(self):
        """Flush pending writes to backend."""
        if self.mode == "memory" and self._dirty:
            for key in self._dirty:
                if key in self._cache:
                    self._backend._set_value(key, self._cache[key])
            self._dirty.clear()
            self._last_flush = time.time()

    def shutdown(self, timeout: float|None = None):
        """Clean shutdown of background thread or memory cache."""
        if self.mode == "background":
            # Process remaining items and stop thread
            self.write_queue.join()
            self.stop_event.set()
            if timeout is not None:
                self.worker.join(timeout)
            else:
                self.worker.join()
        else:  # memory mode
            self.flush()


RevKey = Union[str, int]  # Type for revision keys (string names or integer indices)

class RevisionStrategy(CacheStrategy[KeyT]):
    """Strategy that maintains a history of revisions for each cached value.

    Keeps track of multiple versions of each value with timestamps.
    Supports both automatic numbered revisions (limited by max_revisions)
    and named/pinned revisions that are exempt from the limit.

    Operations:
    - Get latest revision (default behavior)
    - Get specific revision by index (-1 is latest, -2 is previous, etc.)
    - Get named/pinned revision by string name
    - Pin current or new revision with a name
    - Unpin named revision
    - List all revisions (both numbered and named)
    - Clear only unpinned revisions

    Example:
        rev_strategy = RevisionStrategy(max_revisions=5)
        cache = SQLBackend(strategies=[rev_strategy])

        # Normal revisions (limited by max_revisions)
        cache.set('key', 'v1')
        cache.set('key', 'v2')

        # Pin important versions (not limited)
        rev_strategy.pin_revision('key', 'stable', 'v1')
        rev_strategy.pin_revision('key', 'beta', 'v2')

        # Access versions
        stable = rev_strategy.get_revision('key', 'stable')  # v1
        beta = rev_strategy.get_revision('key', 'beta')      # v2
        latest = cache.get('key')                           # v2

        # Remove pin when no longer needed
        rev_strategy.unpin_revision('key', 'beta')
    """
    def __init__(self, max_revisions: int = 10):
        """Initialize with maximum number of revisions to keep.

        Args:
        - max_revisions: Maximum number of revisions to keep per key (default 10).
          Note: Pinned revisions don't count towards this limit
        """
        super().__init__()
        self.max_revisions = max_revisions
        # Key -> {name|index -> (timestamp, value)}
        # String names are pinned revisions (exempt from limit)
        # Integer indices are automatic revisions (follow limit)
        self.revisions: dict[KeyT, dict[RevKey, tuple[float, Any]]] = {}

    def get_revision(self, key: KeyT, rev: RevKey) -> Any:
        """Get a specific revision of a value.

        Args:
        - key: Cache key
        - rev: Either:
          - Revision index (-1 is latest, -2 is previous, etc.)
          - Name of pinned revision ("stable", "experimental", etc.)

        Returns the value at that revision, or CACHE_MISS if not found
        """
        if key not in self.revisions:
            return self._backend.CACHE_MISS
        try:
            return self.revisions[key][rev][1]
        except (KeyError, IndexError):
            return self._backend.CACHE_MISS

    def list_revisions(self, key: KeyT) -> dict[RevKey, tuple[float, Any]]:
        """Get all revisions for a key.

        Args:
            key: Cache key

        Returns:
            Dict mapping revision names/indices to (timestamp, value) tuples
        """
        return dict(self.revisions.get(key, {}))

    def pin_revision(self, key: KeyT, name: str, value: Any|None = None) -> None:
        """Pin current or new revision with a name.

        Args:
            key: Cache key
            name: Name to give this revision
            value: Optional new value to pin. If None, pins current value.
        """
        if key not in self.revisions:
            if value is None:
                raise KeyError(f"No revisions exist for key {key}")
            self.revisions[key] = {}

        if value is None:
            # Pin current value
            latest_idx = max(idx for idx in self.revisions[key].keys() if isinstance(idx, int))
            value = self.revisions[key][latest_idx][1]

        self.revisions[key][name] = (time.time(), value)

    def unpin_revision(self, key: KeyT, name: str) -> None:
        """Remove a pinned revision.

        Args:
            key: Cache key
            name: Name of revision to unpin
        """
        if key in self.revisions:
            self.revisions[key].pop(name, None)

    def pre_get(self, key: KeyT) -> bool:
        """Check if we have any revisions."""
        if key in self.revisions:
            return False  # Skip backend lookup
        return True

    def post_get(self, key: KeyT, value: Any) -> Any:
        """Return latest revision if we have one, otherwise use backend value."""
        if key in self.revisions:
            # Find latest numeric revision
            latest_idx = max(idx for idx in self.revisions[key].keys() if isinstance(idx, int))
            return self.revisions[key][latest_idx][1]
        if value != self._backend.CACHE_MISS:
            # Initialize revisions from backend
            self.revisions[key] = {0: (time.time(), value)}
        return value

    def pre_set(self, key: KeyT, value: Any) -> bool:
        """Add new revision."""
        if key not in self.revisions:
            self.revisions[key] = {}
            next_idx = 0
        else:
            # Find next available index
            numeric_keys = [k for k in self.revisions[key].keys() if isinstance(k, int)]
            next_idx = max(numeric_keys) + 1 if numeric_keys else 0
        self.revisions[key][next_idx] = (time.time(), value)
        # Trim to max revisions (only numeric indices count)
        numeric_revs = sorted((k,v) for k,v in self.revisions[key].items() if isinstance(k, int))
        if len(numeric_revs) > self.max_revisions: # Keep newest revisions
            for idx, _ in numeric_revs[:-self.max_revisions]:
                del self.revisions[key][idx]
        return True  # Still write to backend

    def pre_delete(self, key: KeyT) -> bool:
        """Clear all revisions (including pinned) when key is deleted."""
        if key in self.revisions:
            del self.revisions[key]
        return True

    def pre_clear(self) -> bool:
        """Clear all revisions (including pinned)."""
        self.revisions.clear()
        return True

    def clear_unpinned(self, key: KeyT|None = None) -> None:
        """Clear only unpinned revisions for one or all keys.

        Args:
        - key: Specific key to clear, or None to clear all keys
        """
        if key is not None:
            if key in self.revisions:
                # Keep only string (pinned) keys
                self.revisions[key] = {
                    k: v for k,v in self.revisions[key].items()
                    if isinstance(k, str)
                }
        else:
            for k in self.revisions:
                self.clear_unpinned(k)


class LimitStrategy(CacheStrategy[KeyT]):
    """Strategy that enforces limits on the cache using customizable metrics.

    Uses a metric function to compute a value for each item, and an aggregation
    function to combine those values. When the aggregate exceeds the `limit`,
    items are evicted according to the chosen `eviction` policy:
    - 'lru': Least recently used items are removed first
    - 'fifo': First in, first out
    - 'random': Random items are removed
    - 'lfu': Least frequently used items are removed first
    - 'lifo': Last in, first out (newest items removed first)

    You can use the various classmethods to create common limit strategies:
    - `with_count_limit(max_items)`: Limit total number of items
    - `with_size_limit(max_bytes)`: Limit total size in bytes
    - `with_age_limit(max_age)`: Limit maximum age of items in seconds
    - `with_idle_limit(max_idle)`: Evict items not accessed for max_idle seconds
    - `with_mem_percent_limit(max_percent)`: Evict items when memory usage exceeds threshold
    - `with_frequency_limit(min_accesses, window)`: Keep frequently accessed items

    Note that you can create multiple instances of this strategy with different functions to have
    multiple kinds of limits enforced simultaneously.
    """
    def __init__(self,
                 metric_fn: Callable[[KeyT, Any], float],
                 agg_fn: Callable[[list[float]], float],
                 limit: float,
                 eviction: Literal['lru', 'fifo', 'random', 'lfu', 'lifo'] = 'lru'):
        """Initialize with metric function and limit.

        Args:
            metric_fn: Function that takes (key, value) and returns a float
            agg_fn: Function that takes list of metric values and returns aggregate
            limit: Maximum allowed value for the aggregate
            eviction: Eviction policy to use when limit is exceeded
        """
        super().__init__()
        self.metric_fn = metric_fn
        self.agg_fn = agg_fn
        self.limit = limit
        self.eviction = eviction

        # Track items and their metadata
        self.items: OrderedDict[KeyT, dict] = OrderedDict()
        self.total_metric = 0.0

        # Track access counts for LFU
        self.access_counts: dict[KeyT, int] = defaultdict(int)

    @classmethod
    def with_count_limit(cls, max_items: int, **kwargs) -> LimitStrategy:
        """Create a strategy that limits total number of items."""
        return cls(
            metric_fn=lambda k,v: 1,
            agg_fn=sum,
            limit=max_items,
            **kwargs
        )

    @classmethod
    def with_size_limit(cls, max_bytes: int, **kwargs) -> LimitStrategy:
        """Create a strategy that limits total size in bytes."""
        return cls(
            metric_fn=lambda k,v: sys.getsizeof(v),
            agg_fn=sum,
            limit=max_bytes,
            **kwargs
        )

    @classmethod
    def with_age_limit(cls, max_age: float, **kwargs) -> LimitStrategy:
        """Create a strategy that limits maximum item age in seconds."""
        return cls(
            metric_fn=lambda k,v: time.time() - v['time'],
            agg_fn=max,
            limit=max_age,
            **kwargs
        )

    @classmethod
    def with_idle_limit(cls, max_idle: float, **kwargs) -> LimitStrategy:
        """Create a strategy that evicts items not accessed for max_idle seconds.

        Args:
            max_idle: Maximum time in seconds since last access
            **kwargs: Additional arguments for LimitStrategy
        """
        # Create a strategy instance with access tracking
        strategy = cls(
            metric_fn=lambda k,v: time.time() - strategy.last_access.get(k, 0),
            agg_fn=max,
            limit=max_idle,
            **kwargs
        )
        # Add access time tracking
        strategy.last_access = {}

        # Wrap pre_get to update access times
        original_pre_get = strategy.pre_get
        def wrapped_pre_get(key: KeyT) -> bool:
            strategy.last_access[key] = time.time()
            return original_pre_get(key)

        strategy.pre_get = wrapped_pre_get

        return strategy

    @classmethod
    def with_mem_percent_limit(cls, max_percent: float, **kwargs) -> LimitStrategy:
        """Create a strategy that evicts items when memory usage exceeds threshold.

        Requires the `psutil` library.

        Args:
            max_percent: Maximum memory usage as percentage (0-100)
            **kwargs: Additional arguments for LimitStrategy
        """
        import psutil
        return cls(
            metric_fn=lambda k,v: psutil.Process().memory_percent(),
            agg_fn=max,
            limit=max_percent,
            **kwargs
        )

    @classmethod
    def with_frequency_limit(cls, min_accesses: int, window: float, **kwargs) -> LimitStrategy:
        """Create a strategy that keeps frequently accessed items.

        Args:
            min_accesses: Minimum number of accesses required in window
            window: Time window in seconds to count accesses
            **kwargs: Additional arguments for LimitStrategy
        """
        def _get_access_count(k: Any, v: Any) -> float:
            if not hasattr(v, '__dict__'):
                v.__dict__ = {'access_times': []}
            times = v.__dict__.get('access_times', [])
            # Add current access
            times.append(time.time())
            # Remove old accesses
            cutoff = time.time() - window
            times = [t for t in times if t > cutoff]
            v.__dict__['access_times'] = times
            # Return negative count so max aggregation works for minimum threshold
            return -len(times)

        return cls(
            metric_fn=_get_access_count,
            agg_fn=max,
            limit=-min_accesses,  # Negative since we're using negative counts
            **kwargs
        )

    def initialize(self) -> None:
        """Initialize tracking for existing items in backend."""
        for key in self._backend.iter_keys():
            value = self._backend._get_value(key)
            if value != self._backend.CACHE_MISS:
                self.items[key] = value
        # If we're over limit, start evicting
        current = self._get_total_metric()
        if current > self.limit:
            self._evict_items()

    def _get_metric(self, key: KeyT, value: Any) -> float:
        """Compute metric value for an item."""
        try:
            return float(self.metric_fn(key, value))
        except Exception as e:
            print(f"Error computing metric for {key}: {e}")
            return 0.0

    def _get_total_metric(self) -> float:
        """Compute current total metric value."""
        try:
            values = [self._get_metric(k, v) for k,v in self.items.items()]
            return self.agg_fn(values) if values else 0.0
        except Exception as e:
            print(f"Error computing aggregate metric: {e}")
            return 0.0

    def _evict_items(self, needed: float = 0.0) -> None:
        """Remove items until limit is satisfied."""
        while self.items:
            current = self._get_total_metric()
            if current + needed <= self.limit:
                break

            # Choose item to evict based on policy
            if self.eviction == 'random':
                key = random.choice(list(self.items.keys()))
            elif self.eviction == 'lfu':
                # Find item with lowest access count
                key = min(self.items.keys(), key=lambda k: self.access_counts[k])
            elif self.eviction == 'lifo':
                # Remove newest item (from end of OrderedDict)
                key = next(reversed(self.items))
            else:  # lru and fifo both remove from start of OrderedDict
                key = next(iter(self.items))

            # Remove the item
            self.items.pop(key)
            self._backend._delete_value(key)
            self.stats['evictions'] += 1

    def pre_set(self, key: KeyT, value: Any) -> bool:
        """Check limit before setting value."""
        # Calculate metric for new value
        new_metric = self._get_metric(key, value)
        # If item exists, remove it from consideration
        if key in self.items:
            old_metric = self._get_metric(key, self.items[key])
            self.items.pop(key)
        # Check if we need to evict items
        current = self._get_total_metric()
        if current + new_metric > self.limit:
            self._evict_items(new_metric)
        # Update tracking
        self.items[key] = value
        # Move to end if using LRU
        if self.eviction == 'lru':
            self.items.move_to_end(key)
        return True

    def pre_get(self, key: KeyT) -> bool:
        """Update access tracking and check metric."""
        if key in self.items:
            # Check if item exceeds limit
            if self._get_metric(key, self.items[key]) > self.limit:
                self.items.pop(key)
                if self.eviction == 'lfu':
                    del self.access_counts[key]
                return False
            # Update access tracking
            if self.eviction == 'lru':
                self.items.move_to_end(key)
            elif self.eviction == 'lfu':
                self.access_counts[key] += 1
        return True

    def pre_delete(self, key: KeyT) -> bool:
        """Update tracking when item is deleted."""
        if key in self.items:
            del self.items[key]
            if self.eviction == 'lfu':
                del self.access_counts[key]
        return True

    def pre_clear(self) -> bool:
        """Clear tracking data."""
        self.items.clear()
        if self.eviction == 'lfu':
            self.access_counts.clear()
        return True
