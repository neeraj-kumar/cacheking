from __future__ import annotations

import asyncio
import logging
import threading

from abc import ABC, abstractmethod
from contextlib import asynccontextmanager
from functools import partial, wraps
from pathlib import Path
from typing import Any, Callable, Generic, Iterator, AsyncIterator

import sqlalchemy as sa

try:
    from sqlalchemy.ext.asyncio import create_async_engine, AsyncConnection, AsyncEngine
except Exception:
    pass

from nkpylib.cacher.constants import KeyT, CacheNotFound
from nkpylib.cacher.formatters import CacheFormatter, JsonFormatter
from nkpylib.cacher.strategies import CacheStrategy
from nkpylib.cacher.file_utils import _read_file, _write_atomic
from nkpylib.cacher.keyers import Keyer, TupleKeyer, HashStringKeyer

CACHE_MISS = '<NK_cache_miss>'  # Sentinel value for cache misses


logger = logging.getLogger(__name__)

class CacheBackend(ABC, Generic[KeyT]):
    """Base class for storage backends.

    Each backend is initialized with a formatter that handles serialization.
    """
    CACHE_MISS = CACHE_MISS

    def __init__(self,
                 fn: Callable|None=None,
                 *,
                 formatter: CacheFormatter,
                 keyer: Keyer|None = None,
                 strategies: list[CacheStrategy]|None = None,
                 batch_extractor: Callable[[tuple, dict], list]|None = None,
                 batch_combiner: Callable[[list, tuple, dict], Any]|None = None,
                 **kwargs):
        self.fn = fn
        self.formatter = formatter
        self.keyer = keyer or HashStringKeyer()
        self.strategies = []
        for strategy in (strategies or []):
            strategy._backend = self
            self.strategies.append(strategy)
        self.batch_extractor = batch_extractor
        self.batch_combiner = batch_combiner
        self.stats: dict[str, int] = {
            'hits': 0,
            'misses': 0,
            'evictions': 0
        }

    def _to_key(self, fn: Callable|None, args: tuple, kwargs: dict) -> KeyT:
        """Convert function arguments to a cache key using the keyer."""
        return self.keyer.make_key(fn, args, kwargs)

    def call_with_cache(self, fn: Callable, *args: Any, **kwargs: Any) -> Any:
        """Call a function with caching.

        Note that we do not use the instance's `fn` attribute here, and we also don't check our
        cache for the function itself, and hence the cache might not be for the right thing if
        you're not careful.

        Args:
            fn: The function to call
            *args: Positional arguments for the function
            **kwargs: Keyword arguments for the function

        Returns:
            The cached result or the result of the function call.
        """
        # Convert function arguments to cache key
        key = self._to_key(fn, args, kwargs)

        # Try to get from cache
        result = self.get(key)
        if result != CACHE_MISS:
            return result

        # Not in cache, call function
        result = fn(*args, **kwargs)

        # Store in cache
        self.set(key, result)
        return result

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        """Call our cached function with the given arguments.

        If batch_extractor and batch_combiner are set, treats this as a batch operation.
        Otherwise, treats it as a normal function call.
        """
        if self.batch_extractor is not None and self.batch_combiner is not None:
            # Extract batch items
            #FIXME does this do what we want?
            items = self.batch_extractor(args, kwargs)

            # Process batch with caching
            results = self.call_with_batch_cache(self.fn, items)

            # Combine results back into expected format
            return self.batch_combiner(results, args, kwargs)
        else:
            # Normal function call
            return self.call_with_cache(self.fn, *args, **kwargs)

    def as_decorator(self) -> Callable:
        """Returns this cacher as a decorator that will cache function results.

        Returns:
            A decorator function that will cache results of the decorated function.
        """
        def decorator(func: Callable) -> CacheBackend:
            # Create new instance with same config but with the function
            return self.__class__(
                fn=func,
                formatter=self.formatter,
                keyer=self.keyer,
                strategies=self.strategies,
                batch_extractor=self.batch_extractor,
                batch_combiner=self.batch_combiner,
            )
        return decorator

    def get_stats(self) -> dict[str, int]:
        """Get cache statistics."""
        return self.stats.copy()

    def get(self, key: KeyT) -> Any:
        """Get value for key, running it through all strategies."""
        # Run ALL pre-get hooks
        proceed = all(
            strategy.pre_get(key)
            for strategy in self.strategies
        )
        if not proceed:
            return self.handle_cache_miss(key)

        # Get the value
        value = self._get_value(key)
        if value == CACHE_MISS:
            return self.handle_cache_miss(key)

        # Run post-get hooks
        for strategy in self.strategies:
            value = strategy.post_get(key, value)

        return self.handle_cache_hit(key, value)

    async def get_async(self, key: KeyT) -> Any:
        """Async version of get()."""
        # Run ALL pre-get hooks
        proceed = all(
            strategy.pre_get(key)
            for strategy in self.strategies
        )
        if not proceed:
            return self.handle_cache_miss(key)

        # Get the value
        value = await self._get_value_async(key)
        if value == CACHE_MISS:
            return self.handle_cache_miss(key)

        # Run post-get hooks
        for strategy in self.strategies:
            value = strategy.post_get(key, value)

        return self.handle_cache_hit(key, value)

    def set(self, key: KeyT, value: Any) -> None:
        """Set value after running it through all strategies."""
        # Run ALL pre-set hooks
        proceed = all(
            strategy.pre_set(key, value)
            for strategy in self.strategies
        )
        if not proceed:
            return

        # Store the value
        assert value != CACHE_MISS, "Cannot cache CACHE_MISS sentinel"
        self._set_value(key, value)

        # Run post-set hooks
        for strategy in self.strategies:
            strategy.post_set(key, value)

    async def set_async(self, key: KeyT, value: Any) -> None:
        """Async version of set()."""
        # Run ALL pre-set hooks
        proceed = all(
            strategy.pre_set(key, value)
            for strategy in self.strategies
        )
        if not proceed:
            return

        # Store the value
        assert value != CACHE_MISS, "Cannot cache CACHE_MISS sentinel"
        await self._set_value_async(key, value)

        # Run post-set hooks
        for strategy in self.strategies:
            strategy.post_set(key, value)

    def get_and_set(self, key: KeyT, value_fn: Callable[[Any], Any], skip_cache_miss:bool=False) -> None:
        """Gets the current cache value of `key`, runs `value_fn` on it, and sets the result.

        If the key is not in the cache, `value_fn` is called with `CACHE_MISS`.
        Unless you set `skip_cache_miss=True`, in which case we just exit early.
        You can raise ValueError or return `CACHE_MISS` in `value_fn` to avoid setting a new value.
        """
        current_value = self.get(key)
        if skip_cache_miss and current_value == CACHE_MISS:
            return
        try:
            new_value = value_fn(current_value)
            if new_value == CACHE_MISS:
                return
        except ValueError:
            return
        self.set(key, new_value)

    def delete(self, key: KeyT) -> None:
        """Delete key after checking with all strategies."""
        # Run pre-delete hooks
        for strategy in self.strategies:
            if not strategy.pre_delete(key):
                return

        # Delete the value
        self._delete_value(key)

        # Run post-delete hooks
        for strategy in self.strategies:
            strategy.post_delete(key)

    async def delete_async(self, key: KeyT) -> None:
        """Async version of delete()."""
        # Run pre-delete hooks
        for strategy in self.strategies:
            if not strategy.pre_delete(key):
                return

        # Delete the value
        await self._delete_value_async(key)

        # Run post-delete hooks
        for strategy in self.strategies:
            strategy.post_delete(key)

    def clear(self) -> None:
        """Clear all entries after checking with strategies."""
        # Run ALL pre-clear hooks
        proceed = all(
            strategy.pre_clear()
            for strategy in self.strategies
        )
        if not proceed:
            return

        # Clear the storage
        self._clear()

        # Run post-clear hooks
        for strategy in self.strategies:
            strategy.post_clear()

    async def clear_async(self) -> None:
        """Async version of clear()."""
        # Run ALL pre-clear hooks
        proceed = all(
            strategy.pre_clear()
            for strategy in self.strategies
        )
        if not proceed:
            return

        # Clear the storage
        await self._clear_async()

        # Run post-clear hooks
        for strategy in self.strategies:
            strategy.post_clear()

    def _contains(self, key: KeyT) -> bool:
        """Check if `key` is in cache.

        By default, we try to actually get the value using `_get_value`, but you can have a more
        optimized version.
        """
        return self._get_value(key) != CACHE_MISS

    @abstractmethod
    def _get_value(self, key: KeyT) -> Any:
        """Actually get the value from storage."""
        pass

    @abstractmethod
    def _set_value(self, key: KeyT, value: Any) -> None:
        """Actually set the value in storage."""
        pass

    @abstractmethod
    def _delete_value(self, key: KeyT) -> None:
        """Actually delete the value from storage."""
        pass

    async def _get_value_async(self, key: KeyT) -> Any:
        """Async version of _get_value.

        By default directly calls the sync version.
        Override this for true async implementation.
        """
        return self._get_value(key)

    async def _set_value_async(self, key: KeyT, value: Any) -> None:
        """Async version of _set_value.

        By default directly calls the sync version.
        Override this for true async implementation.
        """
        self._set_value(key, value)

    async def _delete_value_async(self, key: KeyT) -> None:
        """Async version of _delete_value.

        By default directly calls the sync version.
        Override this for true async implementation.
        """
        self._delete_value(key)

    async def _clear_async(self) -> None:
        """Async version of _clear.

        By default directly calls the sync version.
        Override this for true async implementation.
        """
        self._clear()

    def _clear(self) -> None:
        """Clear all entries.

        This version does it by iterating through our keys and deleting each one.
        You can implement a more efficient version in your subclass.
        """
        for key in list(self.iter_keys()):
            self._delete_value(key)

    def check_many(self, keys: list[KeyT]) -> set[KeyT]:
        """Check which keys are in the cache.

        By default this calls _contains() on each key, but you can override this for a faster
        implementation.

        Args:
            keys: List of cache keys to check

        Returns:
            Set of keys that are found in the cache
        """
        found = set()
        for key in keys:
            if self._contains(key):
                found.add(key)
        return found

    def get_many(self, keys: list[KeyT]) -> dict[KeyT, Any]:
        """Get multiple values at once.

        Args:
            keys: List of cache keys to retrieve

        Returns:
            Dict mapping keys to their values, only including found items
        """
        results = {}
        for key in keys:
            value = self.get(key)
            if value != CACHE_MISS:
                results[key] = value
        return results

    def set_many(self, items: dict[KeyT, Any]) -> None:
        """Set multiple key/value pairs at once.

        Args:
            items: Dict mapping keys to values to cache
        """
        for key, value in items.items():
            self.set(key, value)

    def delete_many(self, keys: list[KeyT]) -> None:
        """Deletes multiple keys at once.

        Args:
            keys: list of keys to delete
        """
        for key in keys:
            self.delete(key)

    def call_with_batch_cache(self, fn: Callable[[list], list], items: list) -> list:
        """Call function with batched input, using cache where possible.

        Args:
            fn: Function that takes a list of items and returns list of results
            items: List of items to process

        Returns:
            List of results in same order as input items
        """
        # Convert each item to a cache key
        keys = [self._to_key((item,), {}) for item in items]
        key_to_idx = {key: i for i, key in enumerate(keys)}

        # Get cached results
        cached = self.get_many(keys)

        # Find items that need computing
        uncached_indices = [i for i, key in enumerate(keys) if key not in cached]
        if not uncached_indices:
            # Everything was cached
            return [cached[key] for key in keys]

        # Compute uncached items
        uncached_items = [items[i] for i in uncached_indices]
        new_results = fn(uncached_items)

        if len(new_results) != len(uncached_items):
            raise ValueError(
                f"Batch function returned {len(new_results)} results "
                f"for {len(uncached_items)} inputs"
            )

        # Cache new results
        new_items = {
            keys[uncached_indices[i]]: result
            for i, result in enumerate(new_results)
        }
        self.set_many(new_items)

        # Combine cached and new results
        all_results = list(items)  # Make same size as input
        for key, value in cached.items():
            all_results[key_to_idx[key]] = value
        for i, value in zip(uncached_indices, new_results):
            all_results[i] = value

        return all_results

    def iter_keys(self) -> Iterator[KeyT]:
        """Iterate over all keys in the cache."""
        raise NotImplementedError("iter_keys not implemented")

    async def iter_keys_async(self) -> AsyncIterator[KeyT]:
        """Async version of iter_keys."""
        for key in self.iter_keys():
            yield key

    def handle_cache_miss(self, key: KeyT) -> Any:
        """Handle a cache miss setting."""
        self.stats['misses'] += 1
        return CACHE_MISS

    def handle_cache_hit(self, key: KeyT, value: Any) -> Any:
        """Handle a cache hit."""
        self.stats['hits'] += 1
        return value


class DummyBackend(CacheBackend[KeyT]):
    """Backend that doesn't actually cache anything.

    Always returns CACHE_MISS for gets, and ignores sets/deletes.

    Useful for:
    - Temporarily disabling caching
    - Measuring baseline performance without caching
    """
    def _get_value(self, key: KeyT) -> Any:
        """Always return cache miss."""
        return self.CACHE_MISS

    def _set_value(self, key: KeyT, value: Any) -> None:
        """Do nothing."""
        pass

    def _delete_value(self, key: KeyT) -> None:
        """Do nothing."""
        pass

    def _clear(self) -> None:
        """Do nothing."""
        pass

    def iter_keys(self) -> Iterator[KeyT]:
        """Return empty iterator."""
        return iter([])


class MemoryBackend(CacheBackend[KeyT]):
    """Backend that stores everything in memory.

    Good for temporary caching and testing. Data is lost when process exits.
    """
    def __init__(self, fn: Callable|None=None, **kwargs):
        super().__init__(fn=fn, **kwargs)
        self._cache: dict[KeyT, Any] = {}

    def iter_keys(self) -> Iterator[KeyT]:
        """Iterate over all keys in memory cache."""
        yield from self._cache.keys()

    def _get_value(self, key: KeyT) -> Any:
        """Get value from memory cache."""
        return self._cache.get(key, CACHE_MISS)

    def _set_value(self, key: KeyT, value: Any) -> None:
        """Store value in memory cache."""
        assert value != CACHE_MISS, "Cannot cache CACHE_MISS sentinel"
        self._cache[key] = value

    def _delete_value(self, key: KeyT) -> None:
        """Delete value from memory cache."""
        if key in self._cache:
            del self._cache[key]

    def _clear(self) -> None:
        """Clear all entries in memory cache."""
        self._cache.clear()

class SeparateFileBackend(CacheBackend[KeyT]):
    """Backend that stores each key in a separate file.

    Good for large objects like embeddings or images where you want
    to manage each cached item independently.
    """
    def __init__(self,
                 cache_dir: str|Path,
                 filename_fn: Callable|None=None,
                 fn: Callable|None=None,
                 *,
                 formatter: CacheFormatter|None=None,
                 **kwargs):
        super().__init__(fn=fn, formatter=formatter or JsonFormatter(), **kwargs)
        self.filename_fn = filename_fn
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def _key_to_path(self, key: KeyT) -> Path:
        """Convert cache key to filesystem path."""
        if self.filename_fn:
            # Use custom filename function if provided
            filename = self.filename_fn(key)
        else:
            filename = str(key)
        # replace invalid chars #FIXME
        filename = "".join(c if c.isalnum() else '_' for c in filename)
        # add extension based on formatter
        if not filename.endswith(self.formatter.EXT):
            filename += self.formatter.EXT
        return self.cache_dir / filename

    def _get_value(self, key: KeyT) -> Any:
        """Get value from file storage."""
        path = self._key_to_path(key)
        data = _read_file(path)
        if data is None:
            return self.handle_cache_miss(key)
        try:
            return self.formatter.loads(data)
        except Exception:
            return CACHE_MISS

    def _set_value(self, key: KeyT, value: Any) -> None:
        """Store value in file storage."""
        assert value != CACHE_MISS, "Cannot cache CACHE_MISS sentinel"
        path = self._key_to_path(key)
        _write_atomic(path, self.formatter.dumps(value))

    def _delete_value(self, key: KeyT) -> None:
        """Delete value from file storage."""
        path = self._key_to_path(key)
        try:
            path.unlink()
        except FileNotFoundError:
            pass

    async def _get_value_async(self, key: KeyT) -> Any:
        """Get value from file storage asynchronously."""
        path = self._key_to_path(key)
        data = await asyncio.to_thread(_read_file, path)
        if data is None:
            return self.handle_cache_miss(key)
        try:
            return self.formatter.loads(data)
        except Exception:
            return CACHE_MISS

    async def _set_value_async(self, key: KeyT, value: Any) -> None:
        """Store value in file storage asynchronously."""
        assert value != CACHE_MISS, "Cannot cache CACHE_MISS sentinel"
        path = self._key_to_path(key)
        data = self.formatter.dumps(value)
        await asyncio.to_thread(_write_atomic, path, data)

    async def _delete_value_async(self, key: KeyT) -> None:
        """Delete value from file storage asynchronously."""
        path = self._key_to_path(key)
        try:
            await asyncio.to_thread(path.unlink)
        except FileNotFoundError:
            pass

    async def _clear_async(self) -> None:
        """Clear all entries asynchronously."""
        paths = list(self.cache_dir.iterdir())
        await asyncio.gather(
            *(asyncio.to_thread(path.unlink) for path in paths)
        )

    def _clear(self) -> None:
        """Clear all entries. This deletes all files in our cache dir."""
        for path in self.cache_dir.iterdir():
            try:
                path.unlink()
            except FileNotFoundError:
                pass

    def iter_keys(self) -> Iterator[KeyT]:
        """Iterate over all keys in the cache directory."""
        for path in self.cache_dir.iterdir():
            yield path.stem

    async def iter_keys_async(self) -> AsyncIterator[KeyT]:
        """Async version of iter_keys."""
        paths = await asyncio.to_thread(list, self.cache_dir.iterdir())
        for path in paths:
            yield path.stem


class JointFileBackend(CacheBackend[KeyT]):
    """Backend that stores all keys in a single file.

    Good for small objects like metadata or settings where you want
    to keep everything in one place.

    The formatter should be dict-like at the top-level, supporting:
    - `key in cache`: Check if key exists
    - `cache[key]`: Get value for key
    - `cache[key] = value`: Set value for key
    - `del cache[key]`: Delete key
    - `cache.clear()`: Clear all entries

    Note that this NOT thread-safe -- it reads the entire json file in memory on first read.
    """
    def __init__(self, cache_path: str|Path, fn: Callable|None=None, *, formatter: CacheFormatter|None=None, **kwargs):
        super().__init__(fn=fn, formatter=formatter or JsonFormatter(), **kwargs)
        self.cache_path = Path(cache_path)
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        self._cache: dict[KeyT, Any] = {}
        self._load()

    def _load(self) -> None:
        """Load cache from file."""
        try:
            with open(self.cache_path, 'rb') as f:
                self._cache = self.formatter.loads(f.read())
        except Exception:
            self._cache = {}

    async def _load_async(self) -> None:
        """Load cache from file asynchronously."""
        #TODO see if this is actually usable anywhere?
        try:
            data = await asyncio.to_thread(partial(open, self.cache_path, 'rb'))
            with data as f:
                self._cache = self.formatter.loads(await asyncio.to_thread(f.read))
        except Exception:
            self._cache = {}

    def _save(self) -> None:
        """Save cache to file."""
        _write_atomic(self.cache_path, self.formatter.dumps(self._cache))

    async def _save_async(self) -> None:
        """Save cache to file asynchronously."""
        data = self.formatter.dumps(self._cache)
        await asyncio.to_thread(_write_atomic, self.cache_path, data)

    def _get_value(self, key: KeyT) -> Any:
        """Get value from cache dict."""
        return self._cache.get(key)

    def _set_value(self, key: KeyT, value: Any) -> None:
        """Store value in cache dict and save to file."""
        assert value != CACHE_MISS, "Cannot cache CACHE_MISS sentinel"
        self._cache[key] = value
        self._save()

    async def _set_value_async(self, key: KeyT, value: Any) -> None:
        """Store value in cache dict and save to file asynchronously."""
        assert value != CACHE_MISS, "Cannot cache CACHE_MISS sentinel"
        self._cache[key] = value
        await self._save_async()

    def _delete_value(self, key: KeyT) -> None:
        """Delete value from cache dict and save to file."""
        if key in self._cache:
            del self._cache[key]
            self._save()

    async def _delete_value_async(self, key: KeyT) -> None:
        """Delete value from cache dict and save to file asynchronously."""
        if key in self._cache:
            del self._cache[key]
            await self._save_async()

    def _clear(self) -> None:
        """Clear all entries in cache dict and save to file."""
        self._cache.clear()
        self._save()

    async def _clear_async(self) -> None:
        """Clear all entries in cache dict and save to file asynchronously."""
        self._cache.clear()
        await self._save_async()

    def iter_keys(self) -> Iterator[KeyT]:
        """Iterate over all keys in the cache dict."""
        yield from self._cache


class MultiplexBackend(CacheBackend[KeyT]):
    """Backend that multiplexes operations across multiple other backends.

    Gets return the first hit from any backend.
    Sets/deletes/clears apply to all backends.

    Can be used directly:
        cache = MultiplexBackend([...backends...])
        value = cache.get(key)
        cache.set(key, value)

    Or as a decorator:
        @cache.as_decorator()
        def expensive_function(x, y):
            return x + y
    """
    def __init__(self, backends: list[CacheBackend], **kwargs):
        super().__init__(formatter=backends[0].formatter if backends else None, **kwargs)
        self.backends = backends

    def _get_value(self, key: KeyT) -> Any:
        """Get value from first backend that has it."""
        for backend in self.backends:
            value = backend.get(key)
            if value != CACHE_MISS:
                return value
        return CACHE_MISS

    def _set_value(self, key: KeyT, value: Any) -> None:
        """Set value in all backends."""
        assert value != CACHE_MISS, "Cannot cache CACHE_MISS sentinel"
        for backend in self.backends:
            backend.set(key, value)

    def _delete_value(self, key: KeyT) -> None:
        """Delete from all backends."""
        for backend in self.backends:
            backend.delete(key)

    def _clear(self) -> None:
        """Clear all backends."""
        for backend in self.backends:
            backend.clear()


class SQLBackend(CacheBackend[KeyT]):
    """Backend that stores cache entries in a SQL database.

    Uses sa to handle database connections.
    """
    default_timeout = 20  # seconds
    def __init__(self,
                 url: str='sqlite://',
                 table_name: str='cache',
                 *,
                 fn: Callable|None=None,
                 formatter: CacheFormatter|None=None,
                 async_url: str|None=None,
                 **kwargs):
        """Initializes this cacher.

        Creates the cache table if it doesn't exist.

        Args:
            url: Database URL (e.g. 'sqlite:///cache.sqlite' or 'postgresql://user:pass@localhost/dbname')
                [By default, it creates an in-memory SQLite database]
            table_name: Name of table to use for cache storage (default 'cache')
            fn: Optional function to cache
            formatter: Optional formatter to use for serialization (default JsonFormatter)
            async_url: Optional async database URL if different from sync one
            **kwargs: Additional arguments passed to CacheBackend
        """
        super().__init__(fn=fn, formatter=formatter or JsonFormatter(), **kwargs)

        # Create parent dirs if needed for sqlite
        if url.startswith('sqlite:///'):
            db_path = url.replace('sqlite:///', '')
            Path(db_path).parent.mkdir(parents=True, exist_ok=True)

        async_names = dict(
            sqlite='aiosqlite',
            postgresql='asyncpg',
            mysql='aiomysql',
        )
        # Helper to convert sync URL to async
        def _get_async_url(url: str) -> str:
            """Convert sync database URL to async version."""
            for name, async_name in async_names.items():
                if url.startswith(f'{name}://'):
                    return url.replace(f'{name}://', f'{name}+{async_name}://')

        # Create engines and table
        self.engine = sa.create_engine(url)
        async_url = async_url or _get_async_url(url)
        try:
            self.async_engine = create_async_engine(async_url)
        except Exception as e:
            logger.warning(f"Could not create async engine for URL {async_url}: {e}")
            self.async_engine = None
        metadata = sa.MetaData()
        self.table = sa.Table(
            table_name,
            metadata,
            sa.Column('id', sa.Integer, primary_key=True, autoincrement=True),
            sa.Column('key', sa.String),
            sa.Column('value', sa.LargeBinary),
        )
        # add an index on 'key' for faster lookups, if it doesn't exist
        if not self.table.indexes:
            sa.Index(f'ix_{table_name}_cache_key', self.table.c.key)
        # Create table if it doesn't exist
        metadata.create_all(self.engine)
        self.conns_by_thread = threading.local()
        # create a connection we can reuse
        #self.conn = self.engine.connect()
        logger.info(f'Initialized SQLBackend with table {table_name} in {url}')

    def _get_pragmas(self) -> list[sa.TextClause]:
        """Get any PRAGMA statements needed for sqlite."""
        pragmas = []
        if str(self.engine.url).startswith('sqlite'):
            pragmas.append(sa.text('PRAGMA journal_mode=WAL;'))
            pragmas.append(sa.text(f'PRAGMA busy_timeout={self.default_timeout * 1000};'))
        return pragmas

    @property
    def conn(self) -> sa.Connection:
        """Get a thread-local connection to the database."""
        if not hasattr(self.conns_by_thread, 'conn'):
            conn = self.conns_by_thread.conn = self.engine.connect()
            for pragma in self._get_pragmas():
                conn.execute(pragma)
        return self.conns_by_thread.conn

    def _contains(self, key: KeyT) -> bool:
        """Check if `key` is in the cache."""
        result = self.conn.execute(
            sa.select(self.table.c.key)
            .where(self.table.c.key == str(key))
        ).first()
        return result is not None

    def check_many(self, keys: list[KeyT]) -> set[KeyT]:
        """Check which keys are in the cache.

        This does a sql query with an IN clause for efficiency.

        Args:
            keys: List of cache keys to check

        Returns:
            Set of keys that are found in the cache
        """
        found = set()
        if not keys:
            return found
        str_keys = [str(key) for key in keys]
        result = self.conn.execute(
            sa.select(self.table.c.key)
            .where(self.table.c.key.in_(str_keys))
        )
        found.update([row.key for row in result])
        return found

    def _get_value(self, key: KeyT) -> Any:
        """Get value from database."""
        result = self.conn.execute(
            sa.select(self.table.c.value)
            .where(self.table.c.key == str(key))
        ).first()
        #print(f'for key {key} got result: {result}, {type(result[0])}')
        if result is None:
            return CACHE_MISS
        r = result[0]
        if not isinstance(r, (bytes, str)): # already decoded
            return r
        try:
            return self.formatter.loads(r)
        except Exception as e:
            return CACHE_MISS

    def _set_value(self, key: KeyT, value: Any) -> None:
        """Store value in database."""
        assert value != CACHE_MISS, "Cannot cache CACHE_MISS sentinel"
        serialized = self.formatter.dumps(value)
        #print(f'setting value for key {key} in SQL backend: {serialized[:50]}..., {self._get_value(key)}, {CACHE_MISS}')  # Debug output
        exists = (self._get_value(key) != CACHE_MISS)
        #print(f'Exists: {exists}')
        if exists: # update existing value
            self.conn.execute(
                self.table.update()
                .where(self.table.c.key == str(key))
                .values(value=serialized)
            )
        else: # insert new value
            self.conn.execute(
                self.table.insert()
                .values(key=str(key), value=serialized)
            )
        self.conn.commit()

    def _delete_value(self, key: KeyT) -> None:
        """Delete value from database."""
        self.conn.execute(self.table.delete().where(self.table.c.key == str(key)))
        self.conn.commit()

    def _clear(self) -> None:
        """Clear all entries from database."""
        self.conn.execute(self.table.delete())
        self.conn.commit()

    def iter_keys(self) -> Iterator[KeyT]:
        """Iterate over all keys in the database."""
        for row in self.conn.execute(sa.select(self.table.c.key)):
            yield row.key

    async def iter_keys_async(self) -> AsyncIterator[KeyT]:
        """Async version of iter_keys."""
        if not self.async_engine:
            async for key in super().iter_keys_async():
                yield key
            return

        async with self._get_async_conn() as conn:
            result = await conn.stream(sa.select(self.table.c.key))
            async for row in result:
                yield row.key

    @asynccontextmanager
    async def _get_async_conn(self) -> AsyncConnection:
        """Get an async connection to the database."""
        if not self.async_engine:
            raise RuntimeError("Async database URL not configured")
        async with self.async_engine.connect() as conn:
            for pragma in self._get_pragmas():
                await conn.execute(pragma)
            yield conn

    async def _get_value_async(self, key: KeyT) -> Any:
        """Get value from database asynchronously."""
        if not self.async_engine:
            return await super()._get_value_async(key)

        async with self._get_async_conn() as conn:
            result = await conn.execute(
                sa.select(self.table.c.value)
                .where(self.table.c.key == str(key))
            )
            row = await result.first()

        if row is None:
            return CACHE_MISS
        r = row[0]
        if not isinstance(r, (bytes, str)):
            return r
        try:
            return self.formatter.loads(r)
        except Exception:
            return CACHE_MISS

    async def _set_value_async(self, key: KeyT, value: Any) -> None:
        """Store value in database asynchronously."""
        if not self.async_engine:
            return await super()._set_value_async(key)

        assert value != CACHE_MISS, "Cannot cache CACHE_MISS sentinel"
        serialized = self.formatter.dumps(value)

        async with self._get_async_conn() as conn:
            # Check if key exists
            result = await conn.execute(
                sa.select(self.table.c.key)
                .where(self.table.c.key == str(key))
            )
            exists = await result.first() is not None

            if exists:
                await conn.execute(
                    self.table.update()
                    .where(self.table.c.key == str(key))
                    .values(value=serialized)
                )
            else:
                await conn.execute(
                    self.table.insert()
                    .values(key=str(key), value=serialized)
                )
            await conn.commit()

    async def _delete_value_async(self, key: KeyT) -> None:
        """Delete value from database asynchronously."""
        if not self.async_engine:
            return await super()._delete_value_async(key)

        async with self._get_async_conn() as conn:
            await conn.execute(
                self.table.delete()
                .where(self.table.c.key == str(key))
            )
            await conn.commit()

    async def _clear_async(self) -> None:
        """Clear all entries from database asynchronously."""
        if not self.async_engine:
            return await super()._clear_async()

        async with self._get_async_conn() as conn:
            await conn.execute(self.table.delete())
            await conn.commit()
