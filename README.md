Full-Featured Cacher
--------------------
Written by Neeraj Kumar <me@neerajkumar.org>


Implementation consists of:
- Cache: Main class managing multiple backends and policies
- CacheBackend: Base class for storage+formatter combinations
- CacheStrategy: Base class for cache strategies (TTL, rate limits, etc)
- CacheFormatter: Base class for serialization formats


# Examples
## Basic Usage

Let's say we want to cache the results of a function that fetches some data from a remote API:

```python

from cacher import MemoryBackend


def fetch_from_api(endpoint: str, **kwargs) -> dict:
    print(f'fetching from api at {endpoint} with kw {kwargs}')
    ...


user1_posts = fetch_from_api('/posts', userId=1, sort='asc')
# output: fetching from api at /posts with kw {'userId': 1, 'sort': 'asc'}

# Define a simple cache backend that stores data in memory
cacher = MemoryBackend(fn=fetch_from_api)
user1_posts_cached = cacher('/posts', userId=1, sort='asc')
# output: fetching from api at /posts with kw {'userId': 1, 'sort': 'asc'}
assert user1_posts == user1_posts_cached
user1_posts_cached = cacher('/posts', userId=1, sort='asc')
# [no output, cached result returned]
assert user1_posts == user1_posts_cached
```

Or if you always want to cache outputs from that function, you can apply it as a decorator:

```python
cacher = MemoryBackend()

@cacher
def fetch_from_api(endpoint: str, **kwargs) -> dict:
    ...

user1_posts_cached = fetch_from_api('/posts', userId=1, sort='asc')
```

Now let's say you want to store the cache to disk as JSON files, one per separate set of request
parameters. This is useful for longer-term caching, across multiple runs of your program.
```python
from cacher import SeparateFileBackend, JsonFormatter

cacher = SeparateFileBackend(
    fn=fetch_from_api,
    formatter=JsonFormatter(),
    cache_dir='/cache/dir',
)

cacher('/posts', userId=1, sort='asc')
# the results of that call are stored in '/cache/dir/posts_userId=1_sort=asc.json'
cacher('/posts', userId=2)
# the results of that call are stored in '/cache/dir/posts_userId=2'
```
If you need to send in some parameters that you don't want to be part of the cache key, such as an
api key, you can cache a lambda like this:
```python
from cacher import SeparateFileBackend, JsonFormatter

API_KEY = 'some_api_key'
cacher = SeparateFileBackend(
    fn=lambda endpoint, **kwargs: fetch_from_api(endpoint, api_key=API_KEY, **kwargs),
    formatter=JsonFormatter(),
    cache_dir='/cache/dir',
)
```

Cachers also work fine if you're using methods on a class, such as this:
```python
class ApiClient:
    def call_api(self, endpoint: str, **kwargs) -> dict:
        print(f'fetching from api at {endpoint} with kw {kwargs}')
        ...

client = ApiClient()

cacher = SeparateFileBackend(
    fn=client.call_api,
    formatter=JsonFormatter(),
    cache_dir='/cache/dir',
)
```
You can also apply the decorator version instead:

```python
from cacher import SeparateFileBackend, JsonFormatter

cacher = SeparateFileBackend(
    formatter=JsonFormatter(),
    cache_dir='/cache/dir',
)

class ApiClient:
    @cacher
    def call_api(self, endpoint: str, **kwargs) -> dict:
        print(f'fetching from api at {endpoint} with kw {kwargs}')
        ...
```

## Keyers

So far, we've been using the default `Keyer`, which generates a filename based on the input args and
kwargs to the function. This works fine for many cases, and has the advantage that the filenames are
human-readable, which is useful for debugging and manual inspection of the cache. However,
these filenames can get very long and unwieldy if you have lots of parameters.

So instead, you can use a `HashStringKeyer` that generates a hash of the parameters instead:

```python
from cacher import SeparateFileBackend, JsonFormatter, HashStringKeyer

cacher = SeparateFileBackend(
    fn=fetch_from_api,
    formatter=JsonFormatter(),
    cache_dir='/cache/dir',
    keyer=HashStringKeyer(),
)

cacher('/posts', userId=1, sort='asc')
# Now the results of that call are stored in '/cache/dir/5f4dcc3b5aa765d61d8327deb882cf99.json'
```
If you run this at large scale, you're going to end up with a directory with thousands of files,
which can be slow to read from and write to. So you can also specify a `filename_fn` that takes the
input key and outputs a filename (including subdirs). Let's use the first 4 chars of the hash as a
subdir:

```python
from cacher import SeparateFileBackend, JsonFormatter, HashStringKeyer

cacher = SeparateFileBackend(
    fn=fetch_from_api,
    formatter=JsonFormatter(),
    cache_dir='/cache/dir',
    keyer=HashStringKeyer(),
    filename_fn=lambda key: f'{key[:4]}/{key}.json',
)

cacher('/posts', userId=1, sort='asc')
# Now the results of that call are stored in '/cache/dir/5f4d/5f4dcc3b5aa765d61d8327deb882cf99.json'
```

## Strategies

Of course, one problem with using a disk cache is that it stores stuff forever, which can lead to
results being out of date. You can use a `TTLPolicy` (which is a type of `Strategy`) to automatically
invalidate cached entries after a day:

```python
from cacher import SeparateFileBackend, JsonFormatter, HashStringKeyer, TTLPolicy

cacher = SeparateFileBackend(
    fn=fetch_from_api,
    formatter=JsonFormatter(),
    cache_dir='/cache/dir',
    keyer=HashStringKeyer(),
    filename_fn=lambda key: f'{key[:4]}/{key}.json',
    strategies=[TTLPolicy(ttl_seconds=60*60*24)],
)
```

Another issue with APIs is that they can return errors, and you probably don't want to cache those
(e.g. overloaded or rate-limited errors). The cachers automatically raise the underlying error and
don't cache if the function raises an exception.

For rate-limiting specifically, you can also add a `RateLimiter` (another `Strategy`) to enforce a
minimum interval between calls:
```python
from cacher import SeparateFileBackend, JsonFormatter, HashStringKeyer, TTLPolicy, RateLimiter

cacher = SeparateFileBackend(
    fn=fetch_from_api,
    formatter=JsonFormatter(),
    cache_dir='/cache/dir',
    keyer=HashStringKeyer(),
    filename_fn=lambda key: f'{key[:4]}/{key}.json',
    strategies=[
        TTLPolicy(ttl_seconds=60*60*24),
        RateLimiter(min_interval=1.0),
    ],
)
```
Strategies are implemented as set of hooks that run pre- and post- various operations, such as
checking for a cached value, writing to the cache, etc. You can add as many strategies to a cacher
as you like (they are applied in the order specified, in case that matters). You can also implement
your own strategies.

Another very helpful strategy is `BackgroundWriteStrategy`, which allows you to write to the backend
using a background thread. This is useful if you want to avoid blocking the main thread, with the
tradeoff that not every write will be reflected immediately in the cache (and you might lose some
cache entries if the program ends unexpectedly).

## Other backends

An alternate way to store the cached data is in a single JSON file, where each top-level key is a
hash of the input parameters. You can do that by using a `JointFileBackend` instead of the
`SeparateFileBackend`:

```python
from cacher import JointFileBackend, JsonFormatter, HashStringKeyer, TTLPolicy, RateLimiter

cacher = JointFileBackend(
    fn=fetch_from_api,
    formatter=JsonFormatter(),
    cache_path='/cache/dir/api_cache.json',
    keyer=HashStringKeyer(),
    strategies=[
        TTLPolicy(ttl_seconds=60*60*24),
        RateLimiter(min_interval=1.0),
    ],
)

cacher('/posts', userId=1, sort='asc')
# Now the results of that call are stored in '/cache/dir/api_cache.json', under the key '5f4dcc3b5aa765d61d8327deb882cf99'
```

While this can be useful for some limited cases, keep in mind that there are limitations in using a
single json file for caching. In particular, it has to be read and parsed every time you want to
read or write to it. A simple strategy if you want both speed and durability is to use multiple
cachers. This can be done via the `MultiplexBackend`:

```python
from cacher import MultiplexBackend, SeparateFileBackend, JsonFormatter, HashStringKeyer, TTLPolicy, RateLimiter

mem_cacher = MemoryBackend(fn=fetch_from_api)
disk_cacher = SeparateFileBackend(
    fn=fetch_from_api,
    formatter=JsonFormatter(),
    cache_dir='/cache/dir',
    keyer=HashStringKeyer(),
    strategies=[
        TTLPolicy(ttl_seconds=60*60*24),
        RateLimiter(min_interval=1.0),
    ],
)

cacher = MultiplexBackend(backends=[mem_cacher, disk_cacher])
cacher('/posts', userId=1, sort='asc')
```

TODO explanation of how this deals with write-through, etc

However, in many cases you are probably better off using a database of some kind as your cache
backend. We currently offer support for sql-based databases, LMDB, and redis:
```python
from cacher import SQLBackend, LMDBBackend, RedisBackend, JsonFormatter, HashStringKeyer

std_kw = dict(fn=fetch_from_api, formatter=JsonFormatter(), keyer=HashStringKeyer())

sql_cacher = SQLBackend(engine='sqlite:///cache.db', table='api_cache', **std_kw)

lmdb_cacher = LMDBBackend(db_path='/cache/dir/cache.lmdb', **std_kw)

redis_kw = dict(host='localhost', port=6379, db=0, password=None)
redis_cacher = RedisBackend(**redis_kw, **std_kw)
```
## Batching
Many functions operate on batches of inputs rather than one at a time. For example, you might want
to fetch multiple posts in a single API call. Cacher supports this vi

