Cache.py [![Unlicensed work](https://raw.githubusercontent.com/unlicense/unlicense.org/master/static/favicon.png)](https://unlicense.org/)
========
[wheel](https://gitlab.com/KOLANICH/Cache.py/-/jobs/artifacts/master/raw/dist/Cache-0.CI-py3-none-any.whl?job=build)
[wheel (GHA via `nightly.link`)](https://nightly.link/KOLANICH-libs/Cache.py/workflows/CI/master/urm-0.CI-py3-none-any.whl)
![GitLab Build Status](https://gitlab.com/KOLANICH/Cache.py/badges/master/pipeline.svg)
[![GitHub Actions](https://github.com/KOLANICH-libs/Cache.py/workflows/CI/badge.svg)](https://github.com/KOLANICH-libs/Cache.py/actions/)
![GitLab Coverage](https://gitlab.com/KOLANICH-libs/Cache.py/badges/master/coverage.svg)
[![Coveralls Coverage](https://img.shields.io/coveralls/KOLANICH-libs/Cache.py.svg)](https://coveralls.io/r/KOLANICH-libs/Cache.py)
[![Libraries.io Status](https://img.shields.io/librariesio/github/KOLANICH-libs/Cache.py.svg)](https://libraries.io/github/KOLANICH-libs/Cache.py)

Just a dumb key-value disk-persistent compressed **cache**. Values types available depend on container type:
*  `BlobCache` - cache for dumb binary blobs. A base class, all other classes are just some filters added upon it.
    *  `StringCache` - cache for UTF-8 strings.
        *  `JSONCache` - cache for anything JSON-serializeable. Uses `ujson` if it is available which is faster than built-in `json` module.
    *  `BSONCache` - more space-efficient than `JSONCache`. Less efficient for storage binary blobs and strings than `BlobCache` and `StringCache`. Available if `pymongo` is installed.
    *  `MsgPackCache` - more space-efficient than `BSONCache`. You need a package for MsgPack serialization.
    *  `CBORCache` - may be a bit less efficient than `MsgPackCache`, but supports recursion. You need a package for CBOR serialization: either `cbor` or `cbor2`.
    *  `Cache` - selects the most capable container available. It is not portable and expected to be used on a local machine for caching stuff. If you need compatibility, use containers explicitly.

The keys are `str`ings, `bytes`, `int`s or anything that can be a value (since locating a record is usually done by comparing serialized representation even `dict`s can be a key, if they deterministically result into the same bytes (since `json.dumps` sorts keys, they are)).

```python
import Cache
c = Cache.Cache("./cache.sqlite", Cache.compressors.lzma) # or you can put True to automatically select the best compressor available. File extension matters, based on it backend is automatically selected!
c["str"] = "str"
c["int"] = 10
c["float"] = 10.1
c["object"] = {"some":1, "cached":{"shit":[1,2,3]}}
print(c["str"], c["int"], c["float"], c["object"])

print("object" in c)
del(c["object"])
print("object" in c)
c.empty()
print("str" in c)
```


Why?
----

Because `pickle` is insecure shit which is often **abused** by using it for caching the data not requiring code execution.


Compression optimization
------------------------

When you have populated the cache with enough data to derive an effective shred dictionary, call `optimizeCompression`. This will compute a dictionary, write it into the base and then will start recompression the records with dictionary. Improves compression greatly (6 times) when compared to each record compessed independently. Also should speed-up further compressions. For now works only for `zstd`, and you need to rebuild the python bindings because the impl currently used hardcodes the slow method for dictionary creation.

Backends
--------

Library architecture allows plugging multiple backends. A backend is a wrapper around another database providing a unufied key-value interface. They are probably useful even without this lib.

Following backends are implemented:
* [SQLite](https://sqlite.org)
* LMDB

By default it uses an [SQLite](https://sqlite.org) database as a backend (and historically it was the only store available when there were no backends). Bindings to SQLite are in python stdlib, no need to install any third-party dependencies. If `SQLITE_ENABLE_DBSTAT_VTAB` was not defined during SQLite dynamic shared library used by python (`sqlite3.dll` on Windows, `sqlite3.so` on Linux) build, it is **strongly** recommended to rebuild it with the macrodef defined and replace it in order to make python use it. It is OK to build with MinGW-W64, in fact in Anaconda it is built with MinGW, not with MSVC. It will allow measuring tables size, which is useful for recompression. You can get SQLite library build options using `getSQLiteLibCompileOptions`.

Optionally an [LMDB]() backend is available.

Hacking
-------

See [Contributing.md](./Contributing.md) for architecture overview and design decisions rationale.
