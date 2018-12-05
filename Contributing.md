Architecture overview
=====================

The goals of this lib are:
* secure, but able to serialize compound types
* key-value storage
* disk-persistent
* compressed
* efficient
* easy-to-use

The following are explicitly not goals:
* out-of-the-box-portability. The data is not meant to be read on every machine. It is not an exchange format. It is a cache. It is even called `Cache`. Sacrificing out-of-the-box-portability allows us to automatically choose the best libs available.


For compression we have to use compression libraries. These libraries operate on raw `bytes` and return raw `bytes`. For efficient   key cannot be compressed. Also for look-up efficiency key must be meaningful for the underlying DB. So the following design decision have been taken.

1. Keys are managed by a backend itself.
2. Values are managed by the main part of the lib and a backend sees and stores them as bytes.
3. In order to convert objects to `bytes` and back classes derived from `TransformerBase` are used. They have 2 funcs, `process` and `unprocess`. Since I have reused some code I have written for Kaitai Struct project, the convention is following: `process` method converts `bytes` to the target type, `unprocess` converts the target type to bytes. We use them for serialization, encoding and compression.
4. It is easier to convert some types not directly into `bytes` but to some intermediate type. This way we have filters chains. We specify them using `serializers` class field. When data is upserted a chain is traversed in forward direction, when data is retrieved the chain is traversed in backward direction.
5. Automatic row serialization format and compression selection. Since portability is not a goal, we can check which libs are installed in the environment and use the ones we consider the best. Since a user can install some libs after we have chosen the set of libs for a storage, we have to store the filter chain alongside the data.
6. Compression filters are **special**. They 
    * shouldn't be applied to keys
    * can have additional parameters, the most importantly - a dictionary
    * we need to have a way to create a dictionary.
That's why they deserve an own field (`compressor`) and an own interface (`Compressor`).
7. We don't want to depend on backend extensions, so we do compression and other transformations on our side. For compression it means that we have to compress each row separately.
8. But it harms efficiency. It doesn't exploit inter-row redundancy. We can solve it by storing a dictionary (many compression libs allow us to supply an own one). Having a precomputed dictionary can also speed-up compression (but obviously is suitable only on the data similar to the ones used for dictionary creation.). Because we don't want to store the info about which dictionary was used for compression of each row (it's inefficient), we decide that if a dictionary is present, every row is compressed using this dictionary.
9. We implement recompression this strange way through registering a function becase some troubles with SQLite with simultaneous iteration and write. We workaround that by registering a recompression function and doing updating in SQLite calling that function from SQLite. That's why backends' processing interface is so strange. If such perversions are unnecessary for a backend, just create a `dict`, set its item in `createFunction` and retrieve function from it in `createFunction` by name.
10. As you see, we have a pretty much metadata to store. So we need different tables, one is for data, another one is for metadata. So we introduce the concept of tables and build backend interface around this conception.
11. It is inconvenient to repeat oneselves, so we utilise metaclasses.
12. It is inconvenient to manually specify the full filter chain each time, so we utolise inheritance and the metaclass to append `_appendSerializers` to `serializers`.
13. For ease of use data is upserted/retrieved using `[]` notation, similar to the one of a `dict`.
14. Committing each write to disk harms performance in bulk operations. So we allow a user to tweak count of ops after which the data is commited to the disk. But it is not guaranteed. By the nature of some backends, like LMDB, a programmer has to commit ops to disk after each upsert.
15. There is no sense to store `None`s in the DB, so we treat assigning `None` as deletion.

Testing
-------

It is recommended to mount `ramfs` into the dir where the temporary database gonna be stored in.