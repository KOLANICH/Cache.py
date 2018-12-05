import typing
from pathlib import Path
import warnings

import lmdb
from pantarei import ProgressReporter

from . import StorageBackend
from transformerz import DummyTransformer
from transformerz.struct import int64Transformer, uint64
from transformerz.text import utf8Transformer

lmdbUint64KeyTransformer = DummyTransformer("=Q", uint64)
lmdbBytesKeyTransformer = DummyTransformer("bytes", bytes)
mappers = {t.id.encode("utf-8"): t for t in (int64Transformer, utf8Transformer, lmdbUint64KeyTransformer, lmdbBytesKeyTransformer)}
mappersTypes = {t.tgtType: t for t in mappers.values()}


class LMDBBackend(StorageBackend):
	"""A backend using LMDB as a key-value storage"""

	__slots__ = ("env", "currentWriteTx", "functions")
	FILE_EXTENSIONS = {"mdb",}
	BASE_ARG_TYPES = (lmdb.Environment,)
	NATIVE_VALUE_TYPES = {bytes}

	keyTypesDataBaseName = "keyTypes"

	class SharedWriteTransaction:
		__slots__ = ("tx", "refCount", "env")

		def __init__(self, env):
			self.tx = None
			self.refCount = 0
			self.env = env

		def spawn(self):
			return self.env.begin(write=True, buffers=False)

		def __enter__(self):
			if not self.refCount:
				self.tx = self.spawn()
			self.refCount += 1
			return self

		def __exit__(self, exc_class, exc, traceprocess) -> None:
			self.refCount -= 1
			if not self.refCount:
				self.tx.commit()
				self.tx = None

		def commit(self):
			self.tx.commit()
			self.tx = self.spawn()

		def abort(self):
			self.tx.abort()
			self.tx = self.spawn()

		def __getattr__(self, k):
			return getattr(self.tx, k)

	class _Table(StorageBackend.Table):
		__slots__ = ("table", "keyMapper")
		mapperTypeNameSeparator = "-"
		keyMapperMetaKey = "key"

		def __init__(self, parent, name, keyMapper, valueMapper=None) -> None:
			super().__init__(parent, name)
			self.keyMapper = keyMapper
			self.table = self._open()

		@classmethod
		def getMapper(cls, parent, name, tp):
			keyMapperId = parent.tables.typesMeta[cls.mapperTypeNameSeparator.join((tp, name))]
			#print("keyMapperId", keyMapperId)
			if keyMapperId is not None:
				return mappers[keyMapperId]
			return None

		@classmethod
		def setMapper(cls, parent, name, typ, mapper):
			parent.tables.typesMeta[cls.mapperTypeNameSeparator.join((typ, name))] = mapper.id.encode("utf-8")

		def exists(self) -> bool:
			self.keyMapper = self.__class__.getMapper(self.parent, self.name, self.__class__.keyMapperMetaKey)
			return self.keyMapper is not None

		def _open(self):
			return self.parent.env.open_db(self.name.encode("utf-8"), integerkey=(self.keyMapper is lmdbUint64KeyTransformer))

		def create(self, keyType: type = str, valueType: type = bytes) -> None:
			self.keyMapper = mappersTypes[keyType]
			self.setMapper(self.parent, self.name, self.__class__.keyMapperMetaKey, self.keyMapper)
			self.table = self._open()
			self.parent.commit()

		def getInfo(self):
			with self.parent.getReadTx() as tx:
				return tx.stat(db=self.table)

		def getDataSize(self):
			st = self.getInfo(db=self.table)
			return {"total": st["leaf_pages"] * st["psize"]}

		def __len__(self) -> int:
			return self.getInfo()["entries"]

		def __iter__(self):
			return self.keys()

		def keys(self):
			with self.parent.getReadTx() as tx:
				with tx.cursor(db=self.table) as cur:
					for k in cur.iternext_nodup(keys=True, values=False):
						yield self.keyMapper.process(k)

		def values(self):
			with self.parent.getReadTx() as tx:
				with tx.cursor(db=self.table) as cur:
					yield from cur.iternext_nodup(keys=False, values=True)

		def items(self):
			with self.parent.getReadTx() as tx:
				with tx.cursor(db=self.table) as cur:
					for k, v in cur.iternext_nodup(keys=True, values=True):
						yield (self.keyMapper.process(k), v)

		def __contains__(self, key):
			res = self[key] is not None
			return res

		def __getitem__(self, key: str) -> bytes:
			keyBin = self.keyMapper.unprocess(key)
			res = None
			with self.parent.getReadTx() as tx:
				res = tx.get(keyBin, db=self.table)
			return res

		def __setitem__(self, key: str, value: bytes):
			keyBin = self.keyMapper.unprocess(key)
			with self.parent.currentWriteTx as tx:
				tx.replace(keyBin, value, db=self.table)

		def __delitem__(self, key) -> None:
			with self.parent.currentWriteTx as tx:
				tx.delete(self.keyMapper.unprocess(key), db=self.table)

		def drop(self) -> None:
			with self.parent.currentWriteTx as tx:
				tx.drop(self.table, delete=False)
				del self.parent.tables.typesMeta[self.name]

		def applyToValues(self, funcName: str, progressReporter: ProgressReporter):
			fn = self.parent.functions[funcName]
			with self.parent.currentWriteTx as tx:
				with progressReporter(total=len(self), title=funcName) as pr:
					with tx.cursor(db=self.table) as cur:
						while cur.next():
							cur.replace(fn(cur.value()))
							pr.report("", progress=None, incr=None, op=None)

		def getKeyType(self):
			return self.keyMapper.tgtType

	class Table(_Table):
		__slots__ = ()

		def __init__(self, parent, name) -> None:
			super().__init__(parent, name, self.__class__.getMapper(parent, name, self.__class__.keyMapperMetaKey))

	def __init__(self, base: typing.Union[Path, str, lmdb.Environment] = "./cache", metaDataTableName: str = None) -> None:  # pylint:disable=super-init-not-called # metaclass magic
		if isinstance(base, lmdb.Environment):
			self.path = None
			self.env = base
		elif isinstance(base, (str, Path)):
			self.path = base
			self.env = None
		else:
			raise ValueError("`base` param must be either a path to base, or ':memory:', or a `lmdb.Environment` object")
		self.currentWriteTx = None
		self.functions = {}

	def commit(self):
		pass

	def getReadTx(self):
		return self.env.begin(write=False, buffers=True)

	def __enter__(self) -> "LMDBBackend":
		if self.path is not None:
			self.env = lmdb.open(str(self.path), create=True, writemap=True, subdir=bool(self.path.suffix.lower() in self.__class__.FILE_EXTENSIONS), max_dbs=4)

		self.tables._tables["typesMeta"] = typesMetaTbl = self.__class__._Table(self, self.__class__.keyTypesDataBaseName, utf8Transformer)

		self.currentWriteTx = self.__class__.SharedWriteTransaction(self.env)
		if not typesMetaTbl.exists():
			typesMetaTbl.create(str, str)
		else:
			pass
			#del(typesMetaTbl)
			#self.tables.map("typesMeta", keyTypesDataBaseName)

	def __exit__(self, exc_class, exc, traceprocess) -> None:
		if self.currentWriteTx is not None:
			self.currentWriteTx.__exit__(exc_class, exc, traceprocess)
			self.currentWriteTx = None
		self.tables.__exit__(exc_class, exc, traceprocess)
		if self.env is not None:
			self.env.close()
			self.env = None

	def __del__(self) -> None:
		try:
			self.__exit__(None, None, None)
		except BaseException as ex:  # pylint:disable=broad-except
			warnings.warn("Exception when __exit__ing " + self.__class__.__name__ + " backend: " + repr(ex))
			#import traceback
			#traceback.print_exc()

	def vacuum(self) -> None:
		pass

	def optimize(self) -> None:
		pass

	def createFunction(self, name, f):
		self.functions[name] = f

	def beginTransaction(self):
		pass
