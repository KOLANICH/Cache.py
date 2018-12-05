import typing
from types import FunctionType
import sys
import warnings

from collections import OrderedDict
from collections.abc import MutableMapping
from pathlib import Path

from pantarei import chosenProgressReporter, ProgressReporter, DummyProgressReporter

from transformerz.core import registry, TransformerStack
from transformerz.text import utf8Transformer
from transformerz.compression import BinaryCompressorFactory, Compressor, compressors
from transformerz.serialization.json import jsonSerializer, json

from .storageBackends import getBackendClass
from .storageBackends import _StorageBackend
from .storageBackends.sqlite import SQLiteBackend
from .storageBackends.lmdb import LMDBBackend



class CacheMeta(MutableMapping.__class__):
	def __new__(cls: typing.Type["CacheMeta"], className: str, parents: tuple, attrs: typing.Dict[str, typing.Any], *args, **kwargs) -> "CacheMeta":
		assert len(parents) <= 1
		if "_appendTransformers" in attrs:
			if parents:
				attrs["transformers"] = TransformerStack(parents[0].transformers)
			else:
				attrs["transformers"] = TransformerStack()

			attrs["transformers"].extend(attrs["_appendTransformers"])

		if "__slots__" not in attrs:
			attrs["__slots__"] = ()

		return super().__new__(cls, className, parents, attrs, *args, **kwargs)


defaultBackendCtor = SQLiteBackend


class BlobCache(metaclass=CacheMeta):
	"""Just a simple SQLite-based cache"""

	__slots__ = ("compressor", "newCompressor", "compressorFactory", "commitOnNOps", "opsPending", "nonStringKeys", "keyType", "backend", "metadata", "data", "keyType")
	TABLE_NAME = "cache"
	META_TABLE_NAME = "metadata"
	transformers = TransformerStack()

	def __init__(self, base: typing.Union[Path, str, typing.Any] = "./cache.sqlite", compressorFactory: typing.Optional[typing.Union[str, BinaryCompressorFactory]] = None, commitOnNOps: int = 1, keyType: typing.Optional[type] = None) -> None:
		if isinstance(compressorFactory, str):
			compressorFactory = compressors._COMPRESSORS_DICT[compressorFactory]
		elif compressorFactory is True:
			compressorFactory = compressors._BEST

		if isinstance(base, _StorageBackend):
			self.backend = base
		else:
			backendCtor = getBackendClass(base)
			#print("self.__class__.transformers", self.__class__.transformers)
			#print("self.__class__.transformers[0].srcType", self.__class__.transformers[0].srcType, self.__class__.transformers[0].srcType not in backendCtor.NATIVE_VALUE_TYPES, backendCtor.NATIVE_VALUE_TYPES)
			if self.__class__.transformers:
				if self.__class__.transformers[0].srcType not in backendCtor.NATIVE_VALUE_TYPES:
					optP = None
					optPathLen = float("inf")
					for t in backendCtor.NATIVE_VALUE_TYPES:
						#print(t, "dstType", self.__class__.transformers[0].srcType)
						p = registry.getPath(t, self.__class__.transformers[0].srcType)
						if p:
							l = len(p)
							if l < optPathLen:
								optPathLen = l
								optP = p
					#print("optP", optP)

			if backendCtor:
				self.backend = backendCtor(base, self.__class__.META_TABLE_NAME)
			else:
				raise ValueError("`base` must be either a preconstructed `StorageBackend` object or file path ending with one of the default extensions for the backend or something making it possible to identify backend by type of the passed value")

		if keyType is None:
			keyType = str
		self.keyType = keyType

		self.compressorFactory = compressorFactory
		self.compressor = None
		self.newCompressor = None  # A workaround to SQLite inability to read and write simultaneously. We have to register a function. Since a function cannot be un
		self.commitOnNOps = commitOnNOps
		self.opsPending = 0

	@property
	def willCommit(self) -> bool:
		return self.opsPending and (self.opsPending % self.commitOnNOps == 0)

	def opCommit(self) -> bool:
		if self.willCommit:
			self.commit()
		else:
			self.opsPending += 1
		return True

	def commit(self) -> None:
		self.backend.commit()
		self.opsPending = 0

	def getCacheTableDataSize(self):
		return self.backend.tables.data.getDataSize()

	def __len__(self) -> int:
		return len(self.backend.tables.data)

	def __contains__(self, key: str) -> bool:
		return self[key] is not None

	def __iter__(self) -> typing.Any:
		for key in iter(self.backend.tables.data):
			if self.keyType is typing.Any:
				key = self.__class__.transformers.process(key)
			yield key

	def decompressedValuesBytes(self) -> typing.Iterator[typing.Union[memoryview, bytes]]:
		for val in self.backend.tables.data.values():
			if self.compressor is not None:
				val = self.compressor.process(val)
			yield val

	def values(self) -> typing.Any:
		for el in self.decompressedValuesBytes():
			yield self.__class__.transformers.process(el)

	def keys(self) -> typing.Iterator[typing.Any]:
		return iter(self)

	def decompressedItemsBytes(self) -> typing.Iterator[typing.Any]:
		for key, val in self.backend.tables.data.items():
			if self.compressor is not None:
				val = self.compressor.process(val)
			yield (key, val)

	def items(self) -> typing.Iterator[typing.Any]:
		for key, val in self.decompressedItemsBytes():
			if self.keyType is typing.Any:
				key = self.__class__.transformers.process(key)
			yield (key, self.__class__.transformers.process(val))

	def __getitem__(self, key: str) -> object:
		if self.keyType is typing.Any:
			key = self.__class__.transformers.unprocess(key)

		val = self.backend.tables.data[key]
		if not val:
			return None

		if self.compressor is not None:
			val = self.compressor.process(val)
		val = self.__class__.transformers.process(val)

		return val

	def __setitem__(self, key: str, val: object) -> None:
		if val is None:
			del self[key]
		else:
			if self.keyType is typing.Any:
				key = self.__class__.transformers.unprocess(key)

			val = self.__class__.transformers.unprocess(val)
			if self.compressor is not None:
				val = self.compressor.unprocess(val)
			self.backend.tables.data[key] = val
			self.opCommit()

	def __delitem__(self, key: typing.Any) -> None:
		if self.keyType is typing.Any:
			key = self.__class__.transformers.unprocess(key)

		del self.backend.tables.data[key]
		self.opCommit()

	def isInitialized(self) -> bool:
		return self.backend.tables.metadata.exists()

	def getCacheTableKeyType(self) -> typing.Any:
		return self.backend.tables.data.getKeyType()

	def recreateCompressorWithArgs(self, **kwargs) -> None:
		if self.compressorFactory is not None:
			self.compressor = self.compressorFactory(kwargs)

	@property
	def compressionDictionary(self):
		return self.backend.tables.metadata["dict"]

	@compressionDictionary.setter
	def compressionDictionary(self, newDict):
		warnings.warn("Recompression usually takes long, use `applyCompressionDictionary`, it allows to provide a progress reporter!")
		self.applyCompressionDictionary(newDict)

	def applyCompressionDictionary(self, newDict, progressReporter: typing.Optional[ProgressReporter] = None):
		if progressReporter is None:
			progressReporter = DummyProgressReporter
		if self.compressionDictionary == newDict:
			return
		self.backend.beginTransaction()
		self.newCompressor = self.compressorFactory({"dictionary": newDict})
		self.backend.tables.data.applyToValues("recompress", progressReporter)
		self.compressor = self.newCompressor
		self.newCompressor = None
		self.backend.tables.metadata["dict"] = newDict
		self.compressor = self.compressorFactory({"dictionary": newDict})
		self.commit()

	def createDataTable(self) -> None:
		keyType = self.keyType if (self.keyType is not typing.Any) else bytes
		self.backend.tables.data.create(keyType, bytes)
		self.optimize()

	def __enter__(self) -> "BlobCache":
		self.backend.__enter__()
		self.backend.tables.map(self.__class__.TABLE_NAME, "data")
		self.backend.tables.map(self.__class__.META_TABLE_NAME, "metadata")

		if not self.isInitialized():
			self.backend.tables.metadata.create(str, bytes)
			self.backend.tables.metadata["compression"] = (self.compressorFactory.id if self.compressorFactory else "none").encode("utf-8")
			self.backend.tables.metadata["serializers"] = json.dumps(self.__class__.transformers.id).encode("utf-8")
			self.createDataTable()
			self.commit()
			self.recreateCompressorWithArgs()
		else:
			compressionId = self.backend.tables.metadata["compression"]
			if not isinstance(compressionId, str):
				compressionId = str(compressionId, encoding="utf-8")

			transformersTemp = self.backend.tables.metadata["serializers"]
			transformersTemp = str(transformersTemp, encoding="utf-8")  # memoryview has no `decode`
			transformersTemp = tuple(json.loads(transformersTemp))

			if transformersTemp != self.__class__.transformers.id:
				raise ValueError("This DB transformers chain doesn't match the chain used in the class: " + repr(transformersTemp),)
			keyType = self.getCacheTableKeyType()
			if keyType != self.keyType:
				if self.keyType is typing.Any and keyType is bytes:
					pass  # we serialize the stuff into bytes
				else:
					raise ValueError("This DB uses keys of type (" + repr(keyType) + "), that doesn't match to the one you enforce in ctor (" + repr(self.keyType) + ").")

			self.compressorFactory = compressors._COMPRESSORS_DICT[compressionId]

			dic = self.backend.tables.metadata["dict"]
			#print("dic", dic)
			if dic is not None and len(dict) > 0:
				self.recreateCompressorWithArgs(dictionary=dic)
			else:
				self.recreateCompressorWithArgs()
		self.backend.createFunction("recompress", self._recompressorFunc)
		return self

	def __exit__(self, exc_class, exc, traceprocess) -> None:
		if self.backend is not None:
			self.backend.__exit__(exc_class, exc, traceprocess)

	def __del__(self) -> None:
		try:
			if self.backend is not None:
				self.__exit__(None, None, None)
		except BaseException as ex:  # pylint:disable=broad-except
			warnings.warn("Exception when __exit__ing Cache backend: " + repr(ex))
			#import traceback
			#traceback.print_exc()

	def empty(self) -> None:
		"""Empties the DB"""
		self.backend.tables.data.drop()
		self.createDataTable()
		self.commit()

	def vacuum(self) -> None:
		if self.opsPending:
			self.commit()
		self.backend.vacuum()

	def optimize(self) -> None:
		if self.opsPending:
			self.commit()
		self.backend.optimize()

	def populate(self, target: typing.Iterable[typing.Tuple[typing.Any, typing.Any]], progressReporter: typing.Optional[ProgressReporter] = None) -> None:
		"""Copies the data into this base with progress reporting."""
		if progressReporter is None:
			progressReporter = DummyProgressReporter
		try:
			l = len(target)
		except TypeError:
			l = None
		if issubclass(progressReporter, ProgressReporter):
			r = progressReporter(l, "P")
		else:
			r = progressReporter

		for k, v in target:
			self[k] = v
			r.report(k)

	def recompress(self, targetPath: typing.Optional["pathlib.Path"] = None, compressor: typing.Optional[Compressor] = None, progressReporter: ProgressReporter = None):
		"""Changes database compression. Does this the following way: creates a new DB and compresses to it. Then deletes the old DB and renames the temp one to the old one. We could have implemented in-place recompression, but it would require to store the info about compression progress and have logic to handle that. This is out of scope, this recompression feature is not meant to be used in production because you usually want to use the compression algo (in the most general sense) well-suited for your task. The main purpose of that func is to compress the base with different algos and see how well performs each algo for your task, and then use it."""
		entered = bool(self.backend)
		if entered:  # entered
			self.__exit__(None, None, None)

		if compressor is None:
			compressor = self.compressor

		def dup(targetPath, compressor):
			with self.__class__(targetPath, compressor, commitOnNOps=sys.maxsize) as target:
				target.populate(self.items(), progressReporter=progressReporter)
				target.vacuum()

		with self:
			if targetPath is None:
				targetPath = self.backend.path.parent / (self.backend.path.name + ".tmp")
				dup(targetPath, compressor)
				targetPath.rename(self.backend.path)
			else:
				dup(targetPath, compressor)

		if entered:
			self.__enter__()

	def _recompressorFunc(self, v):
		"""Recomresses a single value. Meant to be used from SQLite. A workaround to a bug with inserting while iterating."""
		if not self.newCompressor:
			raise Exception("Fuck")
		return self.newCompressor.unprocess(self.compressor.process(v))

	def optimizeCompression(self, dictSize: int = None, progressReporter: typing.Optional[ProgressReporter] = None):
		"""Creates shared dictionary for all the records"""
		if not self.compressor or not self.compressor.supportsDicts:
			raise NotImplementedError("Compressor " + repr(self.compressor) + " doesn't support shared dicts")
		self.commit()

		if dictSize is None:
			try:
				occupancy = self.getCacheTableDataSize()
				dataSize = occupancy["total"] - occupancy["wasted"]
				dictSize = dataSize // 10  # the recommended rule of thumb
			except BaseException:
				dictSize = None

		newDict = self.compressor.compute_optimal_dict(self.decompressedValuesBytes(), dictSize)
		self.applyCompressionDictionary(newDict, progressReporter)


class StringCache(BlobCache):
	_appendTransformers = (utf8Transformer,)


class JSONCache(StringCache):
	_appendTransformers = (jsonSerializer,)


Cache = JSONCache

try:
	from transformerz.serialization.pon import ponSerializer

	class PONCache(StringCache):
		_appendTransformers = (ponSerializer,)
except ImportError:
	pass

try:
	from transformerz.serialization.bson import bsonSerializer

	class BSONCache(BlobCache):
		_appendTransformers = (bsonSerializer,)

	Cache = BSONCache
except ImportError:
	pass

try:
	from transformerz.serialization.msgpack import msgpackSerializer

	class MsgPackCache(BlobCache):
		_appendTransformers = (msgpackSerializer,)

	Cache = MsgPackCache
except ImportError:
	pass


try:
	from transformerz.serialization.cbor import cborSerializer

	class CBORCache(BlobCache):
		_appendTransformers = (cborSerializer,)

	Cache = CBORCache
except ImportError:
	pass
