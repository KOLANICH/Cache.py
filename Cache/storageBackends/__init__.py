import typing
from abc import ABC, ABCMeta, abstractmethod
from pathlib import Path

from pantarei import ProgressReporter

class Tablez:
	"""A class that manages tables creation and/or closing"""

	__slots__ = ("_parent", "_tables")

	def __init__(self, parent):
		self._parent = parent
		self._tables = {}

	def __enter__(self) -> "Tablez":
		return self

	def __exit__(self, exc_class, exc, traceprocess) -> None:
		for tn in tuple(self._tables.keys()):
			self._tables[tn].__exit__(exc_class, exc, traceprocess)
			del self._tables[tn]

	def __getattr__(self, k):
		try:
			return self._tables[k]
		except KeyError:
			raise KeyError("Create a mapping from table name to variable name " + repr(k) + " using `map` method first")

	def map(self, tableName, variableName=None):
		if variableName is None:
			variableName = tableName
		self._tables[variableName] = self._parent.__class__.Table(self._parent, tableName).__enter__()


class _StorageBackend(ABC):
	__slots__ = ("path", "tables",)
	"""A unified interface to dumb key-value storages"""
	FILE_EXTENSIONS = None  # see StorageBackendMeta for more info
	BASE_ARG_TYPES = None

	class Table(ABC):
		__slots__ = ("name", "parent")

		def __init__(self, parent, name) -> None:
			self.name = name
			self.parent = parent

		@abstractmethod
		def exists(self) -> bool:
			raise NotImplementedError()

		@abstractmethod
		def create(self, keyType: type = str, valueType: type = bytes) -> None:
			raise NotImplementedError()

		@abstractmethod
		def getInfo(self):
			raise NotImplementedError()

		@abstractmethod
		def getDataSize(self):
			raise NotImplementedError()

		@abstractmethod
		def __len__(self) -> int:
			raise NotImplementedError()

		@abstractmethod
		def keys(self):
			raise NotImplementedError()

		@abstractmethod
		def values(self):
			raise NotImplementedError()

		@abstractmethod
		def items(self):
			raise NotImplementedError()

		@abstractmethod
		def __getitem__(self, key: str) -> bytes:
			raise NotImplementedError()

		@abstractmethod
		def __setitem__(self, key: str, value: bytes):
			raise NotImplementedError()

		@abstractmethod
		def __delitem__(self, key) -> None:
			raise NotImplementedError()

		@abstractmethod
		def drop(self) -> None:
			raise NotImplementedError()

		@abstractmethod
		def applyToValues(self, funcName: str, progressReporter: ProgressReporter):
			raise NotImplementedError()

		def __enter__(self) -> "Table":
			return self

		def __exit__(self, exc_class, exc, traceprocess) -> None:
			pass

		@abstractmethod
		def getKeyType(self):
			raise NotImplementedError

	@abstractmethod
	def __init__(self, *, base: typing.Union[Path, str, typing.Any] = None, metaDataTableName: str = None) -> None:
		raise NotImplementedError()

	@abstractmethod
	def commit(self):
		raise NotImplementedError()

	@abstractmethod
	def __enter__(self) -> "SQLiteBackend":
		raise NotImplementedError()

	@abstractmethod
	def __exit__(self, exc_class, exc, traceprocess) -> None:
		raise NotImplementedError()

	@abstractmethod
	def __del__(self) -> None:
		raise NotImplementedError()

	@abstractmethod
	def vacuum(self) -> None:
		raise NotImplementedError()

	@abstractmethod
	def optimize(self) -> None:
		raise NotImplementedError()

	@abstractmethod
	def createFunction(self, name, f):
		raise NotImplementedError()

	@abstractmethod
	def beginTransaction(self):
		raise NotImplementedError()


extensionsRegistry = {}
typesRegistry = {}


def _constructTablesForObj(self):
	self.tables = Tablez(self)


class StorageBackendMeta(ABCMeta):
	__slots__ = ()
	def __new__(cls, className, parents, attrs, *args, **kwargs):
		attrs = type(attrs)(attrs)

		# if we define __init__ in a class which would have inherited it from _StorageBackend otherwise
		if "__init__" in attrs and len(parents) == 1 and parents[0].__init__ is _StorageBackend.__init__:
			originalInit = attrs["__init__"]
			# add the code creating `tables` prop automatically

			def __init__(self, *args, **kwargs):
				_constructTablesForObj(self)
				originalInit(self, *args, **kwargs)

			__init__.__wraps__ = originalInit
			attrs["__init__"] = __init__

		res = super().__new__(cls, className, parents, attrs, *args, **kwargs)
		if "FILE_EXTENSIONS" in attrs:
			if attrs["FILE_EXTENSIONS"] is not None:
				for ext in attrs["FILE_EXTENSIONS"]:
					extensionsRegistry[ext] = res

		# we could have used type annotations for this, ... but they gonna become dumb strings
		if "BASE_ARG_TYPES" in attrs:
			if attrs["BASE_ARG_TYPES"] is not None:
				for ext in attrs["BASE_ARG_TYPES"]:
					typesRegistry[ext] = res
		return res


def getBackendClass(obj: typing.Any) -> typing.Type[_StorageBackend]:
	if isinstance(obj, str):
		obj = Path(obj)
	if isinstance(obj, Path):
		ext = obj.suffix
		if ext:
			ext = ext[1:].lower()
			if ext in extensionsRegistry:
				return extensionsRegistry[ext]
		return None
	if obj.__class__ in typesRegistry:
		return typesRegistry[obj.__class__]
	return None


class StorageBackend(_StorageBackend, metaclass=StorageBackendMeta):  #pylint:disable=abstract-method
	__slots__ = ()
