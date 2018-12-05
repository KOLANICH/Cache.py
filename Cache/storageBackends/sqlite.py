import typing
from pathlib import Path
import sqlite3
import warnings

from pantarei import ProgressReporter

from . import StorageBackend
from transformerz.struct import uint64

sqliteTypeToPythonType = {
	"INTEGER": int,
	"TEXT": str,
	"BLOB": bytes,
}


def genPythonTypeToSQLiteType(sqliteTypeToPythonType):
	res = {}
	for k, v in sqliteTypeToPythonType.items():
		if isinstance(v, tuple):
			for vv in v:
				res[vv] = k
		else:
			res[v] = k
	return res


pythonTypeToSQLiteType = genPythonTypeToSQLiteType(sqliteTypeToPythonType)


class SQLiteBackend(StorageBackend):
	"""A backend using SQLite as a key-value storage"""

	__slots__ = ("db",)

	FILE_EXTENSIONS = ("sqlite",)
	BASE_ARG_TYPES = (sqlite3.Connection,)
	NATIVE_VALUE_TYPES = set(pythonTypeToSQLiteType.keys())

	class Table(StorageBackend.Table):
		__slots__ = ()

		def exists(self) -> bool:
			return next(self.parent.db.execute("SELECT count(*) FROM `sqlite_master` WHERE `type`='table' AND `name`=?;", (self.name,)))[0]

		def create(self, keyType: type = str, valueType: type = bytes) -> None:
			self.parent.db.executescript(
				r"""create table `""" + self.name + """` (
					key """ + pythonTypeToSQLiteType[keyType] + """ PRIMARY KEY,
					val """ + pythonTypeToSQLiteType[keyType] + """
				);
				"""
			)

		def getInfo(self):
			res = self.parent.db.execute("PRAGMA table_info(`" + self.name + "`);")
			res.row_factory = sqlite3.Row
			return res

		def getDataSize(self):
			res = self.parent.db.execute("SELECT sum(`pgsize`) as `total`, sum(`unused`) as `wasted` FROM `dbstat` WHERE name=?;", (self.name,))
			res.row_factory = sqlite3.Row
			res = dict(next(res))
			return res

		def __len__(self) -> int:
			cur = self.parent.db.execute("select count(*) from `" + self.name + "`;")
			res = next(cur)[0]
			cur.close()
			return res

		@classmethod
		def rawKeysBytes(cls, dbOrCur, name):
			for rec in dbOrCur.execute("select `key` from `" + name + "`;"):
				yield rec[0]

		@classmethod
		def rawValuesBytes(cls, dbOrCur, name):
			for rec in dbOrCur.execute("select `val` from `" + name + "`;"):
				yield rec[0]

		@classmethod
		def rawItemsBytes(cls, dbOrCur, name):
			return dbOrCur.execute("select `key`, `val` from `" + name + "`;")

		def __iter__(self):
			return self.keys()

		def keys(self):
			return self.__class__.rawKeysBytes(self.parent.db, self.name)

		def values(self):
			return self.__class__.rawValuesBytes(self.parent.db, self.name)

		def items(self):
			return self.__class__.rawItemsBytes(self.parent.db, self.name)

		def __getitem__(self, key: str) -> bytes:
			try:
				try:
					cur = self.parent.db.execute("select `val` from `" + self.name + "` where `key` = ?;", (key,))
					res = next(cur)[0]
					return res
				finally:
					cur.close()
			except StopIteration:
				return None

		def __setitem__(self, key: str, value: bytes):
			self.__class__.setRawBytes(self.parent.db, self.name, key, value)

		@classmethod
		def setRawBytes(cls, dbOrCur, tableName, key: str, val: bytes):
			return dbOrCur.execute("insert or replace into `" + tableName + "` (`key`, `val`) values (?, ?);", (key, val))

		def __delitem__(self, key) -> None:
			self.parent.db.execute(
				"delete from `" + self.name + "` where `key` = ?;",
				(key,)
			)

		def drop(self) -> None:
			self.parent.db.execute("drop table `" + self.name + "`;")
			self.parent.commit()

		def applyToValues(self, funcName: str, ProgressReporter):
			self.parent.db.execute("replace into `" + self.name + "` (`key`, `val`) SELECT `key`, " + funcName + "(`val`) from `" + self.name + "`;")

		def getKeyType(self):
			for r in self.getInfo():
				if r["name"] == "key":
					return sqliteTypeToPythonType[r["type"]]
			return None

	def __init__(self, base: typing.Union[Path, str, sqlite3.Connection] = "./cache.sqlite", metaDataTableName: str = None) -> None:  # pylint:disable=super-init-not-called # metaclass magic
		if isinstance(base, sqlite3.Connection):
			self.path = None
			self.db = base
		elif isinstance(base, (str, Path)):
			self.path = base
			self.db = None
		else:
			raise ValueError("`base` param must be either a path to base, or ':memory', or a sqlite3.Connection object")

	def commit(self):
		self.db.commit()

	def getSQLiteLibCompileOptions(self):
		"""
		{'COMPILER': 'gcc-5.2.0', 'ENABLE_COLUMN_METADATA': True, 'ENABLE_FTS3': True, 'ENABLE_FTS5': True, 'ENABLE_JSON1': True, 'ENABLE_RTREE': True, 'THREADSAFE': 1} for Anaconda for Windows
		{'COMPILER': 'gcc-8.1.0', 'ENABLE_ATOMIC_WRITE': True, 'ENABLE_COLUMN_METADATA': True, 'ENABLE_DBSTAT_VTAB': True, 'ENABLE_FTS3': True, 'ENABLE_FTS5': True, 'ENABLE_GEOPOLY': True, 'ENABLE_JSON1': True, 'ENABLE_LOAD_EXTENSION': True, 'ENABLE_MEMORY_MANAGEMENT': True, 'ENABLE_PREUPDATE_HOOK': True, 'ENABLE_RBU': True, 'ENABLE_RTREE': True, 'ENABLE_SESSION': True, 'ENABLE_SNAPSHOT': True, 'ENABLE_STAT4': True, 'ENABLE_STMTVTAB': True, 'ENABLE_UNKNOWN_SQL_FUNCTION': True, 'ENABLE_UNLOCK_NOTIFY': True, 'ENABLE_UPDATE_DELETE_LIMIT': True, 'HAVE_ISNAN': True, 'LIKE_DOESNT_MATCH_BLOBS': True, 'THREADSAFE': 1, 'USE_ALLOCA': True, "ENABLE_DBPAGE_VTAB": True} for self-compiled with MinGW-w64
		"""

		cur = self.db.execute("PRAGMA compile_options;")
		res = {}
		for r in cur:
			spl = r[0].split("=")
			if len(spl) == 2:
				try:
					spl[1] = int(spl[1])
				except ValueError:
					pass
				res[spl[0]] = spl[1]
			elif len(spl) == 1:
				res[spl[0]] = True
			else:
				res[spl[0]] = spl[0:]

		# cannot be checked via PRAGMA compile_options
		try:
			self.db.execute("select count(*) from `sqlite_dbpage`;")
			res["ENABLE_DBPAGE_VTAB"] = True
		except sqlite3.OperationalError:
			pass

		return res

	def __enter__(self) -> "SQLiteBackend":
		if self.path is not None:
			self.db = sqlite3.connect(str(self.path))
		#self.db.isolation_level = None
		#compileOptions = self.getSQLiteLibCompileOptions()

	def __exit__(self, exc_class, exc, traceprocess) -> None:
		if self.path is not None:
			self.commit()
			self.db.close()
			self.db = None

	def __del__(self) -> None:
		try:
			if self.db is not None:
				self.__exit__(None, None, None)
		except BaseException as ex:  # pylint:disable=broad-except
			warnings.warn("Exception when closing SQLite DB: " + repr(ex))

	def vacuum(self) -> None:
		self.db.execute("reindex;")
		self.db.execute("vacuum;")

	def optimize(self) -> None:
		self.db.execute("PRAGMA optimize;")

	def createFunction(self, name, f):
		self.db.create_function(name, 1, f)

	def beginTransaction(self):
		self.db.execute("begin;")

	def applyFunctionToTableValues(self, funcName, tableName):
		self.db.execute("replace into `" + tableName + "` (`key`, `val`) SELECT `key`, " + funcName + "(`val`) from `" + tableName + "`;")
