#!/usr/bin/env python3
import typing
import random
import sqlite3
import sys
import unittest
import warnings
from functools import partial
from pathlib import Path
import random
import sqlite3
import unittest

thisDir = Path(__file__).parent.absolute()
sys.path.insert(0, str(thisDir.parent))

from Cache import StringCache, JSONCache, PONCache, CBORCache, MsgPackCache, BlobCache

dictCapableCaches = (JSONCache, PONCache, MsgPackCache, CBORCache)

databasesFilesDir = thisDir / "databasesFiles"


def testString():
	return "Aa12Бб_" + str(random.randint(1, 0xFFFFFFFF))  # nosec


class SimpleTests(unittest.TestCase):
	testInMemory = True  # Change it to `False` if you want files to be written on disk to be able to examine them with tools like DBeaver
	databasesFilesDir = None

	@classmethod
	def setUpClass(cls):
		if not cls.testInMemory:
			cls.databasesFilesDir = databasesFilesDir / cls.__name__
			cls.databasesFilesDir.mkdir(parents=True, exist_ok=True)

	def doTestRoutine(self, db, cacheClass, testObj, testKey="a"):
		with cacheClass(db) as c:
			initialKeys = tuple(c.keys())
			initialValues = tuple(c.values())
			initialItems = tuple(c.items())
			initialLen = len(initialKeys)

			with self.subTest(operation="insertion"):
				c[testKey] = testObj
			with cacheClass(db) as c:
				self.assertEqual(c[testKey], testObj)

			with cacheClass(db) as c:
				c[testKey] = testObj
			with self.subTest(operation="None deletion"):
				with cacheClass(db) as c:
					c[testKey] = None
				with cacheClass(db) as c:
					self.assertEqual(c[testKey], None)
				with cacheClass(db) as c:
					self.assertEqual(testKey in c, False)

			with cacheClass(db) as c:
				c[testKey] = testObj
			with self.subTest(operation="del deletion"):
				with cacheClass(db) as c:
					del c[testKey]
				with cacheClass(db) as c:
					self.assertEqual(c[testKey], None)
				with cacheClass(db) as c:
					self.assertEqual(testKey in c, False)

			with cacheClass(db) as c:
				c[testKey] = testObj
				c.vacuum()

			with cacheClass(db) as c:
				with self.subTest(operation="__len__"):
					self.assertEqual(len(c), initialLen + 1)

				etalonKeys = initialKeys + (testKey,)

				with self.subTest(operation="__iter__"):
					self.assertEqual(tuple(c), etalonKeys)

				with self.subTest(operation="keys"):
					self.assertEqual(tuple(c.keys()), etalonKeys)

				with self.subTest(operation="values"):
					self.assertEqual(tuple(c.values()), initialValues + (testObj,))

				with self.subTest(operation="items"):
					self.assertEqual(tuple(c.items()), initialItems + ((testKey, testObj),))

			with cacheClass(db) as c:
				c[testKey] = testObj
			with self.subTest(operation="empty"):
				with cacheClass(db) as c:
					c.empty()
					self.assertEqual(c[testKey], None)


class SQLiteSimpleTests(SimpleTests):
	testInMemory = True  # Change it to `False` if you want files to be written on disk to be able to examine them with tools like DBeaver

	def testBlobCache(self):
		with sqlite3.connect(":memory:" if self.testInMemory else self.__class__.databasesFilesDir / "blobCache.sqlite") as db:
			self.doTestRoutine(db, BlobCache, testString().encode("utf-8"))

	def testStringCache(self):
		with sqlite3.connect(":memory:" if self.testInMemory else self.__class__.databasesFilesDir / "stringCache.sqlite") as db:
			self.doTestRoutine(db, StringCache, testString())

	def testDictCapableCaches(self):
		for cacheClass in dictCapableCaches:
			with self.subTest(modelClass=cacheClass):
				with sqlite3.connect(":memory:" if self.testInMemory else self.__class__.databasesFilesDir / (cacheClass.__name__ + ".sqlite")) as db:
					self.doTestRoutine(db, cacheClass, testString())
					self.doTestRoutine(db, cacheClass, {"a": [1, 2, 3.5], "b": None, "c": {}}, "b")

	def testIntKeysCaches(self):
		with sqlite3.connect(":memory:" if self.testInMemory else self.__class__.databasesFilesDir / "stringCacheInt.sqlite") as db:
			self.doTestRoutine(db, partial(StringCache, keyType=int), testString(), testKey=1)

	def testAnyKeysCaches(self):
		keyValues = [
			[1, 2],
			1,
			2.,
			"aaa",
			{"a":2, "b":"c", "d":None}
		]
		for cacheClass in dictCapableCaches:
			with self.subTest(modelClass=cacheClass):
				with sqlite3.connect(":memory:" if self.testInMemory else self.__class__.databasesFilesDir / (cacheClass.__name__ + "AnyKeys.sqlite")) as db:
					for testKey in keyValues:
						with self.subTest(testKey=testKey):
							self.doTestRoutine(db, partial(cacheClass, keyType=typing.Any), testString(), testKey=testKey)

	def testCompression(self):
		for compressor in ("lzma", "deflate", "brotli", "lz4", "zstd", "bzip2"):
			with self.subTest(compressor=compressor):
				with sqlite3.connect(":memory:" if self.testInMemory else self.__class__.databasesFilesDir / ("stringCache_" + compressor + ".sqlite")) as db:
					self.doTestRoutine(db, partial(StringCache, compressorFactory=compressor), testString())

	def testCompressionOptimization(self):
		for compressor in ("zstd",):
			testDataCount = 17
			testStringRepeats = 10
			data = [None] * testDataCount
			for i in range(testDataCount):
				data[i] = testString() * testStringRepeats

			with sqlite3.connect(":memory:" if self.testInMemory else self.__class__.databasesFilesDir / ("stringCache_" + compressor + "_optimization.sqlite")) as db:
				with StringCache(db, compressor, keyType=int) as c:
					c.populate(enumerate(data))
					with self.subTest(compressor=compressor):
						c.optimizeCompression()
						decompressedData = list(c.values())
						self.assertEqual(data, decompressedData)

				self.doTestRoutine(db, partial(StringCache, compressorFactory=compressor, keyType=int), data[0], testKey=100500)


class LMDBSimpleTests(SimpleTests):
	testInMemory = False

	def testBlobCache(self):
		self.doTestRoutine(self.__class__.databasesFilesDir / "blobCache.mdb", BlobCache, testString().encode("utf-8"))

	def testStringCache(self):
		self.doTestRoutine(self.__class__.databasesFilesDir / "stringCache.mdb", StringCache, testString())

	def testDictCapableCaches(self):
		for cacheClass in dictCapableCaches:
			with self.subTest(modelClass=cacheClass):
				dbFileName = self.__class__.databasesFilesDir / (cacheClass.__name__ + ".mdb")
				self.doTestRoutine(dbFileName, cacheClass, testString())
				self.doTestRoutine(dbFileName, cacheClass, {"a": [1, 2, 3.5], "b": None, "c": {}}, "b")

	def testIntKeysCaches(self):
		self.doTestRoutine(self.__class__.databasesFilesDir / "stringCacheInt.mdb", partial(StringCache, keyType=int), testString(), testKey=1)

	def testAnyKeysCaches(self):
		keyValues = [
			[1, 2],
			1,
			2.,
			"aaa",
			{"a":2, "b":"c", "d":None}
		]
		for cacheClass in dictCapableCaches:
			with self.subTest(modelClass=cacheClass):
				for testKey in keyValues:
					with self.subTest(testKey=testKey):
						self.doTestRoutine(self.__class__.databasesFilesDir / (cacheClass.__name__ + "AnyKeys.mdb"), partial(cacheClass, keyType=typing.Any), testString(), testKey=testKey)

	@unittest.skip
	def testCompression(self):
		for compressor in ("lzma", "deflate", "brotli", "lz4", "zstd", "bzip2"):
			with self.subTest(compressor=compressor):
				self.doTestRoutine(self.__class__.databasesFilesDir / ("stringCache_" + compressor + ".mdb"), partial(StringCache, compressorFactory=compressor), testString())

	@unittest.skip
	def testCompressionOptimization(self,):
		for compressor in ("zstd",):
			testDataCount = 17
			testStringRepeats = 10
			data = [None] * testDataCount
			for i in range(testDataCount):
				data[i] = testString() * testStringRepeats

			fileName = self.__class__.databasesFilesDir / ("stringCache_" + compressor + "_optimization.mdb")
			with StringCache(fileName, compressor, keyType=int) as c:
				c.populate(enumerate(data))
				with self.subTest(compressor=compressor):
					c.optimizeCompression()
					decompressedData = list(c.values())
					self.assertEqual(data, decompressedData)

			self.doTestRoutine(fileName, partial(StringCache, compressorFactory=compressor, keyType=int), data[0], testKey=100500)


if __name__ == "__main__":
	unittest.main()
