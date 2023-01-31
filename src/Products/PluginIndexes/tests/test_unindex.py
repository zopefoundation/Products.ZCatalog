#############################################################################
#
# Copyright (c) 2002 Zope Foundation and Contributors.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE
#
#############################################################################

import unittest

from BTrees.IIBTree import difference
from OFS.SimpleItem import SimpleItem
from Testing.makerequest import makerequest

from Products.ZCatalog.query import IndexQuery


class TestUnIndex(unittest.TestCase):

    def _getTargetClass(self):
        from Products.PluginIndexes.unindex import UnIndex
        return UnIndex

    def _makeOne(self, *args, **kw):
        index = self._getTargetClass()(*args, **kw)

        class DummyZCatalog(SimpleItem):
            id = 'DummyZCatalog'

        # Build pseudo catalog and REQUEST environment
        catalog = makerequest(DummyZCatalog())
        indexes = SimpleItem()

        indexes = indexes.__of__(catalog)
        index = index.__of__(indexes)

        return index

    def _makeConflicted(self):
        from ZODB.POSException import ConflictError

        class Conflicted:

            def __str__(self):
                return 'Conflicted'
            __repr__ = __str__

            def __getattr__(self, id, default=object()):
                raise ConflictError('testing')

        return Conflicted()

    def test_empty(self):
        unindex = self._makeOne(id='empty')
        self.assertEqual(unindex.indexed_attrs, ['empty'])

    def test_removeForwardIndexEntry_with_ConflictError(self):
        from ZODB.POSException import ConflictError
        unindex = self._makeOne(id='conflicted')
        unindex._index['conflicts'] = self._makeConflicted()
        self.assertRaises(ConflictError, unindex.removeForwardIndexEntry,
                          'conflicts', 42)

    def test_get_object_datum(self):
        from Products.PluginIndexes.unindex import _marker
        idx = self._makeOne('interesting')

        dummy = object()
        self.assertEqual(idx._get_object_datum(dummy, 'interesting'), _marker)

        class DummyContent2:
            interesting = 'GOT IT'
        dummy = DummyContent2()
        self.assertEqual(idx._get_object_datum(dummy, 'interesting'),
                         'GOT IT')

        class DummyContent3:
            exc = None

            def interesting(self):
                if self.exc:
                    raise self.exc
                return 'GOT IT'
        dummy = DummyContent3()
        self.assertEqual(idx._get_object_datum(dummy, 'interesting'),
                         'GOT IT')

        dummy.exc = AttributeError
        self.assertEqual(idx._get_object_datum(dummy, 'interesting'), _marker)

        dummy.exc = TypeError
        self.assertEqual(idx._get_object_datum(dummy, 'interesting'), _marker)

    def test_cache(self):
        idx = self._makeOne(id='foo')
        idx.query_options = ('query', 'range', 'not', 'operator')

        def testQuery(record, expect=1):
            cache = idx.getRequestCache()
            cache.clear()

            # First query
            res1 = idx._apply_index(record)

            # Cache set?
            self.assertEqual(cache._sets, expect)

            # Cache miss?
            self.assertEqual(cache._misses, expect)

            # Second Query
            res2 = idx._apply_index(record)

            # Cache hit?
            self.assertEqual(cache._hits, expect)

            # Check if result of second query is equal to first query
            result = difference(res1[0], res2[0])
            self.assertEqual(len(result), 0)

        # Dummy tests, result is always empty.
        # NOTE: Sophisticated tests have to be placed on tests
        # of inherited classes (FieldIndex, KeywordIndex etc.)
        #
        # 'or' operator
        record = {'foo': {'query': ['e', 'f'], 'operator': 'or'}}
        testQuery(record)

        # 'and' operator (currently not supported)
        record = {'foo': {'query': ['e', 'f'], 'operator': 'and'}}
        testQuery(record)

        # 'range' option
        record = {'foo': {'query': ('abc', 'abcd'), 'range': 'min:max'}}
        testQuery(record)

        # 'not' option
        record = {'foo': {'query': ['a', 'ab'], 'not': 'a'}}
        testQuery(record)

    def test_getCounter(self):
        index = self._makeOne('counter')

        self.assertEqual(index.getCounter(), 0)

        class Dummy:
            id = 1
            counter = 'counter'

        obj = Dummy()
        index.index_object(obj.id, obj)
        self.assertEqual(index.getCounter(), 1)

        index.unindex_object(obj.id)
        self.assertEqual(index.getCounter(), 2)

        # unknown id
        index.unindex_object(1234)
        self.assertEqual(index.getCounter(), 2)

        # clear changes the index
        index.clear()
        self.assertEqual(index.getCounter(), 3)

    def test_no_type_error(self):
        '''Check that we do not get a TypeError when trying
        to query an index with a key that has an invalid type
        '''
        index = self._makeOne('counter')

        class Dummy:
            id = 1
            counter = 'test'

        obj = Dummy()
        index.index_object(obj.id, obj)

        # With the right query we can find the object
        query = IndexQuery({'counter': 'test'}, 'counter')
        res = index.query_index(query)
        self.assertListEqual(list(res), [1])

        # If the type is not the expected one we cannot
        query = IndexQuery({'counter': None}, 'counter')
        res = index.query_index(query)
        self.assertListEqual(list(res), [])

        query = IndexQuery({'counter': 42}, 'counter')
        res = index.query_index(query)
        self.assertListEqual(list(res), [])

    def test_not(self):
        index = self._makeOne("idx")
        index.query_options = "not", "operator"  # activate `not`, `operator`
        apply = index._apply_index
        for i, vals in enumerate(((10, 11, 12), (11, 12, 13))):
            for v in vals:
                index.insertForwardIndexEntry(v, i)
        query = {"query": (10, 11), "not": (10,)}
        req = dict(idx=query)
        # or(10,11), not(10)
        self.assertEqual((1, ), tuple(apply(req)[0]), "or(10,11), not(10)")
        # and(10, 11), not(10)
        query["operator"] = "and"
        self.assertEqual((), tuple(apply(req)[0]), "and(10, 11), not(10)")
        # 11, not 10
        query["query"] = 11
        self.assertEqual((1,), tuple(apply(req)[0]), "11, not(10)")

    def test_range(self):
        index = self._makeOne("idx")
        index.query_options = "range", "usage"  # activate `range`, `usage`
        apply = index._apply_index
        docs = tuple(range(10))
        for i in docs:
            index.insertForwardIndexEntry(i, i)
        ranges = (9, None), (None, 1), (5, 6),  # fails: (None, None),
        for op in ("range", "usage"):
            for r in ranges:
                spec = (["range"] if op == "usage" else []) \
                    + (["min"] if r[0] is not None else []) \
                    + (["max"] if r[1] is not None else [])
                query = {"query": [v for v in r if v is not None],
                         op: ":".join(spec)}
                self.assertEqual(
                    docs[r[0]: (r[1] + 1 if r[1] is not None else None)],
                    tuple(apply(dict(idx=query))[0]),
                    f"{op}: {r}")
