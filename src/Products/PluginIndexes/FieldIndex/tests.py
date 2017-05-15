##############################################################################
#
# Copyright (c) 2002 Zope Foundation and Contributors.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE.
#
##############################################################################
"""FieldIndex unit tests.
"""

import unittest

from OFS.SimpleItem import SimpleItem
from Testing.makerequest import makerequest


class Dummy(object):

    def __init__(self, foo):
        self._foo = foo

    def foo(self):
        return self._foo

    def __str__(self):
        return '<Dummy: %s>' % self._foo

    __repr__ = __str__


class FieldIndexTests(unittest.TestCase):
    """Test FieldIndex objects.
    """
    def _getTargetClass(self):
        from Products.PluginIndexes.FieldIndex.FieldIndex \
            import FieldIndex
        return FieldIndex

    def _makeOne(self, id, extra=None):
        klass = self._getTargetClass()
        index = klass(id, extra=extra)

        class DummyZCatalog(SimpleItem):
            id = 'DummyZCatalog'

        # Build pseudo catalog and REQUEST environment
        catalog = makerequest(DummyZCatalog())
        indexes = SimpleItem()

        indexes = indexes.__of__(catalog)
        index = index.__of__(indexes)

        return index

    def setUp(self):
        self._index = self._makeOne('foo')
        self._marker = []
        self._values = [(0, Dummy('a')),
                        (1, Dummy('ab')),
                        (2, Dummy('abc')),
                        (3, Dummy('abca')),
                        (4, Dummy('abcd')),
                        (5, Dummy('abce')),
                        (6, Dummy('abce')),
                        (7, Dummy('0'))]
        self._forward = {}
        self._backward = {}
        for k, v in self._values:
            self._backward[k] = v
            keys = self._forward.get(v, [])
            self._forward[v] = keys

        self._noop_req = {'bar': 123}
        self._request = {'foo': 'abce'}
        self._min_req = {'foo': {'query': 'abc', 'range': 'min'}}
        self._min_req_n = {'foo': {'query': 'abc',
                                   'range': 'min',
                                   'not': 'abca'}}
        self._max_req = {'foo': {'query': 'abc',
                                 'range': 'max'}}
        self._max_req_n = {'foo': {'query': 'abc',
                                   'range': 'max',
                                   'not': ['a', 'b', '0']}}
        self._range_req = {'foo': {'query': ('abc', 'abcd'),
                                   'range': 'min:max'}}
        self._range_ren = {'foo': {'query': ('abc', 'abcd'),
                                   'range': 'min:max',
                                   'not': 'abcd'}}
        self._range_non = {'foo': {'query': ('a', 'aa'),
                                   'range': 'min:max',
                                   'not': 'a'}}
        self._zero_req = {'foo': '0'}
        self._not_1 = {'foo': {'query': 'a', 'not': 'a'}}
        self._not_2 = {'foo': {'query': ['a', 'ab'], 'not': 'a'}}
        self._not_3 = {'foo': {'not': 'a'}}
        self._not_4 = {'foo': {'not': ['0']}}
        self._not_5 = {'foo': {'not': ['a', 'b']}}
        self._not_6 = {'foo': 'a', 'bar': {'query': 123, 'not': 1}}

    def _populateIndex(self):
        for k, v in self._values:
            self._index.index_object(k, v)

    def _checkApply(self, req, expectedValues):

        def checkApply():
            result, used = self._index._apply_index(req)
            if hasattr(result, 'keys'):
                result = result.keys()
            assert used == ('foo', )
            assert len(result) == len(expectedValues), \
                '%s | %s' % (list(result), expectedValues)
            for k, v in expectedValues:
                self.assertTrue(k in result)

        index = self._index

        cache = index.getRequestCache()
        cache.clear()

        # first call; regular check
        checkApply()
        self.assertEqual(cache._hits, 0)
        self.assertEqual(cache._sets, 1)
        self.assertEqual(cache._misses, 1)

        # second call; caching check
        checkApply()
        self.assertEqual(cache._hits, 1)

    def test_interfaces(self):
        from Products.PluginIndexes.interfaces import IPluggableIndex
        from Products.PluginIndexes.interfaces import ISortIndex
        from Products.PluginIndexes.interfaces import IUniqueValueIndex
        from Products.PluginIndexes.interfaces import IRequestCacheIndex
        from zope.interface.verify import verifyClass

        klass = self._getTargetClass()

        verifyClass(IPluggableIndex, klass)
        verifyClass(ISortIndex, klass)
        verifyClass(IUniqueValueIndex, klass)
        verifyClass(IRequestCacheIndex, klass)

    def testEmpty(self):
        "Test an empty FieldIndex."

        assert len(self._index) == 0
        assert len(self._index.referencedObjects()) == 0
        self.assertEqual(self._index.numObjects(), 0)

        assert self._index.getEntryForObject(1234) is None
        assert (self._index.getEntryForObject(1234, self._marker)
                is self._marker)
        self._index.unindex_object(1234)  # nothrow

        assert self._index.hasUniqueValuesFor('foo')
        assert not self._index.hasUniqueValuesFor('bar')
        assert len(list(self._index.uniqueValues('foo'))) == 0

        assert self._index._apply_index(self._noop_req) is None
        self._checkApply(self._request, [])
        self._checkApply(self._min_req, [])
        self._checkApply(self._min_req_n, [])
        self._checkApply(self._max_req, [])
        self._checkApply(self._max_req_n, [])
        self._checkApply(self._range_req, [])
        self._checkApply(self._range_ren, [])
        self._checkApply(self._range_non, [])

    def testPopulated(self):
        """ Test a populated FieldIndex """
        self._populateIndex()
        values = self._values

        assert len(self._index) == len(values) - 1  # 'abce' is duplicate
        assert len(self._index.referencedObjects()) == len(values)
        self.assertEqual(self._index.indexSize(), len(values) - 1)

        assert self._index.getEntryForObject(1234) is None
        assert (self._index.getEntryForObject(1234, self._marker)
                is self._marker)
        self._index.unindex_object(1234)  # nothrow

        for k, v in values:
            assert self._index.getEntryForObject(k) == v.foo()

        assert len(list(self._index.uniqueValues('foo'))) == len(values) - 1

        assert self._index._apply_index(self._noop_req) is None

        self._checkApply(self._request, values[-3:-1])
        self._checkApply(self._min_req, values[2:-1])
        self._checkApply(self._min_req_n, values[2:3] + values[4:-1])
        self._checkApply(self._max_req, values[:3] + values[-1:])
        self._checkApply(self._max_req_n, values[1:3])
        self._checkApply(self._range_req, values[2:5])
        self._checkApply(self._range_ren, values[2:4])
        self._checkApply(self._range_non, [])

        self._checkApply(self._not_1, [])
        self._checkApply(self._not_2, values[1:2])
        self._checkApply(self._not_3, values[1:])
        self._checkApply(self._not_4, values[:7])
        self._checkApply(self._not_5, values[1:])
        self._checkApply(self._not_6, values[0:1])

    def testNone(self):
        # Make sure None is ignored.
        self._index.index_object(10, Dummy(None))
        self.assertFalse(None in self._index.uniqueValues('foo'))
        self._checkApply({'foo': None}, [])

    def testReindex(self):
        self._populateIndex()
        self._checkApply({'foo': 'abc'}, [self._values[2], ])
        assert self._index.keyForDocument(2) == 'abc'
        d = Dummy('world')
        self._index.index_object(2, d)
        self._checkApply({'foo': 'world'}, [(2, d), ])
        assert self._index.keyForDocument(2) == 'world'
        del d._foo
        self._index.index_object(2, d)
        self._checkApply({'foo': 'world'}, [])
        try:
            should_not_be = self._index.keyForDocument(2)
        except KeyError:
            # As expected, we deleted that attribute.
            pass
        else:
            # before Collector #291 this would be 'world'
            raise ValueError(repr(should_not_be))

    def testRange(self):
        """Test a range search"""
        index = self._index
        index.clear()

        record = {'foo': {'query': [-99, 3], 'range': 'min:max'}}

        expect = []
        for i in range(100):
            val = i % 10
            obj = Dummy(val)

            if val >= -99 and val <= 3:
                expect.append((i, obj))

            index.index_object(i, obj)

        self._checkApply(record, expect)

        # Make sure that range tests with incompatible paramters
        # don't return empty sets.
        record['foo']['operator'] = 'and'
        self._checkApply(record, expect)
