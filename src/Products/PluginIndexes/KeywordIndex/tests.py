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

import unittest
import sys

from OFS.SimpleItem import SimpleItem
from Testing.makerequest import makerequest

from Products.PluginIndexes.interfaces import (
    missing,
    empty,
)

PY3 = sys.version_info > (3, )


class Dummy(object):

    def __init__(self, foo):
        self._foo = foo

    def foo(self):
        return self._foo

    def __str__(self):
        return '<Dummy: %s>' % self._foo

    __repr__ = __str__


class TestKeywordIndex(unittest.TestCase):

    _old_log_write = None

    def _getTargetClass(self):
        from Products.PluginIndexes.KeywordIndex.KeywordIndex \
            import KeywordIndex
        return KeywordIndex

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
        self._values = [(0, Dummy(['a'])),
                        (1, Dummy(['a', 'b'])),
                        (2, Dummy(['a', 'b', 'c'])),
                        (3, Dummy(['a', 'b', 'c', 'a'])),
                        (4, Dummy(['a', 'b', 'c', 'd'])),
                        (5, Dummy(['a', 'b', 'c', 'e'])),
                        (6, Dummy(['a', 'b', 'c', 'e', 'f'])),
                        (7, Dummy(['0'])),
                        (8, Dummy([])),
                        (9, Dummy(None)),
                        ]
        self._noop_req = {'bar': 123}
        self._all_req = {'foo': ['a']}
        self._some_req = {'foo': ['e']}
        self._overlap_req = {'foo': ['c', 'e']}
        self._string_req = {'foo': 'a'}
        self._zero_req = {'foo': ['0']}
        self._miss_req_1 = {'foo': [missing]}
        self._miss_req_2 = {'foo': ['a', missing]}
        self._empty_req_1 = {'foo': [empty]}
        self._empty_req_2 = {'foo': [empty, 'f']}

        self._not_1 = {'foo': {'query': 'f', 'not': 'f'}}
        self._not_2 = {'foo': {'query': ['e', 'f'], 'not': 'f'}}
        self._not_3 = {'foo': {'not': '0'}}
        self._not_4 = {'foo': {'not': ['0', 'e']}}
        self._not_5 = {'foo': {'not': ['0', 'no-value']}}
        self._not_6 = {'foo': 'c', 'bar': {'query': 123, 'not': 1}}
        self._not_7 = {'foo': {'not': [missing]}}
        self._not_8 = {'foo': {'not': []}}
        self._not_9 = {'foo': {'not': [empty, ]}}
        self._not_10 = {'foo': {'not': [empty, 'f']}}

    def _populateIndex(self):
        for k, v in self._values:
            self._index.index_object(k, v)

    def _checkApply(self, req, expectedValues):

        def checkApply():
            result, used = self._index._apply_index(req)
            assert used == ('foo', )
            assert len(result) == len(expectedValues), \
                '%s | %s' % (list(result),
                             list(map(lambda x: x[0], expectedValues)))

            if hasattr(result, 'keys'):
                result = result.keys()
            for k, v in expectedValues:
                self.assertIn(k, result)

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

    def testAddObjectWOKeywords(self):
        self._populateIndex()
        self._index.index_object(999, None)

    def testEmpty(self):
        assert len(self._index) == 0
        assert len(self._index.referencedObjects()) == 0
        self.assertEqual(self._index.numObjects(), 0)

        assert self._index.getEntryForObject(1234) is None
        assert (self._index.getEntryForObject(1234, self._marker)
                is self._marker), self._index.getEntryForObject(1234)
        self._index.unindex_object(1234)  # nothrow

        assert self._index.hasUniqueValuesFor('foo')
        assert not self._index.hasUniqueValuesFor('bar')
        assert len(list(self._index.uniqueValues('foo'))) == 0

        assert self._index._apply_index(self._noop_req) is None
        self._checkApply(self._all_req, [])
        self._checkApply(self._some_req, [])
        self._checkApply(self._overlap_req, [])
        self._checkApply(self._string_req, [])
        self._checkApply(self._miss_req_1, [])
        self._checkApply(self._miss_req_2, [])

    def testPopulated(self):
        self._populateIndex()
        values = self._values

        self.assertEqual(len(self._index.referencedObjects()), len(values))
        assert self._index.getEntryForObject(1234) is None
        assert (self._index.getEntryForObject(1234, self._marker)
                is self._marker)
        self._index.unindex_object(1234)  # nothrow
        self.assertEqual(self._index.indexSize(), len(values) - 3)

        for k, v in values[:8]:
            entry = self._index.getEntryForObject(k, None)
            entry = list(entry)
            entry.sort()
            kw = sorted(set(v.foo()))
            self.assertEqual(entry, kw)

        empty_indexed = self._index.getSpecialIndex(empty)
        self.assertEqual(8 in empty_indexed, True)

        miss_indexed = self._index.getSpecialIndex(missing)
        self.assertEqual(9 in miss_indexed, True)

        assert len(list(self._index.uniqueValues('foo'))) == len(values) - 3
        assert self._index._apply_index(self._noop_req) is None

        self._checkApply(self._all_req, values[:-3])
        self._checkApply(self._some_req, values[5:7])
        self._checkApply(self._overlap_req, values[2:7])
        self._checkApply(self._string_req, values[:-3])
        self._checkApply(self._miss_req_1, values[9:10])
        self._checkApply(self._miss_req_2, values[:7] + values[9:10])
        self._checkApply(self._empty_req_1, values[8:9])
        self._checkApply(self._empty_req_2, values[6:7] + values[8:9])

        self._checkApply(self._not_1, [])
        self._checkApply(self._not_2, values[5:6])
        self._checkApply(self._not_3, values[:7] + values[8:10])
        self._checkApply(self._not_4, values[:5] + values[8:10])
        self._checkApply(self._not_5, values[:7] + values[8:10])
        self._checkApply(self._not_6, values[2:7])
        self._checkApply(self._not_7, values[:9])
        self._checkApply(self._not_8, values[:10])
        self._checkApply(self._not_9, values[:8] + values[9:10])
        self._checkApply(self._not_10, values[:6] + values[7:8] + values[9:10])

    def testReindexChange(self):
        self._populateIndex()
        values = self._values

        expected = Dummy(['x', 'y'])
        self._index.index_object(6, expected)
        self._checkApply({'foo': ['x', 'y']}, [(6, expected), ])
        self._checkApply({'foo': ['a', 'b', 'c', 'e', 'f']}, values[:6])

        self.assertIs(self._index._unindex.get(9), missing)
        expected = Dummy([])  # empty
        # replace missing with empty
        self._index.index_object(9, expected)
        self._checkApply({'foo': missing}, [])

        expected = Dummy(['z'])
        self._index.index_object(9, expected)
        self._checkApply({'foo': ['z']}, [(9, expected), ])
        self.assertEqual(list(self._index._unindex.get(9)), ['z'])

        self.assertIs(self._index._unindex.get(8), empty)
        expected = Dummy(None)  # missing
        # replace empty with missing
        self._index.index_object(8, expected)
        self._checkApply({'foo': empty}, [])

        expected = Dummy(['q'])
        self._index.index_object(8, expected)
        self._checkApply({'foo': ['q']}, [(8, expected), ])
        self.assertEqual(list(self._index._unindex.get(8)), ['q'])

    def testReindexNoChange(self):
        self._populateIndex()
        self._index.unindex_object(8)
        self._index.unindex_object(9)

        expected = Dummy(['foo', 'bar'])

        res = self._index.index_object(8, expected)
        self.assertEqual(res, True)
        self._checkApply({'foo': ['foo', 'bar']}, [(8, expected), ])

        res = self._index.index_object(8, expected)
        self.assertEqual(res, False)
        self._checkApply({'foo': ['foo', 'bar']}, [(8, expected), ])

        class FauxObject:
            def foo(self):
                raise TypeError

        expected = FauxObject()

        res = self._index.index_object(9, expected)
        self.assertEqual(res, True)
        self._checkApply({'foo': [missing]}, [(9, expected), ])

        res = self._index.index_object(9, expected)
        self.assertEqual(res, False)
        self._checkApply({'foo': [missing]}, [(9, expected), ])

    def testIntersectionWithRange(self):
        # Test an 'and' search, ensuring that 'range' doesn't mess it up.
        self._populateIndex()

        record = {'foo': {'query': ['e', 'f'], 'operator': 'and'}}
        self._checkApply(record, self._values[6:7])

        # Make sure that 'and' tests with incompatible parameters
        # don't return empty sets.
        record['foo']['range'] = 'min:max'
        self._checkApply(record, self._values[6:7])

    def testDuplicateKeywords(self):
        self._index.index_object(0, Dummy(['a', 'a', 'b', 'b']))
        self._index.unindex_object(0)

    def testCollectorIssue889(self):
        # Test that collector issue 889 is solved
        values = self._values
        nonexistent = 'foo-bar-baz'
        self._populateIndex()
        # make sure key is not indexed
        result = self._index._index.get(nonexistent, self._marker)
        assert result is self._marker
        # patched _apply_index now works as expected
        record = {'foo': {'query': [nonexistent], 'operator': 'and'}}
        self._checkApply(record, [])
        record = {'foo': {'query': [nonexistent, 'a'], 'operator': 'and'}}
        # and does not break anything
        self._checkApply(record, [])
        record = {'foo': {'query': ['d'], 'operator': 'and'}}
        self._checkApply(record, values[4:5])
        record = {'foo': {'query': ['a', 'e'], 'operator': 'and'}}
        self._checkApply(record, values[5:7])

    def test_missing_when_noattribute(self):
        to_index = Dummy(['hello'])
        self._index._index_object(10, to_index, attr='UNKNOWN')
        self.assertIs(self._index._unindex.get(10), missing)
        self.assertIs(self._index.getEntryForObject(10), missing)
        self._index.unindex_object(10)
        self.assertIs(self._index.getEntryForObject(10), None)

    def test_missing_when_raising_attribute_error(self):
        class FauxObject:
            def foo(self):
                raise AttributeError
        to_index = FauxObject()
        self._index._index_object(10, to_index, attr='foo')
        self.assertIs(self._index._unindex.get(10), missing)
        self.assertIs(self._index.getEntryForObject(10), missing)

    def test_missing_when_raising_type_error(self):
        self._index.clear()

        if PY3:
            # PY3: BTrees or OOSet store types in sorted order. Keywords of
            # mixed types cannot be sorted and should therefore raise
            # a TypeError.
            self._index.clear()
            to_index = Dummy(['a', tuple('b')])
            self._index._index_object(10, to_index, attr='foo')
            self.assertIs(self._index._unindex.get(10), missing)
            self.assertIs(self._index.getEntryForObject(10), missing)

            # add keyword with different data type
            self._index.clear()
            to_index = Dummy(['a'])
            self._index._index_object(10, Dummy(['a']), attr='foo')
            to_index = Dummy([tuple('b')])
            self._index._index_object(11, to_index, attr='foo')
            self.assertIs(self._index._unindex.get(11), missing)
            self.assertIs(self._index.getEntryForObject(11), missing)

            # replace keyword with different data type
            to_index = Dummy(['b'])
            self._index._index_object(11, to_index, attr='foo')
            to_index = Dummy([tuple('b')])
            self._index._index_object(11, to_index, attr='foo')
            self.assertIs(self._index._unindex.get(11), missing)
            self.assertIs(self._index.getEntryForObject(11), missing)

        # simplest case, call of attribute raises a TypeError
        class FauxObject:
            def foo(self):
                raise TypeError

        self._index.clear()
        to_index = FauxObject()
        self._index._index_object(10, to_index, attr='foo')
        self.assertIs(self._index._unindex.get(10), missing)
        self.assertIs(self._index.getEntryForObject(10), missing)

    def test_value_removes(self):
        to_index = Dummy(['hello'])
        self._index._index_object(10, to_index, attr='foo')
        self.assertEqual(list(self._index._unindex.get(10)), ['hello'])

        to_index = Dummy('')
        self._index._index_object(10, to_index, attr='foo')
        self.assertEqual(list(self._index._unindex.get(10)), [''])

        index_len = len(self._index)
        empty_len = len(self._index.getSpecialIndex(empty))
        missing_len = len(self._index.getSpecialIndex(missing))

        to_index = Dummy(tuple())
        self._index._index_object(10, to_index, attr='foo')
        self.assertEqual(self._index._unindex.get(10), empty)
        self.assertEqual(len(self._index), index_len - 1)
        self.assertEqual(len(self._index.getSpecialIndex(empty)),
                         empty_len + 1)
        self.assertEqual(len(self._index.getSpecialIndex(missing)),
                         missing_len)

        index_len = len(self._index)
        empty_len = len(self._index.getSpecialIndex(empty))
        missing_len = len(self._index.getSpecialIndex(missing))

        to_index = Dummy(None)
        self._index._index_object(10, to_index, attr='foo')
        self.assertEqual(self._index._unindex.get(10), missing)
        self.assertEqual(len(self._index), index_len)
        self.assertEqual(len(self._index.getSpecialIndex(empty)),
                         empty_len - 1)
        self.assertEqual(len(self._index.getSpecialIndex(missing)),
                         missing_len + 1)

    def test_getCounter(self):
        index = self._makeOne('foo')

        self.assertEqual(index.getCounter(), 0)

        obj = Dummy(['hello'])
        index.index_object(10, obj)
        self.assertEqual(index.getCounter(), 1)

        index.unindex_object(10)
        self.assertEqual(index.getCounter(), 2)

        # unknown id
        index.unindex_object(1234)
        self.assertEqual(index.getCounter(), 2)

        # clear is a change
        index.clear()
        self.assertEqual(index.getCounter(), 3)
