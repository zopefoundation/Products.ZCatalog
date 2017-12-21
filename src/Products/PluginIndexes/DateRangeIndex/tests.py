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
from datetime import datetime
from DateTime.DateTime import DateTime
from BTrees.IIBTree import IISet
from OFS.SimpleItem import SimpleItem
from Testing.makerequest import makerequest

from Products.PluginIndexes.util import datetime_to_minutes


class Dummy(object):

    def __init__(self, name, start, stop):
        self._name = name
        self._start = start
        self._stop = stop

    def name(self):
        return self._name

    def start(self):
        return self._start

    def stop(self):
        return self._stop

    def datum(self):
        return (self._start, self._stop)


dummies = [(0, Dummy('a', None, None)),
           (1, Dummy('b', None, None)),
           (2, Dummy('c', 0, None)),
           (3, Dummy('d', 10, None)),
           (4, Dummy('e', None, 4)),
           (5, Dummy('f', None, 11)),
           (6, Dummy('g', 0, 11)),
           (7, Dummy('h', 2, 9)),
           ]


def matchingDummiesByTimeValue(value, precision=1):
    result = []
    value = datetime_to_minutes(value, precision)
    for i, dummy in dummies:
        start = datetime_to_minutes(dummy.start(), precision)
        stop = datetime_to_minutes(dummy.stop(), precision)
        if ((start is None or start <= value) and
                (stop is None or stop >= value)):
            result.append((i, dummy))
    return result


def matchingDummiesByUIDs(uids):
    return [(i, dummies[i]) for i in uids]


class DateRangeIndexTests(unittest.TestCase):

    def _getTargetClass(self):
        from Products.PluginIndexes.DateRangeIndex.DateRangeIndex \
            import DateRangeIndex
        return DateRangeIndex

    def _makeOne(self, id, since_field=None, until_field=None,
                 caller=None, extra=None, precision_value=None):
        klass = self._getTargetClass()
        index = klass(id, since_field, until_field, caller, extra,
                      precision_value=precision_value)

        class DummyZCatalog(SimpleItem):
            id = 'DummyZCatalog'

        # Build pseudo catalog and REQUEST environment
        catalog = makerequest(DummyZCatalog())
        indexes = SimpleItem()

        indexes = indexes.__of__(catalog)
        index = index.__of__(indexes)

        return index

    def _checkApply(self, index, req, expectedValues, resultset=None):

        def checkApply():
            result, used = index._apply_index(req, resultset=resultset)
            if hasattr(result, 'keys'):
                result = result.keys()
            assert used == (index._since_field, index._until_field)
            assert len(result) == len(expectedValues), \
                '%s: %s | %s' % (req, list(result), expectedValues)
            for k, v in expectedValues:
                assert k in result
            return (result, used)

        cache = index.getRequestCache()
        cache.clear()

        # first call; regular check
        result, used = checkApply()
        self.assertEqual(cache._hits, 0)
        self.assertEqual(cache._sets, 1)
        self.assertEqual(cache._misses, 1)

        # second call; caching check
        result, used = checkApply()
        self.assertEqual(cache._hits, 1)
        return result, used

    def test_interfaces(self):
        from Products.PluginIndexes.interfaces import IDateRangeIndex
        from Products.PluginIndexes.interfaces import IPluggableIndex
        from Products.PluginIndexes.interfaces import ISortIndex
        from Products.PluginIndexes.interfaces import IUniqueValueIndex
        from zope.interface.verify import verifyClass

        verifyClass(IDateRangeIndex, self._getTargetClass())
        verifyClass(IPluggableIndex, self._getTargetClass())
        verifyClass(ISortIndex, self._getTargetClass())
        verifyClass(IUniqueValueIndex, self._getTargetClass())

    def test_empty(self):
        empty = self._makeOne('empty')

        self.assertTrue(empty.getEntryForObject(1234) is None)
        empty.unindex_object(1234)  # shouldn't throw

        self.assertFalse(list(empty.uniqueValues('foo')))
        self.assertFalse(list(empty.uniqueValues('foo', withLengths=True)))
        self.assertTrue(empty._apply_index({'zed': 12345}) is None)

        self._checkApply(empty, {'empty': 12345}, [])

    def test_retrieval(self):
        index = self._makeOne('work', 'start', 'stop')

        for i, dummy in dummies:
            index.index_object(i, dummy)

        for i, dummy in dummies:
            self.assertEqual(index.getEntryForObject(i), dummy.datum())

        for value in range(-1, 15):
            matches = matchingDummiesByTimeValue(value)
            results, used = self._checkApply(index, {'work': value}, matches)
            matches = sorted(matches, key=lambda d: d[1].name())

            for result, match in zip(results, matches):
                self.assertEqual(index.getEntryForObject(result),
                                 match[1].datum())

    def test_longdates(self):
        too_large = 2 ** 31
        too_small = -2 ** 31
        index = self._makeOne('work', 'start', 'stop')
        bad = Dummy('bad', too_large, too_large)
        self.assertRaises(OverflowError, index.index_object, 0, bad)
        bad = Dummy('bad', too_small, too_small)
        self.assertRaises(OverflowError, index.index_object, 0, bad)

    def test_floor_date(self):
        index = self._makeOne('work', 'start', 'stop')
        floor = index.floor_value - 1
        bad = Dummy('bad', floor, None)
        index.index_object(0, bad)
        self.assertTrue(0 in index._always.keys())

    def test_ceiling_date(self):
        index = self._makeOne('work', 'start', 'stop')
        ceiling = index.ceiling_value + 1
        bad = Dummy('bad', None, ceiling)
        index.index_object(1, bad)
        self.assertTrue(1 in index._always.keys())

    def test_datetime(self):
        from Products.PluginIndexes.DateIndex.tests import _getEastern
        before = datetime(2009, 7, 11, 0, 0, tzinfo=_getEastern())
        start = datetime(2009, 7, 13, 5, 15, tzinfo=_getEastern())
        between = datetime(2009, 7, 13, 5, 45, tzinfo=_getEastern())
        stop = datetime(2009, 7, 13, 6, 30, tzinfo=_getEastern())
        after = datetime(2009, 7, 14, 0, 0, tzinfo=_getEastern())

        dummy = Dummy('test', start, stop)
        index = self._makeOne('work', 'start', 'stop')
        index.index_object(0, dummy)
        matches = matchingDummiesByUIDs([0])

        self.assertEqual(index.getEntryForObject(0),
                         (DateTime(start).millis() / 60000,
                          DateTime(stop).millis() / 60000))

        self._checkApply(index, {'work': before}, [])

        self._checkApply(index, {'work': start}, matches)

        self._checkApply(index, {'work': between}, matches)

        self._checkApply(index, {'work': stop}, matches)

        self._checkApply(index, {'work': after}, [])

    def test_datetime_naive_timezone(self):
        from Products.PluginIndexes.DateIndex.DateIndex import Local
        before = datetime(2009, 7, 11, 0, 0)
        start = datetime(2009, 7, 13, 5, 15)
        start_local = datetime(2009, 7, 13, 5, 15, tzinfo=Local)
        between = datetime(2009, 7, 13, 5, 45)
        stop = datetime(2009, 7, 13, 6, 30)
        stop_local = datetime(2009, 7, 13, 6, 30, tzinfo=Local)
        after = datetime(2009, 7, 14, 0, 0)

        dummy = Dummy('test', start, stop)
        index = self._makeOne('work', 'start', 'stop')
        index.index_object(0, dummy)
        matches = matchingDummiesByUIDs([0])

        self.assertEqual(index.getEntryForObject(0),
                         (DateTime(start_local).millis() / 60000,
                          DateTime(stop_local).millis() / 60000))

        self._checkApply(index, {'work': before}, [])

        self._checkApply(index, {'work': start}, matches)

        self._checkApply(index, {'work': between}, matches)

        self._checkApply(index, {'work': stop}, matches)

        self._checkApply(index, {'work': after}, [])

    def test_resultset(self):
        index = self._makeOne('work', 'start', 'stop')
        for i, dummy in dummies:
            index.index_object(i, dummy)

        self._checkApply(index, {'work': 20},
                         matchingDummiesByUIDs([0, 1, 2, 3]))

        # a resultset with everything doesn't actually limit
        self._checkApply(index, {'work': 20},
                         matchingDummiesByUIDs([0, 1, 2, 3]),
                         resultset=IISet(range(len(dummies))))

        # a small resultset limits
        self._checkApply(index, {'work': 20},
                         matchingDummiesByUIDs([1, 2]),
                         resultset=IISet([1, 2]))

        # the specified value is included
        self._checkApply(index, {'work': 11},
                         matchingDummiesByUIDs([0, 1, 2, 3, 5, 6]))

        # also for _since_only
        self._checkApply(index, {'work': 10},
                         matchingDummiesByUIDs([0, 1, 2, 3, 5, 6]))

        # the specified value is included with a large resultset
        self._checkApply(index, {'work': 11},
                         matchingDummiesByUIDs([0, 1, 2, 3, 5, 6]),
                         resultset=IISet(range(len(dummies))))

        # this also works for _since_only
        self._checkApply(index, {'work': 10},
                         matchingDummiesByUIDs([0, 1, 2, 3, 5, 6]),
                         resultset=IISet(range(len(dummies))))

        # the specified value is included with a small resultset
        self._checkApply(index, {'work': 11},
                         matchingDummiesByUIDs([0, 5]),
                         resultset=IISet([0, 5, 7]))

    def test_getCounter(self):
        index = self._makeOne('work', 'start', 'stop')
        self.assertEqual(index.getCounter(), 0)

        k, obj = dummies[0]
        index.index_object(k, obj)
        self.assertEqual(index.getCounter(), 1)

        index.unindex_object(k)
        self.assertEqual(index.getCounter(), 2)

        # unknown id
        index.unindex_object(1234)
        self.assertEqual(index.getCounter(), 2)

        # clear is a change
        index.clear()
        self.assertEqual(index.getCounter(), 3)

    def test_precision(self):
        precision = 5
        index = self._makeOne('work', 'start', 'stop',
                              precision_value=precision)

        for i, dummy in dummies:
            index.index_object(i, dummy)

        for i, dummy in dummies:
            datum = map(lambda d: datetime_to_minutes(d, precision),
                        dummy.datum())
            self.assertEqual(index.getEntryForObject(i), tuple(datum))

        for value in range(-1, 15):
            matches = matchingDummiesByTimeValue(value, precision)
            results, used = self._checkApply(index, {'work': value}, matches)
            matches = sorted(matches, key=lambda d: d[1].name())
            for result, match in zip(results, matches):
                datum = map(lambda d: datetime_to_minutes(d, precision),
                            match[1].datum())
                self.assertEqual(index.getEntryForObject(result),
                                 tuple(datum))
