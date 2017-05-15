##############################################################################
#
# Copyright (c) 2010 Zope Foundation and Contributors.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE.
#
##############################################################################

import os
import os.path
from six.moves._thread import LockType
import time
import unittest

from zope.testing import cleanup

from Products.PluginIndexes.BooleanIndex.BooleanIndex import BooleanIndex
from Products.PluginIndexes.DateRangeIndex.DateRangeIndex import DateRangeIndex
from Products.PluginIndexes.FieldIndex.FieldIndex import FieldIndex
from Products.PluginIndexes.KeywordIndex.KeywordIndex import KeywordIndex
from Products.PluginIndexes.PathIndex.PathIndex import PathIndex
from Products.PluginIndexes.UUIDIndex.UUIDIndex import UUIDIndex
from Products.ZCatalog.Catalog import Catalog
from Products.ZCatalog.ZCatalog import ZCatalog


HERE = __file__


class Dummy(object):

    def __init__(self, num):
        self.num = num

    def big(self):
        return self.num > 5

    def numbers(self):
        return (self.num, self.num + 1)

    def getPhysicalPath(self):
        return '/%s' % self.num

    def start(self):
        return '2013-07-%.2d' % (self.num + 1)

    def end(self):
        return '2013-07-%.2d' % (self.num + 2)


class TestNestedDict(unittest.TestCase):

    def setUp(self):
        self.nest = self._makeOne()

    def _makeOne(self):
        from ..plan import NestedDict
        return NestedDict

    def test_novalue(self):
        self.assertEquals(getattr(self.nest, 'value', None), None)

    def test_nolock(self):
        self.assertEquals(getattr(self.nest, 'lock', None), None)


class TestPriorityMap(unittest.TestCase):

    def setUp(self):
        self.pmap = self._makeOne()

    def tearDown(self):
        self.pmap.clear()

    def _makeOne(self):
        from ..plan import PriorityMap
        return PriorityMap

    def test_get_value(self):
        self.assertEquals(self.pmap.get_value(), {})

    def test_get(self):
        self.assertEquals(self.pmap.get('foo'), {})

    def test_set(self):
        self.pmap.set('foo', {'bar': 1})
        self.assertEquals(self.pmap.get('foo'), {'bar': 1})

    def test_clear(self):
        self.pmap.set('foo', {'bar': 1})
        self.pmap.clear()
        self.assertEquals(self.pmap.value, {})

    def test_get_entry(self):
        self.assertEquals(self.pmap.get_entry('foo', 'bar'), {})

    def test_set_entry(self):
        self.pmap.set_entry('foo', 'bar', {'baz': 1})
        self.assertEquals(self.pmap.get_entry('foo', 'bar'), {'baz': 1})

    def test_clear_entry(self):
        self.pmap.set('foo', {'bar': 1})
        self.pmap.clear_entry('foo')
        self.assertEquals(self.pmap.get('foo'), {})


class TestPriorityMapDefault(unittest.TestCase):

    def setUp(self):
        self.pmap = self._makeOne()

    def tearDown(self):
        self.pmap.clear()

    def _makeOne(self):
        from ..plan import PriorityMap
        return PriorityMap

    def test_empty(self):
        self.pmap.load_default()
        self.assertEquals(self.pmap.get_value(), {})

    def test_load_failure(self):
        try:
            os.environ['ZCATALOGQUERYPLAN'] = 'Products.ZCatalog.invalid'
            self.pmap.load_default()
            self.assertEquals(self.pmap.get_value(), {})
        finally:
            del os.environ['ZCATALOGQUERYPLAN']

    def test_load(self):
        from ..plan import Benchmark
        try:
            os.environ['ZCATALOGQUERYPLAN'] = \
                'Products.ZCatalog.tests.queryplan.queryplan'
            self.pmap.load_default()
            expected = {'/folder/catalog': {
                'VALUE_INDEXES': frozenset(['index1']),
                'index1 index2': {
                    'index1': Benchmark(duration=2.0, hits=3, limit=True),
                    'index2': Benchmark(duration=1.5, hits=2, limit=False),
                }}}
            self.assertEquals(self.pmap.get_value(), expected)
        finally:
            del os.environ['ZCATALOGQUERYPLAN']

    def test_load_from_path(self):
        from ..plan import Benchmark
        path = os.path.join(os.path.dirname(HERE), 'queryplan.py')
        self.pmap.load_from_path(path)
        expected = {'/folder/catalog': {
            'VALUE_INDEXES': frozenset(['index1']),
            'index1 index2': {
                'index1': Benchmark(duration=2.0, hits=3, limit=True),
                'index2': Benchmark(duration=1.5, hits=2, limit=False),
            }}}
        self.assertEquals(self.pmap.get_value(), expected)


class TestReports(unittest.TestCase):

    def setUp(self):
        self.reports = self._makeOne()

    def tearDown(self):
        self.reports.clear()

    def _makeOne(self):
        from ..plan import Reports
        return Reports

    def test_value(self):
        self.assertEquals(self.reports.value, {})

    def test_lock(self):
        self.assertEquals(type(self.reports.lock), LockType)


class TestCatalogPlan(cleanup.CleanUp, unittest.TestCase):

    def setUp(self):
        cleanup.CleanUp.setUp(self)
        self.cat = Catalog('catalog')

    def _makeOne(self, catalog=None, query=None):
        from ..plan import CatalogPlan
        if catalog is None:
            catalog = self.cat
        return CatalogPlan(catalog, query=query)

    def test_get_id(self):
        plan = self._makeOne()
        self.assertEquals(plan.get_id(), ('', 'NonPersistentCatalog'))

    def test_get_id_persistent(self):
        zcat = ZCatalog('catalog')
        plan = self._makeOne(zcat._catalog)
        self.assertEquals(plan.get_id(), ('catalog', ))

    def test_getCatalogPlan_empty(self):
        zcat = ZCatalog('catalog')
        self._makeOne(zcat._catalog)
        plan_str = zcat.getCatalogPlan()
        self.assertTrue('queryplan = {' in plan_str)

    def test_getCatalogPlan_full(self):
        zcat = ZCatalog('catalog')
        plan = self._makeOne(zcat._catalog, query={'index1': 1, 'index2': 2})
        plan.start()
        plan.start_split('index1')
        time.sleep(0.001)
        plan.stop_split('index1')
        plan.start_split('index2')
        time.sleep(0.001)
        plan.stop_split('index2')
        plan.stop()
        plan_str = zcat.getCatalogPlan()
        self.assertTrue('queryplan = {' in plan_str)
        self.assertTrue('index1' in plan_str)

    def test_plan_empty(self):
        plan = self._makeOne()
        self.assertEquals(plan.plan(), None)

    def test_start(self):
        plan = self._makeOne()
        plan.start()
        self.assert_(plan.start_time <= time.time())

    def test_start_split(self):
        plan = self._makeOne()
        plan.start_split('index1')
        self.assert_('index1' in plan.interim)

    def test_stop_split(self):
        plan = self._makeOne()
        plan.start_split('index1')
        plan.stop_split('index1')
        self.assert_('index1' in plan.interim)
        i1 = plan.interim['index1']
        self.assert_(i1.start <= i1.end)
        self.assert_('index1' in plan.benchmark)

    def test_stop_split_sort_on(self):
        plan = self._makeOne()
        plan.start_split('sort_on')
        plan.stop_split('sort_on')
        self.assert_('sort_on' in plan.interim)
        so = plan.interim['sort_on']
        self.assert_(so.start <= so.end)
        self.assert_('sort_on' not in plan.benchmark)

    def test_stop(self):
        plan = self._makeOne(query={'index1': 1, 'index2': 2})
        plan.start()
        plan.start_split('index1')
        plan.stop_split('index1')
        plan.start_split('index1')
        plan.stop_split('index1')
        plan.start_split('sort_on')
        plan.stop_split('sort_on')
        time.sleep(0.02)  # wait at least one Windows clock tick
        plan.stop()

        self.assert_(plan.duration > 0)
        self.assert_('index1' in plan.benchmark)
        self.assertEquals(plan.benchmark['index1'].hits, 2)
        self.assert_('index2' in plan.benchmark)
        self.assertEquals(plan.benchmark['index2'].hits, 0)
        self.assertEquals(set(plan.plan()), set(('index1', 'index2')))

    def test_log(self):
        plan = self._makeOne(query={'index1': 1})
        plan.threshold = 0.0
        plan.start()
        plan.start_split('index1')
        plan.stop_split('index1')
        plan.stop()
        plan.log()
        report = plan.report()
        self.assertEquals(len(report), 1)
        self.assertEquals(report[0]['counter'], 2)
        plan.reset()
        self.assertEquals(len(plan.report()), 0)

    def test_valueindexes_get(self):
        plan = self._makeOne()
        self.assertEquals(plan.valueindexes(), frozenset())

    def test_valueindexes_set(self):
        from ..plan import PriorityMap
        from ..plan import VALUE_INDEX_KEY
        plan = self._makeOne()
        indexes = frozenset(['index1', 'index2'])
        PriorityMap.set_entry(plan.cid, VALUE_INDEX_KEY, indexes)
        self.assertEquals(plan.valueindexes(), frozenset(indexes))

    # Test the actual logic for determining value indexes
    # Test make_key


class TestValueIndexes(cleanup.CleanUp, unittest.TestCase):

    def _make_catalog(self):
        zcat = ZCatalog('catalog')
        zcat._catalog.addIndex('big', BooleanIndex('big'))
        zcat._catalog.addIndex('date', DateRangeIndex('date', 'start', 'end'))
        zcat._catalog.addIndex('num', FieldIndex('num'))
        zcat._catalog.addIndex('numbers', KeywordIndex('numbers'))
        zcat._catalog.addIndex('path', PathIndex('getPhysicalPath'))
        zcat._catalog.addIndex('uuid', UUIDIndex('num'))
        for i in range(9):
            obj = Dummy(i)
            zcat.catalog_object(obj, str(i))
        return zcat

    def _make_plan(self, catalog):
        from ..plan import CatalogPlan
        return CatalogPlan(catalog)

    def test_uniquevalues(self):
        zcat = self._make_catalog()
        indexes = zcat._catalog.indexes
        self.assertEqual(len(list(indexes['big'].uniqueValues())), 3)
        self.assertEqual(len(list(indexes['date'].uniqueValues())), 0)
        self.assertEqual(len(list(indexes['date'].uniqueValues('start'))), 9)
        self.assertEqual(len(list(indexes['date'].uniqueValues('end'))), 9)
        self.assertEqual(len(list(indexes['num'].uniqueValues())), 9)
        self.assertEqual(len(list(indexes['numbers'].uniqueValues())), 10)
        self.assertEqual(len(list(indexes['path'].uniqueValues())), 9)
        self.assertEqual(len(list(indexes['uuid'].uniqueValues())), 9)

    def test_valueindexes(self):
        zcat = self._make_catalog()
        plan = self._make_plan(zcat._catalog)
        self.assertEqual(plan.valueindexes(),
                         frozenset(['big', 'num', 'path', 'uuid']))


class TestCatalogReport(cleanup.CleanUp, unittest.TestCase):

    def setUp(self):
        cleanup.CleanUp.setUp(self)
        self.zcat = ZCatalog('catalog')
        self.zcat.long_query_time = 0.0
        self._add_indexes()
        for i in range(9):
            obj = Dummy(i)
            self.zcat.catalog_object(obj, str(i))

    def _add_indexes(self):
        num = FieldIndex('num')
        self.zcat._catalog.addIndex('num', num)
        big = FieldIndex('big')
        self.zcat._catalog.addIndex('big', big)
        numbers = KeywordIndex('numbers')
        self.zcat._catalog.addIndex('numbers', numbers)

    def test_ReportLength(self):
        """ tests the report aggregation """
        self.zcat.manage_resetCatalogReport()

        self.zcat.searchResults(numbers=4, sort_on='num')
        self.zcat.searchResults(numbers=1, sort_on='num')
        self.zcat.searchResults(numbers=3, sort_on='num')

        self.zcat.searchResults(big=True, sort_on='num')
        self.zcat.searchResults(big=True, sort_on='num')
        self.zcat.searchResults(big=False, sort_on='num')

        self.zcat.searchResults(num=[5, 4, 3], sort_on='num')
        self.zcat.searchResults(num=(3, 4, 5), sort_on='num')
        self.assertEqual(4, len(self.zcat.getCatalogReport()))

    def test_ReportCounter(self):
        """ tests the counter of equal queries """
        self.zcat.manage_resetCatalogReport()

        self.zcat.searchResults(numbers=5, sort_on='num')
        self.zcat.searchResults(numbers=6, sort_on='num')
        self.zcat.searchResults(numbers=8, sort_on='num')

        r = self.zcat.getCatalogReport()[0]
        self.assertEqual(r['counter'], 3)

    def test_ReportKey(self):
        """ tests the query keys for uniqueness """
        # query key 1
        key = ('sort_on', ('big', 'True'))
        self.zcat.manage_resetCatalogReport()

        self.zcat.searchResults(big=True, sort_on='num')
        self.zcat.searchResults(big=True, sort_on='num')
        r = self.zcat.getCatalogReport()[0]

        self.assertEqual(r['query'], key)
        self.assertEqual(r['counter'], 2)

        # query key 2
        key = ('sort_on', ('big', 'False'))
        self.zcat.manage_resetCatalogReport()

        self.zcat.searchResults(big=False, sort_on='num')
        r = self.zcat.getCatalogReport()[0]

        self.assertEqual(r['query'], key)
        self.assertEqual(r['counter'], 1)

        # query key 3
        key = ('sort_on', ('num', '[3, 4, 5]'))
        self.zcat.manage_resetCatalogReport()

        self.zcat.searchResults(num=[5, 4, 3], sort_on='num')
        self.zcat.searchResults(num=(3, 4, 5), sort_on='num')
        r = self.zcat.getCatalogReport()[0]

        self.assertEqual(r['query'], key)
        self.assertEqual(r['counter'], 2)
