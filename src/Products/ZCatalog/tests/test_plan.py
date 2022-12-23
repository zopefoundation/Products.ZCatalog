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
import time
import unittest
from _thread import LockType

from zope.testing import cleanup

from Products.PluginIndexes.BooleanIndex.BooleanIndex import BooleanIndex
from Products.PluginIndexes.DateRangeIndex.DateRangeIndex import DateRangeIndex
from Products.PluginIndexes.FieldIndex.FieldIndex import FieldIndex
from Products.PluginIndexes.KeywordIndex.KeywordIndex import KeywordIndex
from Products.PluginIndexes.PathIndex.PathIndex import PathIndex
from Products.PluginIndexes.UUIDIndex.UUIDIndex import UUIDIndex
from Products.ZCatalog.Catalog import Catalog
from Products.ZCatalog.plan import MAX_DISTINCT_VALUES
from Products.ZCatalog.ZCatalog import ZCatalog


HERE = __file__


class Dummy:

    def __init__(self, num):
        self.num = num

    def big(self):
        return self.num > 5

    def numbers(self):
        return (self.num, self.num + 1)

    def getPhysicalPath(self):
        return f'/{self.num}'

    def start(self):
        return f'2013-07-{self.num + 1:02d}'

    def end(self):
        return f'2013-07-{self.num + 2:02d}'


class TestNestedDict(unittest.TestCase):

    def setUp(self):
        self.nest = self._makeOne()

    def _makeOne(self):
        from ..plan import NestedDict
        return NestedDict

    def test_novalue(self):
        self.assertEqual(getattr(self.nest, 'value', None), None)

    def test_nolock(self):
        self.assertEqual(getattr(self.nest, 'lock', None), None)


class TestPriorityMap(unittest.TestCase):

    def setUp(self):
        self.pmap = self._makeOne()

    def tearDown(self):
        self.pmap.clear()

    def _makeOne(self):
        from ..plan import PriorityMap
        return PriorityMap

    def test_get_value(self):
        self.assertEqual(self.pmap.get_value(), {})

    def test_get(self):
        self.assertEqual(self.pmap.get('foo'), {})

    def test_set(self):
        self.pmap.set('foo', {'bar': 1})
        self.assertEqual(self.pmap.get('foo'), {'bar': 1})

    def test_clear(self):
        self.pmap.set('foo', {'bar': 1})
        self.pmap.clear()
        self.assertEqual(self.pmap.value, {})

    def test_get_entry(self):
        self.assertEqual(self.pmap.get_entry('foo', 'bar'), {})

    def test_set_entry(self):
        self.pmap.set_entry('foo', 'bar', {'baz': 1})
        self.assertEqual(self.pmap.get_entry('foo', 'bar'), {'baz': 1})

    def test_clear_entry(self):
        self.pmap.set('foo', {'bar': 1})
        self.pmap.clear_entry('foo')
        self.assertEqual(self.pmap.get('foo'), {})


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
        self.assertEqual(self.pmap.get_value(), {})

    def test_load_failure(self):
        try:
            os.environ['ZCATALOGQUERYPLAN'] = 'Products.ZCatalog.invalid'
            self.pmap.load_default()
            self.assertEqual(self.pmap.get_value(), {})
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
            self.assertEqual(self.pmap.get_value(), expected)
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
        self.assertEqual(self.pmap.get_value(), expected)


class TestReports(unittest.TestCase):

    def setUp(self):
        self.reports = self._makeOne()

    def tearDown(self):
        self.reports.clear()

    def _makeOne(self):
        from ..plan import Reports
        return Reports

    def test_value(self):
        self.assertEqual(self.reports.value, {})

    def test_lock(self):
        self.assertEqual(type(self.reports.lock), LockType)


class TestCatalogPlan(cleanup.CleanUp, unittest.TestCase):

    def assertRegex(self, *args, **kwargs):
        return super().assertRegex(*args, **kwargs)

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
        self.assertEqual(plan.get_id(), ('', 'NonPersistentCatalog'))

    def test_get_id_persistent(self):
        zcat = ZCatalog('catalog')
        plan = self._makeOne(zcat._catalog)
        self.assertEqual(plan.get_id(), ('catalog', ))

    def test_getCatalogPlan_empty(self):
        zcat = ZCatalog('catalog')
        self._makeOne(zcat._catalog)
        plan_str = zcat.getCatalogPlan()
        self.assertIn('queryplan = {', plan_str)

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
        self.assertIn('queryplan = {', plan_str)
        self.assertIn('index1', plan_str)

    def test_getCatalogPlan_partial(self):
        zcat = ZCatalog("catalog")
        cat = zcat._catalog

        class SlowFieldIndex(FieldIndex):
            def query_index(self, record, resultset=None):
                time.sleep(0.05)
                return super().query_index(
                    record, resultset)

        class SlowerDateRangeIndex(DateRangeIndex):
            def query_index(self, record, resultset=None):
                time.sleep(0.4)
                return super().query_index(
                    record, resultset)

        cat.addIndex("num", SlowFieldIndex("num"))
        cat.addIndex("numbers", KeywordIndex("numbers"))
        cat.addIndex("path", PathIndex("getPhysicalPath"))
        cat.addIndex("date", SlowerDateRangeIndex("date", "start", "end"))

        for i in range(MAX_DISTINCT_VALUES * 2):
            obj = Dummy(i)
            zcat.catalog_object(obj, str(i))

        query1 = {"num": 2, "numbers": 3, "date": "2013-07-03"}
        # query with no result, because of `numbers`
        query2 = {"num": 2, "numbers": -1, "date": "2013-07-03"}

        # without a plan index are orderd alphabetically by default
        self.assertEqual(zcat._catalog.getCatalogPlan(query1).plan(), None)
        self.assertEqual(
            cat._sorted_search_indexes(query1),
            ["date", "num", "numbers"]
        )

        self.assertEqual([b.getPath() for b in zcat.search(query1)], ["2"])
        self.assertRegex(
            zcat.getCatalogPlan(),
            r"(?ms).*'date':\s*\([0-9\.]+, [0-9\.]+, True\)"
        )
        self.assertRegex(
            zcat.getCatalogPlan(),
            r"(?ms).*'num':\s*\([0-9\.]+, [0-9\.]+, True\)"
        )
        self.assertRegex(
            zcat.getCatalogPlan(),
            r"(?ms).*'numbers':\s*\([0-9\.]+, [0-9\.]+, True\)"
        )

        # after first search field are orderd by speed
        self.assertEqual(
            cat.getCatalogPlan(query2).plan(), ["numbers", "num", "date"])

        self.assertEqual([b.getPath() for b in zcat.search(query2)], [])

        # `date', `num`, and `numbers` are all involved to filter the
        #  results(limit flag) despite in the last query search whitin
        #  `num` and `date` wasn't done
        self.assertRegex(
            zcat.getCatalogPlan(),
            r"(?ms).*'date':\s*\([0-9\.]+, [0-9\.]+, True\)"
        )
        self.assertRegex(
            zcat.getCatalogPlan(),
            r"(?ms).*'num':\s*\([0-9\.]+, [0-9\.]+, True\)"
        )
        self.assertRegex(
            zcat.getCatalogPlan(),
            r"(?ms).*'numbers':\s*\([0-9\.]+, [0-9\.]+, True\)"
        )
        self.assertEqual(
            cat.getCatalogPlan(query2).plan(), ["numbers", "num", "date"]
        )

        # search again doesn't change the index order
        self.assertEqual([b.getPath() for b in zcat.search(query1)], ["2"])
        self.assertEqual(
            cat.getCatalogPlan(query2).plan(), ["numbers", "num", "date"]
        )

    def test_not_query(self):
        # not query is generally slower, force this behavior for testing
        class SlowNotFieldIndex(FieldIndex):
            def query_index(self, record, resultset=None):
                if getattr(record, 'not', None):
                    time.sleep(0.1)
                return super().query_index(
                    record, resultset)

        zcat = ZCatalog("catalog")
        cat = zcat._catalog
        cat.addIndex(
            'num1', SlowNotFieldIndex('num1', extra={"indexed_attrs": "num"}))
        cat.addIndex(
            'num2', SlowNotFieldIndex('num2', extra={"indexed_attrs": "num"}))
        for i in range(100):
            obj = Dummy(i)
            zcat.catalog_object(obj, str(i))

        query1 = {"num1": {"not": 2}, "num2": 3}
        query2 = {"num1": 2, "num2": {'not': 5}}

        # without a plan index are orderd alphabetically by default
        for query in [query1, query2]:
            self.assertEqual(zcat._catalog.getCatalogPlan(query).plan(), None)
            self.assertEqual(
                cat._sorted_search_indexes(query),
                ["num1", "num2"]
            )

        self.assertEqual([b.getPath() for b in zcat.search(query1)], ['3'])
        self.assertEqual([b.getPath() for b in zcat.search(query2)], ['2'])
        # although there are the same fields, the plans are different, and the
        # slower `not` query put the field as second in the plan
        self.assertEqual(cat.getCatalogPlan(query1).plan(), ["num2", "num1"])
        self.assertEqual(cat.getCatalogPlan(query2).plan(), ["num1", "num2"])

        # search again doesn't change the order
        self.assertEqual([b.getPath() for b in zcat.search(query1)], ['3'])
        self.assertEqual([b.getPath() for b in zcat.search(query2)], ['2'])
        self.assertEqual(cat.getCatalogPlan(query1).plan(), ["num2", "num1"])
        self.assertEqual(cat.getCatalogPlan(query2).plan(), ["num1", "num2"])

    def test_plan_empty(self):
        plan = self._makeOne()
        self.assertEqual(plan.plan(), None)

    def test_start(self):
        plan = self._makeOne()
        plan.start()
        self.assertLessEqual(plan.start_time, time.time())

    def test_start_split(self):
        plan = self._makeOne()
        plan.start_split('index1')
        self.assertIn('index1', plan.interim)

    def test_stop_split(self):
        plan = self._makeOne()
        plan.start_split('index1')
        plan.stop_split('index1')
        self.assertIn('index1', plan.interim)
        i1 = plan.interim['index1']
        self.assertLessEqual(i1.start, i1.end)
        self.assertIn('index1', plan.benchmark)

    def test_stop_split_sort_on(self):
        plan = self._makeOne()
        plan.start_split('sort_on')
        plan.stop_split('sort_on')
        self.assertIn('sort_on', plan.interim)
        so = plan.interim['sort_on']
        self.assertLessEqual(so.start, so.end)
        self.assertNotIn('sort_on', plan.benchmark)

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

        self.assertGreater(plan.duration, 0)
        self.assertIn('index1', plan.benchmark)
        self.assertEqual(plan.benchmark['index1'].hits, 2)
        self.assertIn('index2', plan.benchmark)
        self.assertEqual(plan.benchmark['index2'].hits, 0)
        self.assertEqual(set(plan.plan()), {'index1', 'index2'})

    def test_log(self):
        plan = self._makeOne(query={'index1': 1})
        plan.threshold = 0.0
        plan.start()
        plan.start_split('index1')
        plan.stop_split('index1')
        plan.stop()
        plan.log()
        report = plan.report()
        self.assertEqual(len(report), 1)
        self.assertEqual(report[0]['counter'], 2)
        plan.reset()
        self.assertEqual(len(plan.report()), 0)

    def test_valueindexes_get(self):
        plan = self._makeOne()
        self.assertEqual(plan.valueindexes(), frozenset())

    def test_valueindexes_set(self):
        from ..plan import VALUE_INDEX_KEY
        from ..plan import PriorityMap
        plan = self._makeOne()
        indexes = frozenset(['index1', 'index2'])
        PriorityMap.set_entry(plan.cid, VALUE_INDEX_KEY, indexes)
        self.assertEqual(plan.valueindexes(), frozenset(indexes))

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
        self.assertEqual(len(list(indexes['big'].uniqueValues())), 2)
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
