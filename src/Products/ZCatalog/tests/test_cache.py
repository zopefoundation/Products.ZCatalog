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

import unittest
import transaction

from ZODB import DB
from ZODB.POSException import ConflictError
from zope.testing import cleanup
from ZODB.tests.test_storage import MinimalMemoryStorage
from ZODB.utils import newTid

from Products.PluginIndexes.BooleanIndex.BooleanIndex import BooleanIndex
from Products.PluginIndexes.DateRangeIndex.DateRangeIndex import DateRangeIndex
from Products.PluginIndexes.DateIndex.DateIndex import DateIndex
from Products.PluginIndexes.FieldIndex.FieldIndex import FieldIndex
from Products.PluginIndexes.KeywordIndex.KeywordIndex import KeywordIndex
from Products.PluginIndexes.PathIndex.PathIndex import PathIndex
from Products.PluginIndexes.UUIDIndex.UUIDIndex import UUIDIndex
from Products.ZCatalog.Catalog import Catalog
from Products.ZCatalog.ZCatalog import ZCatalog
from Products.ZCatalog.cache import CatalogQueryKey
from Products.ZCatalog.cache import _get_cache


class DummyMVCCAdapter(object):
    def __init__(self):

        class DummyStorage(object):
            def __init__(self):
                self._start = newTid(None)

        self._storage = DummyStorage()


class Dummy(object):

    def __init__(self, num):
        self.id = str(num)
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


class TestCatalogQueryKey(unittest.TestCase):

    def setUp(self):
        self.cat = Catalog('catalog')

    def _makeOne(self, catalog=None, query=None):
        if catalog is None:
            catalog = self.cat
            catalog._p_jar = DummyMVCCAdapter()

        return CatalogQueryKey(catalog, query=query)

    def test_get_id(self):
        cache_key = self._makeOne()
        self.assertEquals(cache_key.get_id(),
                          ('', 'NonPersistentCatalog'))

    def test_get_id_persistent(self):
        zcat = ZCatalog('catalog')
        cache_key = self._makeOne(zcat._catalog)
        self.assertEquals(cache_key.get_id(), ('catalog', ))


class TestCatalogQueryCaching(cleanup.CleanUp, unittest.TestCase):

    def setUp(self):
        cleanup.CleanUp.setUp(self)

        st = MinimalMemoryStorage()
        db = DB(st)
        cache = _get_cache()
        cache.invalidateAll()

        # init catalog
        cn = db.open()
        r = cn.root()

        r['zcat'] = self._build_zcatalog()
        self.zcat = r['zcat']
        transaction.get().commit()

    def _build_zcatalog(self):

        zcat = ZCatalog('catalog')
        zcat.addColumn('id')
        zcat._catalog.addIndex('big', BooleanIndex('big'))
        zcat._catalog.addIndex('start', DateIndex('start'))
        zcat._catalog.addIndex('date', DateRangeIndex('date', 'start', 'end'))
        zcat._catalog.addIndex('num', FieldIndex('num'))
        zcat._catalog.addIndex('numbers', KeywordIndex('numbers'))
        zcat._catalog.addIndex('path', PathIndex('getPhysicalPath'))
        zcat._catalog.addIndex('uuid', UUIDIndex('num'))
        self.length = length = 9
        for i in range(length):
            obj = Dummy(i)
            zcat.catalog_object(obj, str(i))

        return zcat

    def _apply_query(self, query):
        cache = _get_cache()

        transaction.get().commit()

        # 1st search MISS
        res1 = self.zcat.search(query)
        transaction.get().commit()

        stats = cache.getStatistics()
        
        hits = stats[0]['hits']
        misses = stats[0]['misses']

        # 2nd search HIT
        res2 = self.zcat.search(query)
        transaction.get().commit()

        stats = cache.getStatistics()
        
        # check if chache hits
        self.assertEqual(stats[0]['hits'], hits + 2)
        self.assertEqual(stats[0]['misses'], misses)

        # compare result
        rset1 = list(map(lambda x: x.getRID(), res1))
        rset2 = list(map(lambda x: x.getRID(), res2))
        self.assertEqual(rset1, rset2)

    def _get_cache_key(self, zcatalog, query):
        catalog = zcatalog._catalog
        q = catalog.make_query(query)
        return CatalogQueryKey(catalog, query=q).key

    def test_make_key(self):
        query = {'big': True}
        expect = (('catalog',),
                  frozenset([('big', (True,), self.length)]))
        qkey = self._get_cache_key(self.zcat, query)
        self.assertEqual(qkey, expect)

        query = {'start': '2013-07-01'}
        expect = (('catalog',),
                  frozenset([('start', ('2013-07-01',),
                              self.length)]))
        qkey = self._get_cache_key(self.zcat, query)
        self.assertEqual(qkey, expect)

        query = {'path': '/1', 'date': '2013-07-05', 'numbers': [1, 3]}
        expect = (('catalog',),
                  frozenset([('date', ('2013-07-05',), self.length),
                             ('numbers', (1, 3), self.length),
                             ('path', ('/1',), self.length)]))
        qkey = self._get_cache_key(self.zcat, query)
        self.assertEqual(qkey, expect)

        queries = [{'big': True, 'b_start': 0},
                   {'big': True, 'b_start': 0, 'b_size': 5},
                   {'big': True, 'sort_on': 'big'},
                   {'big': True, 'sort_on': 'big', 'sort_limit': 3},
                   {'big': True, 'sort_on': 'big', 'sort_order': 'descending'},
                   ]
        expect = (('catalog',),
                  frozenset([('big', (True,), self.length)]))

        for query in queries:
            qkey = self._get_cache_key(self.zcat, query)
            self.assertEqual(qkey, expect)

    def test_cache(self):

        query = {'big': True}
        self._apply_query(query)

        query = {'start': '2013-07-01'}
        self._apply_query(query)

        query = {'path': '/1', 'date': '2013-07-05', 'numbers': [1, 3]}
        self._apply_query(query)

    def test_cache_invalidate(self):
        cache = _get_cache()
        cache.invalidateAll()
        transaction.get().commit()

        query = {'big': False}

        res1 = self.zcat.search(query)
        transaction.get().commit()

        stats = cache.getStatistics()
 
        hits = stats[0]['hits']
        misses = stats[0]['misses']

        # catalog new object
        obj = Dummy(20)
        self.zcat.catalog_object(obj, str(20))

        res2 = self.zcat.search(query)
        transaction.get().commit()

        stats = cache.getStatistics()
 
        # check if cache misses (__getitem__ + commit)
        self.assertEqual(stats[0]['hits'], hits)
        self.assertEqual(stats[0]['misses'], misses + 2)

        # compare result
        rset1 = list(map(lambda x: x.getRID(), res1))
        rset2 = list(map(lambda x: x.getRID(), res2))
        self.assertEqual(rset1, rset2)

    def test_cache_mvcc_concurrent_writes(self):
        st = MinimalMemoryStorage()
        db = DB(st)

        query = {'big': True}
        cache = _get_cache()
        cache.invalidateAll()

        # 1st thread
        tm1 = transaction.TransactionManager()
        cn1 = db.open(transaction_manager=tm1)

        # init catalog
        r1 = cn1.root()
        r1['zcat'] = self._build_zcatalog()
        obj = Dummy(20)

        r1['zcat'].catalog_object(obj, obj.id)
        tm1.get().commit()
        cn1.sync()

        # 2nd thread
        tm2 = transaction.TransactionManager()
        cn2 = db.open(transaction_manager=tm2)

        r1 = cn1.root()
        # catalog new object
        obj = Dummy(21)
        r1['zcat'].catalog_object(obj, obj.id)

        # dont cache if catalog has changed
        tm1.get().commit()
        stats = cache.getStatistics()
        self.assertEqual(stats, tuple())

        r2 = cn2.root()
        # catalog new object
        obj = Dummy(22)
        r2['zcat'].catalog_object(obj, obj.id)

        res2 = r2['zcat'].search(query)
        indexed_ids = {rec.id for rec in res2}
        self.assertTrue(obj.id in indexed_ids)

        # raise conflict error because catalog was changed in tm1
        self.assertRaises(ConflictError, tm2.get().commit)

        tm2.get().abort()

        # try it again
        r2 = cn2.root()
        obj = Dummy(22)
        r2['zcat'].catalog_object(obj, obj.id)

        res2 = r2['zcat'].search(query)
        indexed_ids = {rec.id for rec in res2}
        self.assertTrue(obj.id in indexed_ids)

        tm2.get().commit()

        cn1.sync()
        # query without changing catalog
        r1 = cn1.root()
        qkey = self._get_cache_key(r1['zcat'], query)

        # query key is not None, results will be cached
        expect = ((('catalog',), frozenset(
            {('big', (True,), 12)})))
        self.assertEqual(qkey, expect)

        # cache store

        res1 = r1['zcat'].search(query)
        transaction.get().commit()
        
        r2 = cn2.root()
        # cache hit

        res2 = r2['zcat'].search(query)
        transaction.get().commit()
        
        # compare result
        rset1 = list(map(lambda x: x.getRID(), res1))
        rset2 = list(map(lambda x: x.getRID(), res2))
        self.assertEqual(rset1, rset2)
        cache = _get_cache()

        stats = cache.getStatistics()

        hits = stats[0]['hits']
        misses = stats[0]['misses']
        self.assertEqual((hits, misses), (2, 4))
