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

from zope.testing import cleanup

from Products.PluginIndexes.BooleanIndex.BooleanIndex import BooleanIndex
from Products.PluginIndexes.DateRangeIndex.DateRangeIndex import DateRangeIndex
from Products.PluginIndexes.DateIndex.DateIndex import DateIndex
from Products.PluginIndexes.FieldIndex.FieldIndex import FieldIndex
from Products.PluginIndexes.KeywordIndex.KeywordIndex import KeywordIndex
from Products.PluginIndexes.PathIndex.PathIndex import PathIndex
from Products.PluginIndexes.UUIDIndex.UUIDIndex import UUIDIndex
from Products.ZCatalog.Catalog import Catalog
from Products.ZCatalog.ZCatalog import ZCatalog
from Products.ZCatalog.cache import CatalogCacheKey
from Products.ZCatalog.cache import _get_cache


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


class TestCatalogCacheKey(unittest.TestCase):

    def setUp(self):
        self.cat = Catalog('catalog')

    def _makeOne(self, catalog=None, query=None):
        if catalog is None:
            catalog = self.cat
        return CatalogCacheKey(catalog, query=query)

    def test_get_id(self):
        cache_key = self._makeOne()
        self.assertEquals(cache_key.get_id(),
                          ('', 'NonPersistentCatalog'))

    def test_get_id_persistent(self):
        zcat = ZCatalog('catalog')
        cache_key = self._makeOne(zcat._catalog)
        self.assertEquals(cache_key.get_id(), ('catalog', ))


class TestCatalogQueryKey(cleanup.CleanUp, unittest.TestCase):

    def setUp(self):
        cleanup.CleanUp.setUp(self)
        zcat = ZCatalog('catalog')
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

        self.zcat = zcat

    def _apply_query(self, query):
        cache = _get_cache()

        res1 = self.zcat.search(query)
        stats = cache.getStatistics()

        hits = stats[0]['hits']
        misses = stats[0]['misses']

        res2 = self.zcat.search(query)
        stats = cache.getStatistics()

        # check if chache hits
        self.assertEqual(stats[0]['hits'], hits + 1)
        self.assertEqual(stats[0]['misses'], misses)

        # compare result
        rset1 = list(map(lambda x: x.getRID(), res1))
        rset2 = list(map(lambda x: x.getRID(), res2))
        self.assertEqual(rset1, rset2)

    def _get_cache_key(self, query=None):
        catalog = self.zcat._catalog
        query = catalog.make_query(query)
        return CatalogCacheKey(catalog, query=query).key

    def test_make_key(self):
        query = {'big': True}
        expect = (('catalog',),
                  frozenset([('big', (True,),
                              (self.length,
                               b'\x00\x00\x00\x00\x00\x00\x00\x00'))])
                  )

        self.assertEquals(self._get_cache_key(query), expect)

        query = {'start': '2013-07-01'}
        expect = (('catalog',),
                  frozenset([('start', ('2013-07-01',),
                              (self.length,
                               b'\x00\x00\x00\x00\x00\x00\x00\x00'))])

                  )
        
        self.assertEquals(self._get_cache_key(query), expect)

        query = {'path': '/1', 'date': '2013-07-05', 'numbers': [1, 3]}
        expect = (('catalog',),
                  frozenset([('date', ('2013-07-05',),
                              (self.length,
                               b'\x00\x00\x00\x00\x00\x00\x00\x00')),
                             ('numbers', (1, 3),
                              (self.length,
                               b'\x00\x00\x00\x00\x00\x00\x00\x00')),
                             ('path', ('/1',),
                              (self.length,
                               b'\x00\x00\x00\x00\x00\x00\x00\x00'))]))
        
        self.assertEquals(self._get_cache_key(query), expect)

        queries = [{'big': True, 'b_start': 0},
                   {'big': True, 'b_start': 0, 'b_size': 5},
                   {'big': True, 'sort_on': 'big'},
                   {'big': True, 'sort_on': 'big', 'sort_limit': 3},
                   {'big': True, 'sort_on': 'big', 'sort_order': 'descending'},
                   ]
        expect = (('catalog',),
                  frozenset([('big', (True,),
                              (self.length,
                               b'\x00\x00\x00\x00\x00\x00\x00\x00'))]))

        for query in queries:
            self.assertEquals(self._get_cache_key(query), expect)

    def test_cache(self):
        query = {'big': True}
        self._apply_query(query)

        query = {'start': '2013-07-01'}
        self._apply_query(query)

        query = {'path': '/1', 'date': '2013-07-05', 'numbers': [1, 3]}
        self._apply_query(query)

    def test_cache_invalidate(self):
        cache = _get_cache()
        query = {'big': False}

        res1 = self.zcat.search(query)
        stats = cache.getStatistics()

        hits = stats[0]['hits']
        misses = stats[0]['misses']

        # catalog new object
        obj = Dummy(20)
        self.zcat.catalog_object(obj, str(20))

        res2 = self.zcat.search(query)
        stats = cache.getStatistics()

        # check if chache misses
        self.assertEqual(stats[0]['hits'], hits)
        self.assertEqual(stats[0]['misses'], misses + 1)

        # compare result
        rset1 = list(map(lambda x: x.getRID(), res1))
        rset2 = list(map(lambda x: x.getRID(), res2))
        self.assertEqual(rset1, rset2)
