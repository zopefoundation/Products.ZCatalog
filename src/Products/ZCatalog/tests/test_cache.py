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

    def _get_cache_key(self, query=None):
        catalog = self.zcat._catalog
        return CatalogCacheKey(catalog, query=query).key

    def test_make_key(self):
        query = {'big': True}
        key = (('catalog',), frozenset([('big', self.length, (True,))]))
        self.assertEquals(self._get_cache_key(query), key)

#class TestCatalogCaching(unittest.TestCase):
#
#      def test_caching
