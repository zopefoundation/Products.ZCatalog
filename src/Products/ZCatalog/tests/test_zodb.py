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

import random
import sys
import unittest

import ExtensionClass
from OFS.Folder import Folder
from Testing.makerequest import makerequest
import transaction
import Zope2

from Products.PluginIndexes.FieldIndex.FieldIndex import FieldIndex
from Products.ZCatalog.ZCatalog import ZCatalog


class ZDummy(ExtensionClass.Base):
    meta_type = 'dummy'

    def __init__(self, num):
        self.id = 'dummy_%d' % (num,)
        self.title = 'Dummy %d' % (num,)


class ContentLayer(object):

    @classmethod
    def setUp(cls):
        cls.app = makerequest(Zope2.app())
        cls.app._setObject('Catalog', ZCatalog('Catalog'))
        cls.app.Catalog.addIndex('meta_type', FieldIndex('meta_type'))
        cls.app.Catalog.addColumn('id')
        cls.app.Catalog.addColumn('title')
        cls.app._setObject('Database', Folder('Database'))
        # newly added objects have ._p_jar == None, initialize it
        transaction.savepoint()
        cache_size = cls.app._p_jar.db().getCacheSize()
        cls._fill_catalog(cls.app, cls.app.Catalog, cache_size * 3)
        # attach new persistent objects to ZODB connection
        transaction.savepoint()

    @classmethod
    def tearDown(cls):
        for obj_id in ('Catalog', 'Database'):
            cls.app._delObject(obj_id, suppress_events=True)

    @staticmethod
    def _make_dummy():
        num = random.randint(0, sys.maxsize)
        return ZDummy(num)

    @staticmethod
    def _make_persistent_folder(app, obj_id):
        app.Database._setObject(obj_id, Folder(obj_id))
        result = app.Database[obj_id]
        result.title = 'Folder %s' % (obj_id,)
        return result

    @classmethod
    def _fill_catalog(cls, app, catalog, num_objects, times_more=10):
        # Catalog num_objects of "interesting" documents and intersperse
        # them with (num_objects * times_more) of dummy objects,
        # making sure that "interesting" objects do not share
        # the same metadata bucket (as it happens in typical use).
        def catalog_dummies(num_dummies):
            for j in range(num_dummies):
                obj = cls._make_dummy()
                catalog.catalog_object(obj, uid=obj.id)
        for i in range(num_objects):
            # catalog average of TIMES_MORE / 2 dummy objects
            catalog_dummies(random.randint(1, times_more))
            # catalog normal object
            obj_id = 'folder_%i' % (i,)
            catalog.catalog_object(cls._make_persistent_folder(app, obj_id))
            # catalog another TIMES_MORE / 2 dummy objects
            catalog_dummies(random.randint(1, times_more))


class TestPersistentZCatalog(unittest.TestCase):

    layer = ContentLayer

    def _get_zodb_info(self, obj):
        conn = obj._p_jar
        cache_size_limit = conn.db().getCacheSize()
        return (conn, cache_size_limit)

    def _actual_cache_size(self, obj):
        return obj._p_jar._cache.cache_non_ghost_count

    def _test_catalog_search(self, threshold=None):
        catalog = self.layer.app.Catalog
        conn, ignore = self._get_zodb_info(catalog)
        conn.cacheMinimize()
        # run large query and read its results
        catalog.threshold = threshold
        aggregate = 0
        for record in catalog(meta_type='Folder'):
            aggregate += len(record.title)
        return catalog

    def test_unmaintained_search(self):
        # run large query without cache maintenance
        catalog = self._test_catalog_search(threshold=None)
        ignore, cache_size_limit = self._get_zodb_info(catalog)
        # ZODB connection cache grows out of size limit and eats memory
        actual_size = self._actual_cache_size(catalog)
        self.assertTrue(actual_size >= cache_size_limit * 1.1)

    def test_maintained_search(self):
        # run big query with cache maintenance
        threshold = 50
        catalog = self._test_catalog_search(threshold=threshold)
        ignore, cache_size_limit = self._get_zodb_info(catalog)
        # ZODB connection cache stays within its size limit
        actual_size = self._actual_cache_size(catalog)
        self.assertTrue(actual_size <= cache_size_limit + threshold)
