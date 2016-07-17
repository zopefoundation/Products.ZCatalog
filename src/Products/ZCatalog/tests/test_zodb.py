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
""" Unittests for ZCatalog interaction with ZODB persistency
"""

import ExtensionClass
from OFS.Folder import Folder
from Products.PluginIndexes.FieldIndex.FieldIndex import FieldIndex
from Products.ZCatalog.ZCatalog import ZCatalog
import random
from Testing.makerequest import makerequest
import sys
import transaction
import unittest
import Zope2


class zdummy(ExtensionClass.Base):
    meta_type = 'dummy'

    def __init__(self, num):
        self.id = 'dummy_%d' % (num,)
        self.title = 'Dummy %d' % (num,)


class TestPersistentZCatalog(unittest.TestCase):

    def setUp(self):
        self.app = makerequest(Zope2.app())
        self.app._setObject('Catalog', ZCatalog('Catalog'))
        self.app.Catalog.addIndex('meta_type', FieldIndex('meta_type'))
        self.app.Catalog.addColumn('id')
        self.app.Catalog.addColumn('title')
        self.app._setObject('Database', Folder('Database'))
        # newly added objects have ._p_jar == None, initialize it
        transaction.savepoint()

    def tearDown(self):
        for obj_id in ('Catalog', 'Database'):
            self.app._delObject(obj_id, suppress_events=True)

    def _make_dummy(self):
        num = random.randint(0, sys.maxint)
        return zdummy(num)

    def _make_persistent_folder(self, obj_id):
        self.app.Database._setObject(obj_id, Folder(obj_id))
        result = self.app.Database[obj_id]
        result.title = 'Folder %s' % (obj_id,)
        return result

    def _get_zodb_info(self, obj):
        conn = obj._p_jar
        cache_size_limit = conn.db().getCacheSize()
        return conn, cache_size_limit

    def _actual_cache_size(self, obj):
        return obj._p_jar._cache.cache_non_ghost_count

    NUM_RESULTS = 1500
    TIMES_MORE = 10

    def _fill_catalog(self, catalog, num_objects):
        # catalog num_objects of "interesting" documents
        # and intersperse them with (num_objects * TIMES_MORE) of dummy objects,
        # making sure that "interesting" objects do not share
        # the same metadata bucket (as it happens in typical use)
        def catalog_dummies(num_dummies):
            for j in range(num_dummies):
                obj = self._make_dummy()
                catalog.catalog_object(obj, uid=obj.id)
        for i in range(num_objects):
            # catalog average of TIMES_MORE / 2 dummy objects
            catalog_dummies(random.randint(1, self.TIMES_MORE))
            # catalog normal object
            obj_id = 'folder_%i' % (i,)
            catalog.catalog_object(self._make_persistent_folder(obj_id))
            # catalog another TIMES_MORE / 2 dummy objects
            catalog_dummies(random.randint(1, self.TIMES_MORE))
        # attach new persistent objects to ZODB connection
        transaction.savepoint()

    def _test_catalog_search(self, threshold=None):
        catalog = self.app.Catalog
        self._fill_catalog(catalog, self.NUM_RESULTS)
        conn, ignore = self._get_zodb_info(catalog)
        conn.cacheGC()
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
        self.assertTrue(actual_size > cache_size_limit * 2)

    def test_maintained_search(self):
        # run big query with cache maintenance
        threshold = 128
        catalog = self._test_catalog_search(threshold=threshold)
        ignore, cache_size_limit = self._get_zodb_info(catalog)
        # ZODB connection cache stays within its size limit
        actual_size = self._actual_cache_size(catalog)
        self.assertTrue(actual_size <= cache_size_limit + threshold)
