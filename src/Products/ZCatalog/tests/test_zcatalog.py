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

import ExtensionClass
from AccessControl import Unauthorized
from AccessControl.SecurityManagement import noSecurityManager
from AccessControl.SecurityManagement import setSecurityManager
from Acquisition import Implicit
from OFS.Folder import Folder as OFS_Folder
from Testing.makerequest import makerequest


class Folder(OFS_Folder):

    def __init__(self, id):
        self._setId(id)
        super(OFS_Folder, self).__init__()


class ZDummy(ExtensionClass.Base):

    def __init__(self, num):
        self.num = num

    def title(self):
        return f'{self.num:d}'


class ZDummyFalse(ZDummy):

    def __nonzero__(self):
        return False


class DummyLenFail(ZDummy):

    def __init__(self, num, fail):
        super().__init__(num)
        self.fail = fail

    def __len__(self):
        self.fail('__len__() was called')


class DummyNonzeroFail(ZDummy):

    def __init__(self, num, fail):
        super().__init__(num)
        self.fail = fail

    def __nonzero__(self):
        self.fail('__nonzero__() was called')


class FakeTraversalError(KeyError):
    """fake traversal exception for testing"""


class FakeParent(Implicit):
    # fake parent mapping unrestrictedTraverse to
    # catalog.resolve_path as simulated by TestZCatalog

    marker = object()

    def __init__(self, d):
        super().__init__()
        self.d = d

    def unrestrictedTraverse(self, path, default=marker):
        result = self.d.get(path, default)
        if result is self.marker:
            raise FakeTraversalError(path)
        return result


class PickySecurityManager:

    def __init__(self, badnames=[]):
        self.badnames = badnames

    def validateValue(self, value):
        return 1

    def validate(self, accessed, container, name, value):
        if name not in self.badnames:
            return 1
        raise Unauthorized(name)


class ZCatalogBase:

    def _makeOne(self):
        from Products.ZCatalog.ZCatalog import ZCatalog
        return ZCatalog('Catalog')

    def _makeOneIndex(self, name):
        from Products.PluginIndexes.FieldIndex.FieldIndex import FieldIndex
        return FieldIndex(name)

    def setUp(self):
        self._catalog = self._makeOne()

    def tearDown(self):
        self._catalog = None


class TestZCatalog(ZCatalogBase, unittest.TestCase):

    def setUp(self):
        ZCatalogBase.setUp(self)

        self._catalog.resolve_path = self._resolve_num
        from Products.PluginIndexes.KeywordIndex.KeywordIndex import \
            KeywordIndex
        title = KeywordIndex('title')
        self._catalog.addIndex('title', title)
        self._catalog.addColumn('title')

        self.upper = 10
        self.d = {}
        for x in range(0, self.upper):
            # make uid a string of the number
            ob = ZDummy(x)
            self.d[str(x)] = ob
            self._catalog.catalog_object(ob, str(x))

    def _resolve_num(self, num):
        return self.d[num]

    def test_interfaces(self):
        from zope.interface.verify import verifyClass

        from Products.ZCatalog.interfaces import IZCatalog
        from Products.ZCatalog.ZCatalog import ZCatalog

        verifyClass(IZCatalog, ZCatalog)

    def test_len(self):
        self.assertEqual(len(self._catalog), self.upper)

    # manage_edit
    # manage_subbingToggle

    def testBooleanEvalOn_manage_catalogObject(self):
        self.d['11'] = DummyLenFail(11, self.fail)
        self.d['12'] = DummyNonzeroFail(12, self.fail)

        class MyResponse:
            # A fake response that doesn't bomb on manage_catalogObject().
            def redirect(self, url):
                pass

        # this next call should not fail
        self._catalog.manage_catalogObject(None, MyResponse(),
                                           'URL1', urls=('11', '12'))

    # manage_uncatalogObject
    # manage_catalogReindex

    def testBooleanEvalOn_refreshCatalog_getobject(self):
        # wrap catalog under the fake parent providing unrestrictedTraverse()
        catalog = self._catalog.__of__(FakeParent(self.d))
        # replace entries to test refreshCatalog
        self.d['0'] = DummyLenFail(0, self.fail)
        self.d['1'] = DummyNonzeroFail(1, self.fail)
        # this next call should not fail
        catalog.refreshCatalog()

        for uid in ('0', '1'):
            rid = catalog.getrid(uid)
            # neither should these
            catalog.getobject(rid)

    # manage_catalogClear
    # manage_catalogFoundItems
    # manage_addColumn
    # manage_delColumn
    # manage_addIndex
    # manage_delIndex
    # manage_clearIndex

    def testReindexIndexDoesntDoMetadata(self):
        self.d['0'].num = 9999
        self._catalog.reindexIndex('title', {})
        data = self._catalog.getMetadataForUID('0')
        self.assertEqual(data['title'], '0')

    def testReindexIndexesFalse(self):
        # setup
        false_id = self.upper + 1
        ob = ZDummyFalse(false_id)
        self.d[str(false_id)] = ob
        self._catalog.catalog_object(ob, str(false_id))
        # test, object evaluates to false; there was bug which caused the
        # object to be removed from index
        ob.num = 9999
        self._catalog.reindexIndex('title', {})
        result = self._catalog(title='9999')
        self.assertEqual(1, len(result))

    # manage_reindexIndex
    # catalog_object
    # uncatalog_object
    # uniqueValuesFor
    # getpath
    # getrid

    def test_getobject_traversal(self):
        # getobject doesn't mask TraversalErrors and doesn't delegate to
        # resolve_url
        # wrap catalog under the fake parent providing unrestrictedTraverse()
        catalog = self._catalog.__of__(FakeParent(self.d))

        def resolve_url(path, REQUEST):
            # make resolve_url fail if ZCatalog falls back on it
            self.fail('.resolve_url() should not be called by .getobject()')

        catalog.resolve_url = resolve_url

        # traversal should work at first
        rid0 = catalog.getrid('0')
        # lets set it up so the traversal fails
        del self.d['0']
        self.assertRaises(FakeTraversalError,
                          catalog.getobject, rid0, REQUEST=object())
        # and if there is a None at the traversal point, that's where it
        # should return
        self.d['0'] = None
        self.assertEqual(catalog.getobject(rid0), None)

    def testGetMetadataForUID(self):
        testNum = str(self.upper - 3)  # as good as any..
        data = self._catalog.getMetadataForUID(testNum)
        self.assertEqual(data['title'], testNum)

    def testGetIndexDataForUID(self):
        testNum = str(self.upper - 3)
        data = self._catalog.getIndexDataForUID(testNum)
        self.assertEqual(data['title'][0], testNum)

    def testUpdateMetadata(self):
        self._catalog.catalog_object(ZDummy(1), '1')
        data = self._catalog.getMetadataForUID('1')
        self.assertEqual(data['title'], '1')
        self._catalog.catalog_object(ZDummy(2), '1', update_metadata=0)
        data = self._catalog.getMetadataForUID('1')
        self.assertEqual(data['title'], '1')
        self._catalog.catalog_object(ZDummy(2), '1', update_metadata=1)
        data = self._catalog.getMetadataForUID('1')
        self.assertEqual(data['title'], '2')
        # update_metadata defaults to true, test that here
        self._catalog.catalog_object(ZDummy(1), '1')
        data = self._catalog.getMetadataForUID('1')
        self.assertEqual(data['title'], '1')

    # getMetadataForRID
    # getIndexDataForRID

    def testGetAllBrains(self):
        brain_class = self._catalog._catalog._v_result_class
        brains = []
        for brain in self._catalog.getAllBrains():
            brains.append(brain)
            self.assertIsInstance(brain, brain_class)
            self.assertTrue(hasattr(brain, 'title'))
        self.assertEqual(len(brains), len(self._catalog))

    def testSearchAll(self):
        all = self._catalog.searchAll()
        for b in all:
            self.assertTrue(hasattr(b, 'title'))
        self.assertEqual(len(all), len(self._catalog))

    # schema
    # indexes
    # index_objects
    # getIndexObjects
    # _searchable_arguments
    # _searchable_result_columns

    def testSearchResults(self):
        query = {'title': ['5', '6', '7']}
        sr = self._catalog.searchResults(query)
        self.assertEqual(len(sr), 3)

    def testCall(self):
        query = {'title': ['5', '6', '7']}
        sr = self._catalog(query)
        self.assertEqual(len(sr), 3)

    def testSearch(self):
        query = {'title': ['5', '6', '7']}
        sr = self._catalog.search(query)
        self.assertEqual(len(sr), 3)

    def testSearchNot(self):
        query = {'title': {'not': ['0']}}
        sr = self._catalog.search(query)
        self.assertEqual(len(sr), 9)

    # resolve_url
    # resolve_path
    # manage_setProgress
    # _getProgressThreshold

    def test_catalogView(self):
        catalog = makerequest(self._catalog)
        # hack `getPhysicalPath` to avoid problem with the catalog plan
        catalog.getPhysicalPath = None
        # provide `ZopeVersion`
        catalog.ZopeVersion = lambda *arg, **kw: 4
        vr = catalog.manage_catalogView()
        self.assertTrue("There are no objects in the Catalog." not in vr,
                        "catalogView wrongly reports `no objects`")


class TestAddDelColumnIndex(ZCatalogBase, unittest.TestCase):

    def testAddIndex(self):
        self._catalog.addIndex('id', self._makeOneIndex('id'))
        self.assertIn('id', self._catalog.indexes())

    def testDelIndex(self):
        self._catalog.addIndex('title', self._makeOneIndex('title'))
        self.assertTrue('title', self._catalog.indexes())
        self._catalog.delIndex('title')
        self.assertNotIn('title', self._catalog.indexes())

    def testClearIndex(self):
        self._catalog.addIndex('title', self._makeOneIndex('title'))
        idx = self._catalog._catalog.getIndex('title')
        for x in range(10):
            ob = ZDummy(x)
            self._catalog.catalog_object(ob, str(x))
        self.assertEqual(len(idx), 10)
        self._catalog.clearIndex('title')
        self.assertEqual(len(idx), 0)

    def testAddColumn(self):
        self._catalog.addColumn('num', default_value=0)
        self.assertIn('num', self._catalog.schema())

    def testDelColumn(self):
        self._catalog.addColumn('title')
        self._catalog.delColumn('title')
        self.assertNotIn('title', self._catalog.schema())


class TestZCatalogGetObject(ZCatalogBase, unittest.TestCase):
    # Test what objects are returned by brain.getObject()

    def setUp(self):
        ZCatalogBase.setUp(self)
        self._catalog.addIndex('id', self._makeOneIndex('id'))
        root = Folder('')
        root.getPhysicalRoot = lambda: root
        self.root = root
        self.root.catalog = self._catalog

    def tearDown(self):
        ZCatalogBase.tearDown(self)
        noSecurityManager()

    def test_getObject_found(self):
        # Check normal traversal
        root = self.root
        catalog = root.catalog
        root.ob = Folder('ob')
        catalog.catalog_object(root.ob)
        brain = catalog.searchResults({'id': 'ob'})[0]
        self.assertEqual(brain.getPath(), '/ob')
        self.assertEqual(brain.getObject().getId(), 'ob')

    def test_getObject_missing_raises_NotFound(self):
        # Check that if the object is missing we raise
        from zExceptions import NotFound
        root = self.root
        catalog = root.catalog
        root.ob = Folder('ob')
        catalog.catalog_object(root.ob)
        brain = catalog.searchResults({'id': 'ob'})[0]
        del root.ob
        self.assertRaises((NotFound, AttributeError, KeyError),
                          brain.getObject)

    def test_getObject_restricted_raises_Unauthorized(self):
        # Check that if the object's security does not allow traversal,
        # None is returned
        root = self.root
        catalog = root.catalog
        root.fold = Folder('fold')
        root.fold.ob = Folder('ob')
        catalog.catalog_object(root.fold.ob)
        brain = catalog.searchResults({'id': 'ob'})[0]
        # allow all accesses
        pickySecurityManager = PickySecurityManager()
        setSecurityManager(pickySecurityManager)
        self.assertEqual(brain.getObject().getId(), 'ob')
        # disallow just 'ob' access
        pickySecurityManager = PickySecurityManager(['ob'])
        setSecurityManager(pickySecurityManager)
        self.assertRaises(Unauthorized, brain.getObject)
        # disallow just 'fold' access
        pickySecurityManager = PickySecurityManager(['fold'])
        setSecurityManager(pickySecurityManager)
        ob = brain.getObject()
        self.assertFalse(ob is None)
        self.assertEqual(ob.getId(), 'ob')

    # Now test _unrestrictedGetObject

    def test_unrestrictedGetObject_found(self):
        # Check normal traversal
        root = self.root
        catalog = root.catalog
        root.ob = Folder('ob')
        catalog.catalog_object(root.ob)
        brain = catalog.searchResults({'id': 'ob'})[0]
        self.assertEqual(brain.getPath(), '/ob')
        self.assertEqual(brain._unrestrictedGetObject().getId(), 'ob')

    def test_unrestrictedGetObject_restricted(self):
        # Check that if the object's security does not allow traversal,
        # it's still is returned
        root = self.root
        catalog = root.catalog
        root.fold = Folder('fold')
        root.fold.ob = Folder('ob')
        catalog.catalog_object(root.fold.ob)
        brain = catalog.searchResults({'id': 'ob'})[0]
        # allow all accesses
        pickySecurityManager = PickySecurityManager()
        setSecurityManager(pickySecurityManager)
        self.assertEqual(brain._unrestrictedGetObject().getId(), 'ob')
        # disallow just 'ob' access
        pickySecurityManager = PickySecurityManager(['ob'])
        setSecurityManager(pickySecurityManager)
        self.assertEqual(brain._unrestrictedGetObject().getId(), 'ob')
        # disallow just 'fold' access
        pickySecurityManager = PickySecurityManager(['fold'])
        setSecurityManager(pickySecurityManager)
        self.assertEqual(brain._unrestrictedGetObject().getId(), 'ob')

    def test_unrestrictedGetObject_missing_raises_NotFound(self):
        # Check that if the object is missing we raise
        from zExceptions import NotFound
        root = self.root
        catalog = root.catalog
        root.ob = Folder('ob')
        catalog.catalog_object(root.ob)
        brain = catalog.searchResults({'id': 'ob'})[0]
        del root.ob
        self.assertRaises((NotFound, AttributeError, KeyError),
                          brain._unrestrictedGetObject)
