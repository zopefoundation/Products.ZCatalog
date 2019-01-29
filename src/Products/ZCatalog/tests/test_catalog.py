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
""" Unittests for Catalog.
"""

import unittest
from Testing.ZopeTestCase.warnhook import WarningsHook

from itertools import chain
import random

from BTrees.IIBTree import IISet
import ExtensionClass
from Products.PluginIndexes.FieldIndex.FieldIndex import FieldIndex
from Products.PluginIndexes.KeywordIndex.KeywordIndex import KeywordIndex
from Products.ZCTextIndex.OkapiIndex import OkapiIndex
from Products.ZCTextIndex.ZCTextIndex import PLexicon
from Products.ZCTextIndex.ZCTextIndex import ZCTextIndex


def sort(iterable, reverse=False):
    L = list(iterable)
    if reverse:
        L.sort(reverse=True)
    else:
        L.sort()
    return L


class zdummy(ExtensionClass.Base):
    def __init__(self, num):
        self.num = num

    def title(self):
        return '%d' % self.num


class dummy(ExtensionClass.Base):

    att1 = 'att1'
    att2 = 'att2'
    att3 = ['att3']
    foo = 'foo'

    def __init__(self, num):
        self.num = num

    def col1(self):
        return 'col1'

    def col2(self):
        return 'col2'

    def col3(self):
        return ['col3']


class MultiFieldIndex(FieldIndex):

    def getIndexQueryNames(self):
        return [self.id, 'bar']


class objRS(ExtensionClass.Base):

    def __init__(self, num):
        self.number = num


class CatalogBase(object):

    def _makeOne(self):
        from Products.ZCatalog.Catalog import Catalog
        return Catalog()

    def setUp(self):
        self._catalog = self._makeOne()

    def tearDown(self):
        self._catalog = None


class TestAddDelColumn(CatalogBase, unittest.TestCase):

    def testAdd(self):
        self._catalog.addColumn('id')
        self.assertEqual('id' in self._catalog.schema, True,
                         'add column failed')

    def testAddBad(self):
        from Products.ZCatalog.Catalog import CatalogError
        self.assertRaises(CatalogError, self._catalog.addColumn, '_id')

    def testAddWithSpace(self):
        self._catalog.addColumn(' space ')
        self.assertEqual(' space ' not in self._catalog.schema, True,
                         'space not stripped in add column')
        self.assertEqual('space' in self._catalog.schema, True,
                         'stripping space in add column failed')

    def testDel(self):
        self._catalog.addColumn('id')
        self._catalog.delColumn('id')
        self.assert_('id' not in self._catalog.schema,
                     'del column failed')


class TestAddDelIndexes(CatalogBase, unittest.TestCase):

    def testAddFieldIndex(self):
        idx = FieldIndex('id')
        self._catalog.addIndex('id', idx)
        self.assert_(isinstance(self._catalog.indexes['id'],
                                type(FieldIndex('id'))),
                     'add field index failed')

    def testAddTextIndex(self):
        self._catalog.lexicon = PLexicon('lexicon')
        idx = ZCTextIndex('id', caller=self._catalog,
                          index_factory=OkapiIndex, lexicon_id='lexicon')
        self._catalog.addIndex('id', idx)
        i = self._catalog.indexes['id']
        self.assert_(isinstance(i, ZCTextIndex), 'add text index failed')

    def testAddKeywordIndex(self):
        idx = KeywordIndex('id')
        self._catalog.addIndex('id', idx)
        i = self._catalog.indexes['id']
        self.assert_(isinstance(i, type(KeywordIndex('id'))),
                     'add kw index failed')

    def testAddWithSpace(self):
        idx = KeywordIndex(' space ')
        self._catalog.addIndex(' space ', idx)
        self.assertEqual(' space ' not in self._catalog.indexes, True,
                         'space not stripped in add index')
        self.assertEqual('space' in self._catalog.indexes, True,
                         'stripping space in add index failed')
        i = self._catalog.indexes['space']
        # Note: i.id still has spaces in it.
        self.assert_(isinstance(i, KeywordIndex))

    def testDelFieldIndex(self):
        idx = FieldIndex('id')
        self._catalog.addIndex('id', idx)
        self._catalog.delIndex('id')
        self.assert_('id' not in self._catalog.indexes,
                     'del index failed')

    def testDelTextIndex(self):
        self._catalog.lexicon = PLexicon('lexicon')
        idx = ZCTextIndex('id', caller=self._catalog,
                          index_factory=OkapiIndex, lexicon_id='lexicon')
        self._catalog.addIndex('id', idx)
        self._catalog.delIndex('id')
        self.assert_('id' not in self._catalog.indexes,
                     'del index failed')

    def testDelKeywordIndex(self):
        idx = KeywordIndex('id')
        self._catalog.addIndex('id', idx)
        self._catalog.delIndex('id')
        self.assert_('id' not in self._catalog.indexes,
                     'del index failed')


class TestCatalog(CatalogBase, unittest.TestCase):

    upper = 100

    nums = range(upper)
    for i in range(upper):
        j = random.randrange(0, upper)
        tmp = nums[i]
        nums[i] = nums[j]
        nums[j] = tmp

    def setUp(self):
        self._catalog = self._makeOne()
        self._catalog.lexicon = PLexicon('lexicon')
        col1 = FieldIndex('col1')
        col2 = ZCTextIndex('col2', caller=self._catalog,
                          index_factory=OkapiIndex, lexicon_id='lexicon')
        col3 = KeywordIndex('col3')

        self._catalog.addIndex('col1', col1)
        self._catalog.addIndex('col2', col2)
        self._catalog.addIndex('col3', col3)
        self._catalog.addColumn('col1')
        self._catalog.addColumn('col2')
        self._catalog.addColumn('col3')

        att1 = FieldIndex('att1')
        att2 = ZCTextIndex('att2', caller=self._catalog,
                          index_factory=OkapiIndex, lexicon_id='lexicon')
        att3 = KeywordIndex('att3')
        num = FieldIndex('num')
        foo = MultiFieldIndex('foo')

        self._catalog.addIndex('att1', att1)
        self._catalog.addIndex('att2', att2)
        self._catalog.addIndex('att3', att3)
        self._catalog.addIndex('num', num)
        self._catalog.addIndex('foo', foo)
        self._catalog.addColumn('att1')
        self._catalog.addColumn('att2')
        self._catalog.addColumn('att3')
        self._catalog.addColumn('num')

        for x in range(0, self.upper):
            self._catalog.catalogObject(dummy(self.nums[x]), repr(x))
        self._catalog = self._catalog.__of__(dummy('foo'))

    def test_clear(self):
        catalog = self._make_one()
        self.assertTrue(len(catalog) > 0)
        catalog.clear()
        self.assertEqual(catalog._length(), 0)
        self.assertEqual(len(catalog), 0)
        self.assertEqual(len(catalog.data), 0)
        self.assertEqual(len(catalog.paths), 0)
        self.assertEqual(len(catalog.uids), 0)
        for index_id in catalog.indexes:
            index = catalog.getIndex(index_id)
            self.assertEqual(index.numObjects(), 0)

    def test_getitem(self):
        def extra(catalog):
            catalog.addColumn('att1')

        catalog = self._make_one(extra=extra)
        catalog_rids = set(catalog.data)
        brain_class = catalog._v_result_class
        brains = []
        brain_rids = set()
        for rid in catalog_rids:
            brain = catalog[rid]
            brains.append(brain)
            brain_rids.add(brain.getRID())
            self.assertIsInstance(brain, brain_class)
            self.assertEqual(brain.att1, 'att1')

        self.assertEqual(len(brains), len(catalog))
        self.assertEqual(catalog_rids, brain_rids)

    # updateBrains
    # __setstate__
    # useBrains
    # getIndex
    # updateMetadata

    def testCatalogObjectUpdateMetadataFalse(self):
        ob = dummy(9999)
        self._catalog.catalogObject(ob, `9999`)
        brain = self._catalog(num=9999)[0]
        self.assertEqual(brain.att1, 'att1')
        ob.att1 = 'foobar'
        self._catalog.catalogObject(ob, `9999`, update_metadata=0)
        brain = self._catalog(num=9999)[0]
        self.assertEqual(brain.att1, 'att1')
        self._catalog.catalogObject(ob, `9999`)
        brain = self._catalog(num=9999)[0]
        self.assertEqual(brain.att1, 'foobar')

    def uncatalog(self):
        for x in range(0, self.upper):
            self._catalog.uncatalogObject(`x`)

    def testUncatalogFieldIndex(self):
        self.uncatalog()
        a = self._catalog(att1='att1')
        self.assertEqual(len(a), 0, 'len: %s' % len(a))

    def testUncatalogTextIndex(self):
        self.uncatalog()
        a = self._catalog(att2='att2')
        self.assertEqual(len(a), 0, 'len: %s' % len(a))

    def testUncatalogKeywordIndex(self):
        self.uncatalog()
        a = self._catalog(att3='att3')
        self.assertEqual(len(a), 0, 'len: %s' % len(a))

    def testBadUncatalog(self):
        try:
            self._catalog.uncatalogObject('asdasdasd')
        except Exception:
            self.fail('uncatalogObject raised exception on bad uid')

    def testUncatalogTwice(self):
        self._catalog.uncatalogObject(`0`)
        def _second(self):
            self._catalog.uncatalogObject(`0`)
        self.assertRaises(Exception, _second)

    def testCatalogLength(self):
        for x in range(0, self.upper):
            self._catalog.uncatalogObject(`x`)
        self.assertEqual(len(self._catalog), 0)

    def testUniqueValuesForLength(self):
        a = self._catalog.uniqueValuesFor('att1')
        self.assertEqual(len(a), 1, 'bad number of unique values %s' % a)

    def testUniqueValuesForContent(self):
        a = self._catalog.uniqueValuesFor('att1')
        self.assertEqual(a[0], 'att1', 'bad content %s' % a[0])

    # hasuid
    # recordify
    # instantiate
    # getMetadataForRID
    # getIndexDataForRID
    # make_query

    def test_sorted_search_indexes_empty(self):
        result = self._catalog._sorted_search_indexes({})
        self.assertEquals(len(result), 0)

    def test_sorted_search_indexes_one(self):
        result = self._catalog._sorted_search_indexes({'att1': 'a'})
        self.assertEquals(result, ['att1'])

    def test_sorted_search_indexes_many(self):
        query = {'att1': 'a', 'att2': 'b', 'num': 1}
        result = self._catalog._sorted_search_indexes(query)
        self.assertEquals(set(result), set(['att1', 'att2', 'num']))

    def test_sorted_search_indexes_priority(self):
        # att2 and col2 don't support ILimitedResultIndex, att1 does
        query = {'att1': 'a', 'att2': 'b', 'col2': 'c'}
        result = self._catalog._sorted_search_indexes(query)
        self.assertEquals(result.index('att1'), 2)

    def test_sorted_search_indexes_match_alternate_attr(self):
        query = {'bar': 'b'}
        result = self._catalog._sorted_search_indexes(query)
        self.assertEquals(result, ['foo'])

    def test_sorted_search_indexes_no_match(self):
        result = self._catalog._sorted_search_indexes({'baz': 'a'})
        self.assertEquals(result, [])

    # search

    def test_sortResults(self):
        brains = self._catalog({'att1': 'att1'})
        rs = IISet([b.getRID() for b in brains])
        si = self._catalog.getIndex('num')
        result = self._catalog.sortResults(rs, si)
        self.assertEqual([r.num for r in result], range(100))

    def test_sortResults_reversed(self):
        brains = self._catalog({'att1': 'att1'})
        rs = IISet([b.getRID() for b in brains])
        si = self._catalog.getIndex('num')
        result = self._catalog.sortResults(rs, si, reverse=True)
        self.assertEqual([r.num for r in result], list(reversed(range(100))))

    def test_sortResults_limit(self):
        brains = self._catalog({'att1': 'att1'})
        rs = IISet([b.getRID() for b in brains])
        si = self._catalog.getIndex('num')
        result = self._catalog.sortResults(rs, si, limit=10)
        self.assertEqual(len(result), 10)
        self.assertEqual(result.actual_result_count, 100)
        self.assertEqual([r.num for r in result], range(10))

    def test_sortResults_limit_reversed(self):
        brains = self._catalog({'att1': 'att1'})
        rs = IISet([b.getRID() for b in brains])
        si = self._catalog.getIndex('num')
        result = self._catalog.sortResults(rs, si, reverse=True, limit=10)
        self.assertEqual(len(result), 10)
        self.assertEqual(result.actual_result_count, 100)
        expected = list(reversed(range(90, 100)))
        self.assertEqual([r.num for r in result], expected)

    def testLargeSortedResultSetWithSmallIndex(self):
        # This exercises the optimization in the catalog that iterates
        # over the sort index rather than the result set when the result
        # set is much larger than the sort index.
        a = self._catalog(att1='att1', sort_on='att1')
        self.assertEqual(len(a), self.upper)
        self.assertEqual(a.actual_result_count, self.upper)

    def testSortLimit(self):
        full = self._catalog(att1='att1', sort_on='num')
        a = self._catalog(att1='att1', sort_on='num', sort_limit=10)
        self.assertEqual([r.num for r in a], [r.num for r in full[:10]])
        self.assertEqual(a.actual_result_count, self.upper)
        a = self._catalog(att1='att1', sort_on='num',
                          sort_limit=10, sort_order='reverse')
        rev = [r.num for r in full[-10:]]
        rev.reverse()
        self.assertEqual([r.num for r in a], rev)
        self.assertEqual(a.actual_result_count, self.upper)

    def testBigSortLimit(self):
        a = self._catalog(att1='att1', sort_on='num', sort_limit=self.upper*3)
        self.assertEqual(a.actual_result_count, self.upper)
        self.assertEqual(a[0].num, 0)
        a = self._catalog(att1='att1',
            sort_on='num', sort_limit=self.upper*3, sort_order='reverse')
        self.assertEqual(a.actual_result_count, self.upper)
        self.assertEqual(a[0].num, self.upper - 1)

    def testSortLimitViaBatchingArgsBeforeStart(self):
        query = dict(att1='att1', sort_on='num', b_start=-5, b_size=8)
        result = self._catalog(query)
        self.assertEqual(result.actual_result_count, 100)
        self.assertEqual([r.num for r in result], range(0, 3))

    def testSortLimitViaBatchingArgsStart(self):
        query = dict(att1='att1', sort_on='num', b_start=0, b_size=5)
        result = self._catalog(query)
        self.assertEqual(result.actual_result_count, 100)
        self.assertEqual([r.num for r in result], range(0, 5))

    def testSortLimitViaBatchingEarlyFirstHalf(self):
        query = dict(att1='att1', sort_on='num', b_start=11, b_size=17)
        result = self._catalog(query)
        self.assertEqual(result.actual_result_count, 100)
        self.assertEqual([r.num for r in result], range(11, 28))

    def testSortLimitViaBatchingArgsLateFirstHalf(self):
        query = dict(att1='att1', sort_on='num', b_start=30, b_size=15)
        result = self._catalog(query)
        self.assertEqual(result.actual_result_count, 100)
        self.assertEqual([r.num for r in result], range(30, 45))

    def testSortLimitViaBatchingArgsLeftMiddle(self):
        query = dict(att1='att1', sort_on='num', b_start=45, b_size=8)
        result = self._catalog(query)
        self.assertEqual(result.actual_result_count, 100)
        self.assertEqual([r.num for r in result], range(45, 53))

    def testSortLimitViaBatchingArgsRightMiddle(self):
        query = dict(att1='att1', sort_on='num', b_start=48, b_size=8)
        result = self._catalog(query)
        self.assertEqual(result.actual_result_count, 100)
        self.assertEqual([r.num for r in result], range(48, 56))

    def testSortLimitViaBatchingArgsEarlySecondHalf(self):
        query = dict(att1='att1', sort_on='num', b_start=55, b_size=15)
        result = self._catalog(query)
        self.assertEqual(result.actual_result_count, 100)
        self.assertEqual([r.num for r in result], range(55, 70))

    def testSortLimitViaBatchingArgsSecondHalf(self):
        query = dict(att1='att1', sort_on='num', b_start=70, b_size=15)
        result = self._catalog(query)
        self.assertEqual(result.actual_result_count, 100)
        self.assertEqual([r.num for r in result], range(70, 85))

    def testSortLimitViaBatchingArgsEnd(self):
        query = dict(att1='att1', sort_on='num', b_start=90, b_size=10)
        result = self._catalog(query)
        self.assertEqual(result.actual_result_count, 100)
        self.assertEqual([r.num for r in result], range(90, 100))

    def testSortLimitViaBatchingArgsOverEnd(self):
        query = dict(att1='att1', sort_on='num', b_start=90, b_size=15)
        result = self._catalog(query)
        self.assertEqual(result.actual_result_count, 100)
        self.assertEqual([r.num for r in result], range(90, 100))

    def testSortLimitViaBatchingArgsOutside(self):
        query = dict(att1='att1', sort_on='num', b_start=110, b_size=10)
        result = self._catalog(query)
        self.assertEqual(result.actual_result_count, 100)
        self.assertEqual([r.num for r in result], [])

    def testSortedResultLengthWithMissingDocs(self):
        # remove the `0` document from the num index only
        num_index = self._catalog.getIndex('num')
        pos_of_zero = self.nums.index(0)
        uid = self._catalog.uids.get(repr(pos_of_zero))
        self.assertEqual(self._catalog[uid].num, 0)
        num_index.unindex_object(uid)
        # make sure it was removed
        self.assertEqual(len(num_index), 99)
        # sort over the smaller num index
        query = dict(att1='att1', sort_on='num', sort_limit=10)
        result = self._catalog(query)
        # the `0` document was removed
        self.assertEqual(result[0].num, 1)
        self.assertEqual(len(result), 10)
        # there are only 99 documents left
        self.assertEqual(result.actual_result_count, 99)

    # _get_sort_attr
    # _getSortIndex
    # searchResults

    def testResultLength(self):
        a = self._catalog(att1='att1')
        self.assertEqual(len(a), self.upper,
                         'length should be %s, its %s' % (self.upper, len(a)))

    def testMappingWithEmptyKeysDoesntReturnAll(self):
        # Queries with empty keys used to return all, because of a bug in the
        # parseIndexRequest function, mistaking a CatalogSearchArgumentsMap
        # for a Record class
        a = self._catalog({'col1': '', 'col2': '', 'col3': ''})
        self.assertEqual(len(a), 0, 'length should be 0, its %s' % len(a))

    def testFieldIndexLength(self):
        a = self._catalog(att1='att1')
        self.assertEqual(len(a), self.upper,
                         'should be %s, but is %s' % (self.upper, len(a)))

    def testTextIndexLength(self):
        a = self._catalog(att2='att2')
        self.assertEqual(len(a), self.upper,
                         'should be %s, but is %s' % (self.upper, len(a)))

    def testKeywordIndexLength(self):
        a = self._catalog(att3='att3')
        self.assertEqual(len(a), self.upper,
                         'should be %s, but is %s' % (self.upper, len(a)))

    def testGoodSortIndex(self):
        upper = self.upper
        a = self._catalog(att1='att1', sort_on='num')
        self.assertEqual(len(a), upper,
                         'length should be %s, its %s' % (upper, len(a)))
        for x in range(self.upper):
            self.assertEqual(a[x].num, x)

    def testBadSortIndex(self):
        from Products.ZCatalog.Catalog import CatalogError
        def badsortindex():
            self._catalog(sort_on='foofaraw')
        self.assertRaises(CatalogError, badsortindex)

    def testWrongKindOfIndexForSort(self):
        from Products.ZCatalog.Catalog import CatalogError
        def wrongsortindex():
            self._catalog(sort_on='att2')
        self.assertRaises(CatalogError, wrongsortindex)

    def testTextIndexQWithSortOn(self):
        upper = self.upper
        a = self._catalog(sort_on='num', att2='att2')
        self.assertEqual(len(a), upper,
                         'length should be %s, its %s' % (upper, len(a)))
        for x in range(self.upper):
            self.assertEqual(a[x].num, x)

    def testTextIndexQWithoutSortOn(self):
        upper = self.upper
        a = self._catalog(att2='att2')
        self.assertEqual(len(a), upper,
                         'length should be %s, its %s' % (upper, len(a)))

    def testKeywordIndexWithMinRange(self):
        a = self._catalog(att3={'query': 'att', 'range': 'min'})
        self.assertEqual(len(a), self.upper)

    def testKeywordIndexWithMaxRange(self):
        a = self._catalog(att3={'query': 'att35', 'range': ':max'})
        self.assertEqual(len(a), self.upper)

    def testKeywordIndexWithMinMaxRangeCorrectSyntax(self):
        a = self._catalog(att3={'query': ['att', 'att35'], 'range': 'min:max'})
        self.assertEqual(len(a), self.upper)

    def testKeywordIndexWithMinMaxRangeWrongSyntax(self):
        # checkKeywordIndex with min/max range wrong syntax.
        a = self._catalog(att3={'query': ['att'], 'range': 'min:max'})
        self.assert_(len(a) != self.upper)

    def testCombinedTextandKeywordQuery(self):
        a = self._catalog(att3='att3', att2='att2')
        self.assertEqual(len(a), self.upper)


class TestRangeSearch(CatalogBase, unittest.TestCase):

    def setUp(self):
        self._catalog = self._makeOne()
        index = FieldIndex('number')
        self._catalog.addIndex('number', index)
        self._catalog.addColumn('number')

        for i in range(5000):
            obj = objRS(random.randrange(0, 20000))
            self._catalog.catalogObject(obj, i)

        self._catalog = self._catalog.__of__(objRS(200))

    def testRangeSearch(self):
        for i in range(1000):
            m = random.randrange(0, 20000)
            n = m + 1000

            for r in self._catalog.searchResults(
                number={'query': (m, n), 'range': 'min:max'}):

                size = r.number
                self.assert_(m<=size and size<=n,
                             "%d vs [%d,%d]" % (r.number, m, n))


class TestCatalogReturnAll(CatalogBase, unittest.TestCase):

    def setUp(self):
        self.warningshook = WarningsHook()
        self.warningshook.install()
        self._catalog = self._makeOne()

    def testEmptyMappingReturnsAll(self):
        col1 = FieldIndex('col1')
        self._catalog.addIndex('col1', col1)
        for x in range(0, 10):
            self._catalog.catalogObject(dummy(x), repr(x))
        self.assertEqual(len(self._catalog), 10)
        length = len(self._catalog({}))
        self.assertEqual(length, 10)

    def tearDown(self):
        CatalogBase.tearDown(self)
        self.warningshook.uninstall()


class TestCatalogSearchArgumentsMap(unittest.TestCase):

    def _makeOne(self, request=None, keywords=None):
        from Products.ZCatalog.Catalog import CatalogSearchArgumentsMap
        return CatalogSearchArgumentsMap(request, keywords)

    def test_init_empty(self):
        argmap = self._makeOne()
        self.assert_(argmap)

    def test_init_request(self):
        argmap = self._makeOne(dict(foo='bar'), None)
        self.assertEquals(argmap.get('foo'), 'bar')

    def test_init_keywords(self):
        argmap = self._makeOne(None, dict(foo='bar'))
        self.assertEquals(argmap.get('foo'), 'bar')

    def test_getitem(self):
        argmap = self._makeOne(dict(a='a'), dict(b='b'))
        self.assertEquals(argmap['a'], 'a')
        self.assertEquals(argmap['b'], 'b')
        self.assertRaises(KeyError, argmap.__getitem__, 'c')

    def test_getitem_emptystring(self):
        argmap = self._makeOne(dict(a='', c='c'), dict(b='', c=''))
        self.assertRaises(KeyError, argmap.__getitem__, 'a')
        self.assertRaises(KeyError, argmap.__getitem__, 'b')
        self.assertEquals(argmap['c'], 'c')

    def test_get(self):
        argmap = self._makeOne(dict(a='a'), dict(b='b'))
        self.assertEquals(argmap.get('a'), 'a')
        self.assertEquals(argmap.get('b'), 'b')
        self.assertEquals(argmap.get('c'), None)
        self.assertEquals(argmap.get('c', 'default'), 'default')

    def test_keywords_precedence(self):
        argmap = self._makeOne(dict(a='a', c='r'), dict(b='b', c='k'))
        self.assertEquals(argmap.get('c'), 'k')
        self.assertEquals(argmap['c'], 'k')

    def test_haskey(self):
        argmap = self._makeOne(dict(a='a'), dict(b='b'))
        self.assert_(argmap.has_key('a'))
        self.assert_(argmap.has_key('b'))
        self.assert_(not argmap.has_key('c'))

    def test_contains(self):
        argmap = self._makeOne(dict(a='a'), dict(b='b'))
        self.assert_('a' in argmap)
        self.assert_('b' in argmap)
        self.assert_('c' not in argmap)


class TestMergeResults(CatalogBase, unittest.TestCase):

    def setUp(self):
        self.catalogs = []
        for i in range(3):
            cat = self._makeOne()
            cat.lexicon = PLexicon('lexicon')
            cat.addIndex('num', FieldIndex('num'))
            cat.addIndex('big', FieldIndex('big'))
            cat.addIndex('number', FieldIndex('number'))
            i = ZCTextIndex('title', caller=cat, index_factory=OkapiIndex,
                            lexicon_id='lexicon')
            cat.addIndex('title', i)
            cat = cat.__of__(zdummy(16336))
            for i in range(10):
                obj = zdummy(i)
                obj.big = i > 5
                obj.number = True
                cat.catalogObject(obj, str(i))
            self.catalogs.append(cat)

    def testNoFilterOrSort(self):
        from Products.ZCatalog.Catalog import mergeResults
        results = [cat.searchResults(
                   dict(number=True), _merge=0) for cat in self.catalogs]
        merged_rids = [r.getRID() for r in mergeResults(
            results, has_sort_keys=False, reverse=False)]
        expected = [r.getRID() for r in chain(*results)]
        self.assertEqual(sort(merged_rids), sort(expected))

    def testSortedOnly(self):
        from Products.ZCatalog.Catalog import mergeResults
        results = [cat.searchResults(
                   dict(number=True, sort_on='num'), _merge=0)
                   for cat in self.catalogs]
        merged_rids = [r.getRID() for r in mergeResults(
            results, has_sort_keys=True, reverse=False)]
        expected = sort(chain(*results))
        expected = [rid for sortkey, rid, getitem in expected]
        self.assertEqual(merged_rids, expected)

    def testSortReverse(self):
        from Products.ZCatalog.Catalog import mergeResults
        results = [cat.searchResults(
                   dict(number=True, sort_on='num'), _merge=0)
                   for cat in self.catalogs]
        merged_rids = [r.getRID() for r in mergeResults(
            results, has_sort_keys=True, reverse=True)]
        expected = sort(chain(*results), reverse=True)
        expected = [rid for sortkey, rid, getitem in expected]
        self.assertEqual(merged_rids, expected)

    def testLimitSort(self):
        from Products.ZCatalog.Catalog import mergeResults
        results = [cat.searchResults(
                   dict(att1='att1', number=True, sort_on='num',
                   sort_limit=2), _merge=0)
                   for cat in self.catalogs]
        merged_rids = [r.getRID() for r in mergeResults(
            results, has_sort_keys=True, reverse=False)]
        expected = sort(chain(*results))
        expected = [rid for sortkey, rid, getitem in expected]
        self.assertEqual(merged_rids, expected)

    def testScored(self):
        from Products.ZCatalog.Catalog import mergeResults
        results = [cat.searchResults(title='4 or 5 or 6', _merge=0)
                   for cat in self.catalogs]
        merged_rids = [r.getRID() for r in mergeResults(
            results, has_sort_keys=True, reverse=False)]
        expected = sort(chain(*results))
        expected = [rid for sortkey, (nscore, score, rid), getitem in expected]
        self.assertEqual(merged_rids, expected)

    def testSmallIndexSort(self):
        # Test that small index sort optimization is not used for merging
        from Products.ZCatalog.Catalog import mergeResults
        results = [cat.searchResults(
                   dict(number=True, sort_on='big'), _merge=0)
                   for cat in self.catalogs]
        merged_rids = [r.getRID() for r in mergeResults(
            results, has_sort_keys=True, reverse=False)]
        expected = sort(chain(*results))
        expected = [rid for sortkey, rid, getitem in expected]
        self.assertEqual(merged_rids, expected)


class TestScoring(CatalogBase, unittest.TestCase):

    def _get_catalog(self):
        return self._catalog.__of__(zdummy(16336))

    def setUp(self):
        self._catalog = self._makeOne()
        self._catalog.lexicon = PLexicon('lexicon')
        idx = ZCTextIndex('title', caller=self._catalog,
                          index_factory=OkapiIndex, lexicon_id='lexicon')
        self._catalog.addIndex('title', idx)
        self._catalog.addIndex('true', FieldIndex('true'))
        self._catalog.addColumn('title')
        cat = self._get_catalog()
        for i in (1, 2, 3, 10, 11, 110, 111):
            obj = zdummy(i)
            obj.true = True
            if i == 110:
                obj.true = False
            cat.catalogObject(obj, str(i))

    def test_simple_search(self):
        cat = self._get_catalog()
        brains = cat(title='10')
        self.assertEqual(len(brains), 1)
        self.assertEqual(brains[0].title, '10')

    def test_or_search(self):
        cat = self._get_catalog()
        brains = cat(title='2 OR 3')
        self.assertEqual(len(brains), 2)

    def test_scored_search(self):
        cat = self._get_catalog()
        brains = cat(title='1*')
        self.assertEqual(len(brains), 5)
        self.assertEqual(brains[0].title, '111')

    def test_combined_scored_search(self):
        cat = self._get_catalog()
        brains = cat(title='1*', true=True)
        self.assertEqual(len(brains), 4)
        self.assertEqual(brains[0].title, '111')

    def test_combined_scored_search_planned(self):
        from ..plan import Benchmark
        from ..plan import PriorityMap
        cat = self._get_catalog()
        query = dict(title='1*', true=True)
        plan = cat.getCatalogPlan()
        plan_key = plan.make_key(query)
        catalog_id = plan.get_id()
        # plan with title first
        PriorityMap.set_entry(catalog_id, plan_key, dict(
            title=Benchmark(1, 1, False),
            true=Benchmark(2, 1, False),
            ))
        brains = cat(query)
        self.assertEqual(len(brains), 4)
        self.assertEqual(brains[0].title, '111')
        # plan with true first
        PriorityMap.set_entry(catalog_id, plan_key, dict(
            title=Benchmark(2, 1, False),
            true=Benchmark(1, 1, False),
            ))
        brains = cat(query)
        self.assertEqual(len(brains), 4)
        self.assertEqual(brains[0].title, '111')


def test_suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(TestAddDelColumn))
    suite.addTest(unittest.makeSuite(TestAddDelIndexes))
    suite.addTest(unittest.makeSuite(TestCatalog))
    suite.addTest(unittest.makeSuite(TestRangeSearch))
    suite.addTest(unittest.makeSuite(TestCatalogReturnAll))
    suite.addTest(unittest.makeSuite(TestCatalogSearchArgumentsMap))
    suite.addTest(unittest.makeSuite(TestMergeResults))
    suite.addTest(unittest.makeSuite(TestScoring))
    return suite
