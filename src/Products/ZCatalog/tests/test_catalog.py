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

from itertools import chain
import random

from BTrees.IIBTree import IISet
import ExtensionClass
from Products.PluginIndexes.FieldIndex.FieldIndex import FieldIndex
from Products.PluginIndexes.KeywordIndex.KeywordIndex import KeywordIndex
from Products.ZCTextIndex.OkapiIndex import OkapiIndex
from Products.ZCTextIndex.ZCTextIndex import PLexicon
from Products.ZCTextIndex.ZCTextIndex import ZCTextIndex


class ZDummy(ExtensionClass.Base):
    def __init__(self, num):
        self.num = num

    def title(self):
        return '%d' % self.num


class Dummy(ExtensionClass.Base):

    att1 = 'att1'
    att2 = 'att2'
    att3 = ['att3']
    foo = 'foo'

    def __init__(self, num):
        self.num = num
        if isinstance(num, int) and (self.num % 10) == 0:
            self.ends_in_zero = True

    def col1(self):
        return 'col1'

    def col2(self):
        return 'col2'

    def col3(self):
        return ['col3']


class MultiFieldIndex(FieldIndex):

    def getIndexQueryNames(self):
        return [self.id, 'bar']


class ObjRS(ExtensionClass.Base):

    def __init__(self, num):
        self.number = num


class TestAddDelColumn(unittest.TestCase):

    def _make_one(self):
        from Products.ZCatalog.Catalog import Catalog
        return Catalog()

    def test_add(self):
        catalog = self._make_one()
        catalog.addColumn('id')
        self.assertEqual('id' in catalog.schema, True, 'add column failed')

    def test_add_bad(self):
        from Products.ZCatalog.Catalog import CatalogError
        catalog = self._make_one()
        self.assertRaises(CatalogError, catalog.addColumn, '_id')

    def test_add_with_space(self):
        catalog = self._make_one()
        catalog.addColumn(' space ')
        self.assertEqual(' space ' not in catalog.schema, True,
                         'space not stripped in add column')
        self.assertEqual('space' in catalog.schema, True,
                         'stripping space in add column failed')

    def test_add_brains(self):
        catalog = self._make_one()
        catalog.addColumn('col1')
        catalog.addColumn('col3')
        for i in range(3):
            catalog.catalogObject(Dummy(3), repr(i))
        self.assertTrue('col2' not in catalog.data.values()[0])
        catalog.addColumn('col2', default_value='new')
        self.assert_('col2' in catalog.schema, 'add column failed')
        self.assertTrue('new' in catalog.data.values()[0])

    def test_add_threshold(self):
        catalog = self._make_one()
        catalog.addColumn('col1')
        for i in range(3):
            catalog.catalogObject(Dummy(3), repr(i))
        catalog.addColumn('col2', threshold=None)

    def test_del(self):
        catalog = self._make_one()
        catalog.addColumn('id')
        catalog.delColumn('id')
        self.assert_('id' not in catalog.schema, 'del column failed')

    def test_del_brains(self):
        catalog = self._make_one()
        catalog.addColumn('col1')
        catalog.addColumn('col2')
        catalog.addColumn('col3')
        for i in range(3):
            catalog.catalogObject(Dummy(3), repr(i))
        self.assertTrue('col2' in catalog.data.values()[0])
        catalog.delColumn('col2')
        self.assert_('col2' not in catalog.schema, 'del column failed')
        self.assertTrue('col2' not in catalog.data.values()[0])

    def test_del_threshold(self):
        catalog = self._make_one()
        catalog.addColumn('col1')
        for i in range(3):
            catalog.catalogObject(Dummy(3), repr(i))
        catalog.delColumn('col1', threshold=None)


class TestAddDelIndexes(unittest.TestCase):

    def _make_one(self):
        from Products.ZCatalog.Catalog import Catalog
        return Catalog()

    def test_add_field_index(self):
        catalog = self._make_one()
        idx = FieldIndex('id')
        catalog.addIndex('id', idx)
        self.assert_(isinstance(catalog.indexes['id'], FieldIndex))

    def test_add_text_index(self):
        catalog = self._make_one()
        catalog.lexicon = PLexicon('lexicon')
        idx = ZCTextIndex('id', caller=catalog,
                          index_factory=OkapiIndex, lexicon_id='lexicon')
        catalog.addIndex('id', idx)
        i = catalog.indexes['id']
        self.assert_(isinstance(i, ZCTextIndex))

    def test_add_keyword_index(self):
        catalog = self._make_one()
        idx = KeywordIndex('id')
        catalog.addIndex('id', idx)
        i = catalog.indexes['id']
        self.assert_(isinstance(i, KeywordIndex))

    def test_add_with_space(self):
        catalog = self._make_one()
        idx = KeywordIndex(' space ')
        catalog.addIndex(' space ', idx)
        self.assertEqual(' space ' not in catalog.indexes, True,
                         'space not stripped in add index')
        self.assertEqual('space' in catalog.indexes, True,
                         'stripping space in add index failed')
        i = catalog.indexes['space']
        # Note: i.id still has spaces in it.
        self.assert_(isinstance(i, KeywordIndex))

    def test_del_field_index(self):
        catalog = self._make_one()
        idx = FieldIndex('id')
        catalog.addIndex('id', idx)
        catalog.delIndex('id')
        self.assert_('id' not in catalog.indexes)

    def test_del_text_index(self):
        catalog = self._make_one()
        catalog.lexicon = PLexicon('lexicon')
        idx = ZCTextIndex('id', caller=catalog,
                          index_factory=OkapiIndex, lexicon_id='lexicon')
        catalog.addIndex('id', idx)
        catalog.delIndex('id')
        self.assert_('id' not in catalog.indexes)

    def test_del_keyword_index(self):
        catalog = self._make_one()
        idx = KeywordIndex('id')
        catalog.addIndex('id', idx)
        catalog.delIndex('id')
        self.assert_('id' not in catalog.indexes)


class TestCatalog(unittest.TestCase):

    upper = 10

    nums = list(range(upper))
    for i in range(upper):
        j = random.randrange(0, upper)
        tmp = nums[i]
        nums[i] = nums[j]
        nums[j] = tmp

    def _make_one(self, extra=None):
        from Products.ZCatalog.Catalog import Catalog
        catalog = Catalog()
        catalog.lexicon = PLexicon('lexicon')

        att1 = FieldIndex('att1')
        catalog.addIndex('att1', att1)

        att2 = ZCTextIndex('att2', caller=catalog,
                           index_factory=OkapiIndex, lexicon_id='lexicon')
        catalog.addIndex('att2', att2)

        att3 = KeywordIndex('att3')
        catalog.addIndex('att3', att3)

        if extra is not None:
            extra(catalog)

        for x in range(0, self.upper):
            catalog.catalogObject(Dummy(self.nums[x]), repr(x))
        return catalog.__of__(Dummy('foo'))

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
        def extra(catalog):
            catalog.addColumn('att1')
            num = FieldIndex('num')
            catalog.addIndex('num', num)

        catalog = self._make_one(extra=extra)
        ob = Dummy(9999)
        catalog.catalogObject(ob, '9999')
        brain = catalog(num=9999)[0]
        self.assertEqual(brain.att1, 'att1')
        ob.att1 = 'foobar'
        catalog.catalogObject(ob, '9999', update_metadata=0)
        brain = catalog(num=9999)[0]
        self.assertEqual(brain.att1, 'att1')
        catalog.catalogObject(ob, '9999')
        brain = catalog(num=9999)[0]
        self.assertEqual(brain.att1, 'foobar')

    def testUniqueValuesForLength(self):
        catalog = self._make_one()
        a = catalog.uniqueValuesFor('att1')
        self.assertEqual(len(a), 1, 'bad number of unique values %s' % a)

    def testUniqueValuesForContent(self):
        catalog = self._make_one()
        a = catalog.uniqueValuesFor('att1')
        self.assertEqual(a[0], 'att1', 'bad content %s' % a[0])

    # hasuid
    # recordify
    # instantiate
    # getMetadataForRID
    # getIndexDataForRID
    # make_query

    def testKeywordIndexWithMinRange(self):
        catalog = self._make_one()
        a = catalog(att3={'query': 'att', 'range': 'min'})
        self.assertEqual(len(a), self.upper)

    def testKeywordIndexWithMaxRange(self):
        catalog = self._make_one()
        a = catalog(att3={'query': 'att35', 'range': ':max'})
        self.assertEqual(len(a), self.upper)

    def testKeywordIndexWithMinMaxRangeCorrectSyntax(self):
        catalog = self._make_one()
        a = catalog(att3={'query': ['att', 'att35'], 'range': 'min:max'})
        self.assertEqual(len(a), self.upper)

    def testKeywordIndexWithMinMaxRangeWrongSyntax(self):
        # checkKeywordIndex with min/max range wrong syntax.
        catalog = self._make_one()
        a = catalog(att3={'query': ['att'], 'range': 'min:max'})
        self.assert_(len(a) != self.upper)

    def testCombinedTextandKeywordQuery(self):
        catalog = self._make_one()
        a = catalog(att3='att3', att2='att2')
        self.assertEqual(len(a), self.upper)
        a = catalog(att3='att3', att2='none')
        self.assertEqual(len(a), 0)

    def testResultLength(self):
        catalog = self._make_one()
        a = catalog(att1='att1')
        self.assertEqual(len(a), self.upper,
                         'length should be %s, its %s' % (self.upper, len(a)))

    def test_query_empty(self):
        # Queries with empty mappings used to return all.
        def extra(catalog):
            col1 = FieldIndex('col1')
            catalog.addIndex('col1', col1)
        catalog = self._make_one(extra=extra)
        self.assertTrue(len(catalog) > 0)
        all_data = catalog({})
        self.assertEqual(len(all_data), 0)

    def test_query_empty_keys(self):
        # Queries with empty keys used to return all.
        def extra(catalog):
            col1 = FieldIndex('col1')
            catalog.addIndex('col1', col1)
        catalog = self._make_one(extra=extra)
        a = catalog({'col1': ''})
        self.assertEqual(len(a), 0, 'length should be 0, its %s' % len(a))

    def test_field_index_length(self):
        catalog = self._make_one()
        a = catalog(att1='att1')
        self.assertEqual(len(a), self.upper)
        a = catalog(att1='none')
        self.assertEqual(len(a), 0)

    def test_text_index_length(self):
        catalog = self._make_one()
        a = catalog(att2='att2')
        self.assertEqual(len(a), self.upper)
        a = catalog(att2='none')
        self.assertEqual(len(a), 0)

    def test_keyword_index_length(self):
        catalog = self._make_one()
        a = catalog(att3='att3')
        self.assertEqual(len(a), self.upper)
        a = catalog(att3='none')
        self.assertEqual(len(a), 0)


class TestCatalogSortBatch(unittest.TestCase):

    upper = 100

    nums = list(range(upper))
    for i in range(upper):
        j = random.randrange(0, upper)
        tmp = nums[i]
        nums[i] = nums[j]
        nums[j] = tmp

    def _make_one(self, extra=None):
        from Products.ZCatalog.Catalog import Catalog
        catalog = Catalog()
        catalog.lexicon = PLexicon('lexicon')
        att1 = FieldIndex('att1')
        att2 = ZCTextIndex('att2', caller=catalog,
                           index_factory=OkapiIndex, lexicon_id='lexicon')
        catalog.addIndex('att2', att2)
        num = FieldIndex('num')

        catalog.addIndex('att1', att1)
        catalog.addIndex('num', num)
        catalog.addColumn('num')

        foo = MultiFieldIndex('foo')
        catalog.addIndex('foo', foo)

        if extra is not None:
            extra(catalog)

        for x in range(0, self.upper):
            catalog.catalogObject(Dummy(self.nums[x]), repr(x))
        return catalog.__of__(Dummy('foo'))

    def test_sorted_search_indexes_empty(self):
        catalog = self._make_one()
        result = catalog._sorted_search_indexes({})
        self.assertEquals(len(result), 0)

    def test_sorted_search_indexes_one(self):
        catalog = self._make_one()
        result = catalog._sorted_search_indexes({'att1': 'a'})
        self.assertEquals(result, ['att1'])

    def test_sorted_search_indexes_many(self):
        catalog = self._make_one()
        query = {'att1': 'a', 'att2': 'b', 'num': 1}
        result = catalog._sorted_search_indexes(query)
        self.assertEquals(set(result), set(['att1', 'att2', 'num']))

    def test_sorted_search_indexes_priority(self):
        # att2 and col2 don't support ILimitedResultIndex, att1 does
        catalog = self._make_one()
        query = {'att1': 'a', 'att2': 'b', 'col2': 'c'}
        result = catalog._sorted_search_indexes(query)
        self.assertEquals(result.index('att2'), 0)
        self.assertEquals(result.index('att1'), 1)

    def test_sorted_search_indexes_match_alternate_attr(self):
        catalog = self._make_one()
        query = {'bar': 'b'}
        result = catalog._sorted_search_indexes(query)
        self.assertEquals(result, ['foo'])

    def test_sorted_search_indexes_no_match(self):
        catalog = self._make_one()
        result = catalog._sorted_search_indexes({'baz': 'a'})
        self.assertEquals(result, [])

    def test_sortResults(self):
        catalog = self._make_one()
        brains = catalog({'att1': 'att1'})
        rs = IISet([b.getRID() for b in brains])
        si = catalog.getIndex('num')
        result = catalog.sortResults(rs, si)
        self.assertEqual([r.num for r in result], list(range(100)))

    def test_sortResults_reversed(self):
        catalog = self._make_one()
        brains = catalog({'att1': 'att1'})
        rs = IISet([b.getRID() for b in brains])
        si = catalog.getIndex('num')
        result = catalog.sortResults(rs, si, reverse=True)
        self.assertEqual([r.num for r in result], list(reversed(range(100))))

    def test_sortResults_limit(self):
        catalog = self._make_one()
        brains = catalog({'att1': 'att1'})
        rs = IISet([b.getRID() for b in brains])
        si = catalog.getIndex('num')
        result = catalog.sortResults(rs, si, limit=10)
        self.assertEqual(len(result), 10)
        self.assertEqual(result.actual_result_count, 100)
        self.assertEqual([r.num for r in result], list(range(10)))

    def test_sortResults_limit_reversed(self):
        catalog = self._make_one()
        brains = catalog({'att1': 'att1'})
        rs = IISet([b.getRID() for b in brains])
        si = catalog.getIndex('num')
        result = catalog.sortResults(rs, si, reverse=True, limit=10)
        self.assertEqual(len(result), 10)
        self.assertEqual(result.actual_result_count, 100)
        expected = list(reversed(range(90, 100)))
        self.assertEqual([r.num for r in result], expected)

    def testLargeSortedResultSetWithSmallIndex(self):
        # This exercises the optimization in the catalog that iterates
        # over the sort index rather than the result set when the result
        # set is much larger than the sort index.
        catalog = self._make_one()
        a = catalog(att1='att1', sort_on='att1')
        self.assertEqual(len(a), self.upper)
        self.assertEqual(a.actual_result_count, self.upper)

    def testSortLimit(self):
        catalog = self._make_one()
        full = list(catalog(att1='att1', sort_on='num'))
        a = catalog(att1='att1', sort_on='num', sort_limit=10)
        self.assertEqual([r.num for r in a], [r.num for r in full[:10]])
        self.assertEqual(a.actual_result_count, self.upper)
        a = catalog(att1='att1', sort_on='num',
                    sort_limit=10, sort_order='reverse')
        rev = [r.num for r in full[-10:]]
        rev.reverse()
        self.assertEqual([r.num for r in a], rev)
        self.assertEqual(a.actual_result_count, self.upper)

    def testBigSortLimit(self):
        catalog = self._make_one()
        a = catalog(
            att1='att1', sort_on='num', sort_limit=self.upper * 3)
        self.assertEqual(a.actual_result_count, self.upper)
        self.assertEqual(a[0].num, 0)
        a = catalog(att1='att1', sort_on='num',
                    sort_limit=self.upper * 3, sort_order='reverse')
        self.assertEqual(a.actual_result_count, self.upper)
        self.assertEqual(a[0].num, self.upper - 1)

    def testSortLimitViaBatchingArgsBeforeStart(self):
        catalog = self._make_one()
        query = dict(att1='att1', sort_on='num', b_start=-5, b_size=8)
        result = catalog(query)
        self.assertEqual(result.actual_result_count, 100)
        self.assertEqual([r.num for r in result], list(range(0, 3)))

    def testSortLimitViaBatchingArgsStart(self):
        catalog = self._make_one()
        query = dict(att1='att1', sort_on='num', b_start=0, b_size=5)
        result = catalog(query)
        self.assertEqual(result.actual_result_count, 100)
        self.assertEqual([r.num for r in result], list(range(0, 5)))

    def testSortLimitViaBatchingEarlyFirstHalf(self):
        catalog = self._make_one()
        query = dict(att1='att1', sort_on='num', b_start=11, b_size=17)
        result = catalog(query)
        self.assertEqual(result.actual_result_count, 100)
        self.assertEqual([r.num for r in result], list(range(11, 28)))

    def testSortLimitViaBatchingArgsLateFirstHalf(self):
        catalog = self._make_one()
        query = dict(att1='att1', sort_on='num', b_start=30, b_size=15)
        result = catalog(query)
        self.assertEqual(result.actual_result_count, 100)
        self.assertEqual([r.num for r in result], list(range(30, 45)))

    def testSortLimitViaBatchingArgsLeftMiddle(self):
        catalog = self._make_one()
        query = dict(att1='att1', sort_on='num', b_start=45, b_size=8)
        result = catalog(query)
        self.assertEqual(result.actual_result_count, 100)
        self.assertEqual([r.num for r in result], list(range(45, 53)))

    def testSortLimitViaBatchingArgsRightMiddle(self):
        catalog = self._make_one()
        query = dict(att1='att1', sort_on='num', b_start=48, b_size=8)
        result = catalog(query)
        self.assertEqual(result.actual_result_count, 100)
        self.assertEqual([r.num for r in result], list(range(48, 56)))

    def testSortLimitViaBatchingArgsRightMiddleSortOnTwoSecond(self):
        catalog = self._make_one()
        query = dict(att1='att1', sort_on=('att1', 'num'),
                     sort_order=('', 'reverse'), b_start=48, b_size=8)
        result = catalog(query)
        self.assertEqual(result.actual_result_count, 100)
        self.assertEqual([r.num for r in result], list(range(51, 43, -1)))

    def testSortLimitViaBatchingArgsEarlySecondHalf(self):
        catalog = self._make_one()
        query = dict(att1='att1', sort_on='num', b_start=55, b_size=15)
        result = catalog(query)
        self.assertEqual(result.actual_result_count, 100)
        self.assertEqual([r.num for r in result], list(range(55, 70)))

    def testSortLimitViaBatchingArgsEarlySecondHalfSortOnTwoFirst(self):
        catalog = self._make_one()
        query = dict(att1='att1', sort_on=('att1', 'num'),
                     sort_order=('reverse', ''), b_start=55, b_size=15)
        result = catalog(query)
        self.assertEqual(result.actual_result_count, 100)
        self.assertEqual([r.num for r in result], list(range(55, 70)))

    def testSortLimitViaBatchingArgsEarlySecondHalfSortOnTwoSecond(self):
        catalog = self._make_one()
        query = dict(att1='att1', sort_on=('att1', 'num'),
                     sort_order=('', 'reverse'), b_start=55, b_size=15)
        result = catalog(query)
        self.assertEqual(result.actual_result_count, 100)
        self.assertEqual([r.num for r in result], list(range(44, 29, -1)))

    def testSortLimitViaBatchingArgsEarlySecondHalfSortOnTwoBoth(self):
        catalog = self._make_one()
        query = dict(att1='att1', sort_on=('att1', 'num'),
                     sort_order=('reverse', 'reverse'), b_start=55, b_size=15)
        result = catalog(query)
        self.assertEqual(result.actual_result_count, 100)
        self.assertEqual([r.num for r in result], list(range(44, 29, -1)))

    def testSortLimitViaBatchingArgsSecondHalf(self):
        catalog = self._make_one()
        query = dict(att1='att1', sort_on='num', b_start=70, b_size=15)
        result = catalog(query)
        self.assertEqual(result.actual_result_count, 100)
        self.assertEqual([r.num for r in result], list(range(70, 85)))

    def testSortLimitViaBatchingArgsEnd(self):
        catalog = self._make_one()
        query = dict(att1='att1', sort_on='num', b_start=90, b_size=10)
        result = catalog(query)
        self.assertEqual(result.actual_result_count, 100)
        self.assertEqual([r.num for r in result], list(range(90, 100)))

    def testSortLimitViaBatchingArgsOverEnd(self):
        catalog = self._make_one()
        query = dict(att1='att1', sort_on='num', b_start=90, b_size=15)
        result = catalog(query)
        self.assertEqual(result.actual_result_count, 100)
        self.assertEqual([r.num for r in result], list(range(90, 100)))

    def testSortLimitViaBatchingArgsOutside(self):
        catalog = self._make_one()
        query = dict(att1='att1', sort_on='num', b_start=110, b_size=10)
        result = catalog(query)
        self.assertEqual(result.actual_result_count, 100)
        self.assertEqual([r.num for r in result], [])

    def testSortedResultLengthWithMissingDocs(self):
        catalog = self._make_one()
        # remove the `0` document from the num index only
        num_index = catalog.getIndex('num')
        pos_of_zero = self.nums.index(0)
        uid = catalog.uids.get(repr(pos_of_zero))
        self.assertEqual(catalog[uid].num, 0)
        num_index.unindex_object(uid)
        # make sure it was removed
        self.assertEqual(len(num_index), 99)
        # sort over the smaller num index
        query = dict(att1='att1', sort_on='num', sort_limit=10)
        result = catalog(query)
        # the `0` document was removed
        self.assertEqual(result[0].num, 1)
        self.assertEqual(len(result), 10)
        # there are only 99 documents left
        self.assertEqual(result.actual_result_count, 99)

    # _get_sort_attr
    # _getSortIndex

    def test_search_not(self):
        catalog = self._make_one()
        query = dict(att1='att1', num={'not': [0, 1]})
        result = catalog(query)
        self.assertEqual(len(result), self.upper - 2)

    def test_search_not_nothing(self):
        def extra(catalog):
            col1 = FieldIndex('col1')
            catalog.addIndex('col1', col1)
        catalog = self._make_one(extra)
        query = dict(att1='att1', col1={'not': 'col1'})
        result = catalog(query)
        self.assertEqual(len(result), 0)

    def test_search_not_no_value_in_index(self):
        def extra(catalog):
            ends_in_zero = FieldIndex('ends_in_zero')
            catalog.addIndex('ends_in_zero', ends_in_zero)
        catalog = self._make_one(extra=extra)
        query = dict(att1='att1', ends_in_zero={'not': False})
        result = catalog(query)
        self.assertEqual(len(result), 10)

    def test_sort_on_good_index(self):
        catalog = self._make_one()
        upper = self.upper
        a = catalog(att1='att1', sort_on='num')
        self.assertEqual(len(a), upper)
        for x in range(self.upper):
            self.assertEqual(a[x].num, x)

    def test_sort_on_bad_index(self):
        from Products.ZCatalog.Catalog import CatalogError
        catalog = self._make_one()

        def badsortindex():
            catalog(sort_on='foofaraw')
        self.assertRaises(CatalogError, badsortindex)

    def test_sort_on_wrong_index(self):
        from Products.ZCatalog.Catalog import CatalogError
        catalog = self._make_one()

        def wrongsortindex():
            catalog(sort_on='att2')
        self.assertRaises(CatalogError, wrongsortindex)

    def test_sort_on(self):
        catalog = self._make_one()
        upper = self.upper
        a = catalog(sort_on='num', att2='att2')
        self.assertEqual(len(a), upper)
        for x in range(self.upper):
            self.assertEqual(a[x].num, x)

    def test_sort_on_missing(self):
        catalog = self._make_one()
        upper = self.upper
        a = catalog(att2='att2')
        self.assertEqual(len(a), upper)

    def test_sort_on_two(self):
        catalog = self._make_one()
        upper = self.upper
        a = catalog(sort_on=('att1', 'num'), att1='att1')
        self.assertEqual(len(a), upper)
        for x in range(self.upper):
            self.assertEqual(a[x].num, x)

    def test_sort_on_two_reverse(self):
        catalog = self._make_one()
        upper = self.upper
        a = catalog(sort_on=('att1', 'num'), att1='att1',
                    sort_order='reverse')
        self.assertEqual(len(a), upper)
        for x in range(upper - 1):
            self.assertTrue(a[x].num > a[x + 1].num)

    def test_sort_on_two_reverse_neither(self):
        catalog = self._make_one()
        upper = self.upper
        a = catalog(sort_on=('att1', 'num'), att1='att1',
                    sort_order=('', ''))
        self.assertEqual(len(a), upper)
        for x in range(upper - 1):
            self.assertTrue(a[x].num < a[x + 1].num)

    def test_sort_on_two_reverse_first(self):
        catalog = self._make_one()
        upper = self.upper
        a = catalog(sort_on=('att1', 'num'), att1='att1',
                    sort_order=('reverse', ''))
        self.assertEqual(len(a), upper)
        for x in range(upper - 1):
            self.assertTrue(a[x].num < a[x + 1].num)

    def test_sort_on_two_reverse_second(self):
        catalog = self._make_one()
        upper = self.upper
        a = catalog(sort_on=('att1', 'num'), att1='att1',
                    sort_order=('', 'reverse'))
        self.assertEqual(len(a), upper)
        for x in range(upper - 1):
            self.assertTrue(a[x].num > a[x + 1].num)

    def test_sort_on_two_reverse_both(self):
        catalog = self._make_one()
        upper = self.upper
        a = catalog(sort_on=('att1', 'num'), att1='att1',
                    sort_order=('reverse', 'reverse'))
        self.assertEqual(len(a), upper)
        for x in range(upper - 1):
            self.assertTrue(a[x].num > a[x + 1].num)

    def test_sort_on_two_reverse_too_many(self):
        catalog = self._make_one()
        upper = self.upper
        a = catalog(sort_on=('att1', 'num'), att1='att1',
                    sort_order=('', '', 'reverse', ''))
        self.assertEqual(len(a), upper)
        for x in range(upper - 1):
            self.assertTrue(a[x].num < a[x + 1].num)

    def test_sort_on_two_small_limit(self):
        catalog = self._make_one()
        a = catalog(sort_on=('att1', 'num'), att1='att1', sort_limit=10)
        self.assertEqual(len(a), 10)
        for x in range(9):
            self.assertTrue(a[x].num < a[x + 1].num)

    def test_sort_on_two_small_limit_reverse(self):
        catalog = self._make_one()
        a = catalog(sort_on=('att1', 'num'), att1='att1',
                    sort_limit=10, sort_order='reverse')
        self.assertEqual(len(a), 10)
        for x in range(9):
            self.assertTrue(a[x].num > a[x + 1].num)

    def test_sort_on_two_big_limit(self):
        catalog = self._make_one()
        a = catalog(sort_on=('att1', 'num'), att1='att1',
                    sort_limit=self.upper * 3)
        self.assertEqual(len(a), 100)
        for x in range(99):
            self.assertTrue(a[x].num < a[x + 1].num)

    def test_sort_on_two_big_limit_reverse(self):
        catalog = self._make_one()
        a = catalog(sort_on=('att1', 'num'), att1='att1',
                    sort_limit=self.upper * 3, sort_order='reverse')
        self.assertEqual(len(a), 100)
        for x in range(99):
            self.assertTrue(a[x].num > a[x + 1].num)

    def test_sort_on_three(self):
        def extra(catalog):
            col2 = FieldIndex('col2')
            catalog.addIndex('col2', col2)
        catalog = self._make_one(extra)
        a = catalog(sort_on=('att1', 'col2', 'num'), att1='att1')
        self.assertEqual(len(a), self.upper)
        for x in range(self.upper):
            self.assertEqual(a[x].num, x)

    def test_sort_on_three_reverse(self):
        def extra(catalog):
            col2 = FieldIndex('col2')
            catalog.addIndex('col2', col2)
        catalog = self._make_one(extra)
        a = catalog(sort_on=('att1', 'col2', 'num'), att1='att1',
                    sort_order='reverse')
        self.assertEqual(len(a), self.upper)
        for x in range(self.upper - 1):
            self.assertTrue(a[x].num > a[x + 1].num)

    def test_sort_on_three_reverse_last(self):
        def extra(catalog):
            col2 = FieldIndex('col2')
            catalog.addIndex('col2', col2)
        catalog = self._make_one(extra)
        a = catalog(sort_on=('att1', 'col2', 'num'), att1='att1',
                    sort_order=('', '', 'reverse'))
        self.assertEqual(len(a), self.upper)
        for x in range(self.upper - 1):
            self.assertTrue(a[x].num > a[x + 1].num)

    def test_sort_on_three_small_limit(self):
        def extra(catalog):
            col2 = FieldIndex('col2')
            catalog.addIndex('col2', col2)
        catalog = self._make_one(extra)
        a = catalog(sort_on=('att1', 'col2', 'num'), att1='att1',
                    sort_limit=10)
        self.assertEqual(len(a), 10)
        for x in range(9):
            self.assertTrue(a[x].num < a[x + 1].num)

    def test_sort_on_three_big_limit(self):
        def extra(catalog):
            col2 = FieldIndex('col2')
            catalog.addIndex('col2', col2)
        catalog = self._make_one(extra)
        a = catalog(sort_on=('att1', 'col2', 'num'), att1='att1',
                    sort_limit=self.upper * 3)
        self.assertEqual(len(a), 100)
        for x in range(99):
            self.assertTrue(a[x].num < a[x + 1].num)


class TestUnCatalog(unittest.TestCase):

    upper = 5

    def _make_one(self):
        from Products.ZCatalog.Catalog import Catalog
        catalog = Catalog()
        catalog.lexicon = PLexicon('lexicon')
        att1 = FieldIndex('att1')
        att2 = ZCTextIndex('att2', caller=catalog,
                           index_factory=OkapiIndex, lexicon_id='lexicon')
        att3 = KeywordIndex('att3')
        catalog.addIndex('att1', att1)
        catalog.addIndex('att2', att2)
        catalog.addIndex('att3', att3)

        for x in range(0, self.upper):
            catalog.catalogObject(Dummy(x), repr(x))
        return catalog.__of__(Dummy('foo'))

    def _uncatalog(self, catalog):
        for x in range(0, self.upper):
            catalog.uncatalogObject(repr(x))

    def test_uncatalog_field_index(self):
        catalog = self._make_one()
        self._uncatalog(catalog)
        a = catalog(att1='att1')
        self.assertEqual(len(a), 0, 'len: %s' % len(a))

    def test_uncatalog_text_index(self):
        catalog = self._make_one()
        self._uncatalog(catalog)
        a = catalog(att2='att2')
        self.assertEqual(len(a), 0, 'len: %s' % len(a))

    def test_uncatalog_keyword_index(self):
        catalog = self._make_one()
        self._uncatalog(catalog)
        a = catalog(att3='att3')
        self.assertEqual(len(a), 0, 'len: %s' % len(a))

    def test_bad_uncatalog(self):
        catalog = self._make_one()
        try:
            catalog.uncatalogObject('asdasdasd')
        except Exception:
            self.fail('uncatalogObject raised exception on bad uid')

    def test_uncatalog_twice(self):
        catalog = self._make_one()
        catalog.uncatalogObject('0')

        def _second(self):
            catalog.uncatalogObject('0')
        self.assertRaises(Exception, _second)

    def test_uncatalog_ength(self):
        catalog = self._make_one()
        self._uncatalog(catalog)
        self.assertEqual(len(catalog), 0)


class TestRangeSearch(unittest.TestCase):

    def _make_one(self):
        from Products.ZCatalog.Catalog import Catalog
        return Catalog()

    def test_range_search(self):
        catalog = self._make_one()
        index = FieldIndex('number')
        catalog.addIndex('number', index)
        catalog.addColumn('number')
        for i in range(50):
            obj = ObjRS(random.randrange(0, 200))
            catalog.catalogObject(obj, i)
        catalog = catalog.__of__(ObjRS(20))

        for i in range(10):
            m = random.randrange(0, 200)
            n = m + 10
            for r in catalog(number={'query': (m, n), 'range': 'min:max'}):
                size = r.number
                self.assert_(m <= size and size <= n,
                             "%d vs [%d,%d]" % (r.number, m, n))


class TestMergeResults(unittest.TestCase):

    def _make_one(self):
        from Products.ZCatalog.Catalog import Catalog
        return Catalog()

    def _make_many(self):
        from Products.ZCatalog.Catalog import mergeResults
        catalogs = []
        for i in range(3):
            cat = self._make_one()
            cat.lexicon = PLexicon('lexicon')
            cat.addIndex('num', FieldIndex('num'))
            cat.addIndex('big', FieldIndex('big'))
            cat.addIndex('number', FieldIndex('number'))
            i = ZCTextIndex('title', caller=cat, index_factory=OkapiIndex,
                            lexicon_id='lexicon')
            cat.addIndex('title', i)
            cat = cat.__of__(ZDummy(16336))
            for i in range(10):
                obj = ZDummy(i)
                obj.big = i > 5
                obj.number = True
                cat.catalogObject(obj, str(i))
            catalogs.append(cat)
        return catalogs, mergeResults

    def _sort(self, iterable, reverse=False):
        L = list(iterable)
        if reverse:
            L.sort(reverse=True)
        else:
            L.sort()
        return L

    def test_no_filter_or_sort(self):
        catalogs, mergeResults = self._make_many()
        results = [cat.searchResults(
                   dict(number=True), _merge=0) for cat in catalogs]
        merged_rids = [r.getRID() for r in mergeResults(
            results, has_sort_keys=False, reverse=False)]
        expected = [r.getRID() for r in chain(*results)]
        self.assertEqual(self._sort(merged_rids), self._sort(expected))

    def test_sorted_only(self):
        catalogs, mergeResults = self._make_many()
        results = [cat.searchResults(
                   dict(number=True, sort_on='num'), _merge=0)
                   for cat in catalogs]
        merged_rids = [r.getRID() for r in mergeResults(
            results, has_sort_keys=True, reverse=False)]
        expected = self._sort(chain(*results))
        expected = [rid for sortkey, rid, getitem in expected]
        self.assertEqual(merged_rids, expected)

    def test_sort_reverse(self):
        catalogs, mergeResults = self._make_many()
        results = [cat.searchResults(
                   dict(number=True, sort_on='num'), _merge=0)
                   for cat in catalogs]
        merged_rids = [r.getRID() for r in mergeResults(
            results, has_sort_keys=True, reverse=True)]
        expected = self._sort(chain(*results), reverse=True)
        expected = [rid for sortkey, rid, getitem in expected]
        self.assertEqual(merged_rids, expected)

    def test_limit_sort(self):
        catalogs, mergeResults = self._make_many()
        results = [cat.searchResults(
                   dict(att1='att1', number=True, sort_on='num',
                        sort_limit=2), _merge=0)
                   for cat in catalogs]
        merged_rids = [r.getRID() for r in mergeResults(
            results, has_sort_keys=True, reverse=False)]
        expected = self._sort(chain(*results))
        expected = [rid for sortkey, rid, getitem in expected]
        self.assertEqual(merged_rids, expected)

    def test_scored(self):
        catalogs, mergeResults = self._make_many()
        results = [cat.searchResults(title='4 or 5 or 6', _merge=0)
                   for cat in catalogs]
        merged_rids = [r.getRID() for r in mergeResults(
            results, has_sort_keys=True, reverse=False)]
        expected = self._sort(chain(*results))
        expected = [rid for sortkey, (nscore, score, rid), getitem in expected]
        self.assertEqual(merged_rids, expected)

    def test_small_index_sort(self):
        # Test that small index sort optimization is not used for merging
        catalogs, mergeResults = self._make_many()
        results = [cat.searchResults(
                   dict(number=True, sort_on='big'), _merge=0)
                   for cat in catalogs]
        merged_rids = [r.getRID() for r in mergeResults(
            results, has_sort_keys=True, reverse=False)]
        expected = self._sort(chain(*results))
        expected = [rid for sortkey, rid, getitem in expected]
        self.assertEqual(merged_rids, expected)


class TestScoring(unittest.TestCase):

    def _make_one(self):
        from Products.ZCatalog.Catalog import Catalog
        catalog = Catalog()
        catalog.lexicon = PLexicon('lexicon')
        idx = ZCTextIndex('title', caller=catalog,
                          index_factory=OkapiIndex, lexicon_id='lexicon')
        catalog.addIndex('title', idx)
        catalog.addIndex('true', FieldIndex('true'))
        catalog.addColumn('title')
        for i in (1, 2, 3, 10, 11, 110, 111):
            obj = ZDummy(i)
            obj.true = True
            if i == 110:
                obj.true = False
            catalog.catalogObject(obj, str(i))
        return catalog.__of__(ZDummy(1))

    def test_simple_search(self):
        cat = self._make_one()
        brains = cat(title='10')
        self.assertEqual(len(brains), 1)
        self.assertEqual(brains[0].title, '10')

    def test_or_search(self):
        cat = self._make_one()
        brains = cat(title='2 OR 3')
        self.assertEqual(len(brains), 2)

    def test_scored_search(self):
        cat = self._make_one()
        brains = cat(title='1*')
        self.assertEqual(len(brains), 5)
        self.assertEqual(brains[0].title, '111')

    def test_combined_scored_search(self):
        cat = self._make_one()
        brains = cat(title='1*', true=True)
        self.assertEqual(len(brains), 4)
        self.assertEqual(brains[0].title, '111')

    def test_combined_scored_search_planned(self):
        from ..plan import Benchmark
        from ..plan import PriorityMap
        cat = self._make_one()
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
        # plan with true first
        PriorityMap.set_entry(catalog_id, plan_key, dict(
            title=Benchmark(2, 1, False),
            true=Benchmark(1, 1, False),
        ))
        brains = cat(query)
        self.assertEqual(len(brains), 4)
