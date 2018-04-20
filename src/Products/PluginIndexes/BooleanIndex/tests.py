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

from BTrees.IIBTree import IISet


class Dummy(object):

    def __init__(self, docid, truth):
        self.id = docid
        self.truth = truth


class TestBooleanIndex(unittest.TestCase):

    def _getTargetClass(self):
        from Products.PluginIndexes.BooleanIndex import BooleanIndex
        return BooleanIndex.BooleanIndex

    def _makeOne(self, attr='truth'):
        return self._getTargetClass()(attr)

    def test_index_true(self):
        index = self._makeOne()
        obj = Dummy(1, True)
        index._index_object(obj.id, obj, attr='truth')
        self.assertTrue(1 in index._unindex)
        self.assertFalse(1 in index._index)

    def test_index_false(self):
        index = self._makeOne()
        obj = Dummy(1, False)
        index._index_object(obj.id, obj, attr='truth')
        self.assertTrue(1 in index._unindex)
        self.assertFalse(1 in index._index)

    def test_index_missing_attribute(self):
        index = self._makeOne()
        obj = Dummy(1, True)
        index._index_object(obj.id, obj, attr='missing')
        self.assertFalse(1 in index._unindex)
        self.assertFalse(1 in index._index)

    def test_search_true(self):
        index = self._makeOne()
        obj = Dummy(1, True)
        index._index_object(obj.id, obj, attr='truth')
        obj = Dummy(2, False)
        index._index_object(obj.id, obj, attr='truth')

        res, idx = index._apply_index({'truth': True})
        self.assertEqual(idx, ('truth', ))
        self.assertEqual(list(res), [1])

    def test_search_false(self):
        index = self._makeOne()
        obj = Dummy(1, True)
        index._index_object(obj.id, obj, attr='truth')
        obj = Dummy(2, False)
        index._index_object(obj.id, obj, attr='truth')

        res, idx = index._apply_index({'truth': False})
        self.assertEqual(idx, ('truth', ))
        self.assertEqual(list(res), [2])

    def test_search_inputresult(self):
        index = self._makeOne()
        obj = Dummy(1, True)
        index._index_object(obj.id, obj, attr='truth')
        obj = Dummy(2, False)
        index._index_object(obj.id, obj, attr='truth')
        obj = Dummy(3, True)
        index._index_object(obj.id, obj, attr='truth')

        # The less common value is indexed
        self.assertEqual(index._index_value, 0)

        res, idx = index._apply_index({'truth': True}, resultset=IISet([]))
        self.assertEqual(idx, ('truth', ))
        self.assertEqual(list(res), [])

        res, idx = index._apply_index({'truth': False}, resultset=IISet([]))
        self.assertEqual(idx, ('truth', ))
        self.assertEqual(list(res), [])

        res, idx = index._apply_index({'truth': True}, resultset=IISet([1]))
        self.assertEqual(list(res), [1])

        res, idx = index._apply_index({'truth': False}, resultset=IISet([1]))
        self.assertEqual(list(res), [])

        res, idx = index._apply_index({'truth': True}, resultset=IISet([2]))
        self.assertEqual(list(res), [])

        res, idx = index._apply_index({'truth': False}, resultset=IISet([2]))
        self.assertEqual(list(res), [2])

        res, idx = index._apply_index({'truth': True},
                                      resultset=IISet([1, 2]))
        self.assertEqual(list(res), [1])

        res, idx = index._apply_index({'truth': False},
                                      resultset=IISet([1, 2]))
        self.assertEqual(list(res), [2])

        res, idx = index._apply_index({'truth': True},
                                      resultset=IISet([1, 3]))
        self.assertEqual(list(res), [1, 3])

        res, idx = index._apply_index({'truth': False},
                                      resultset=IISet([1, 3]))
        self.assertEqual(list(res), [])

        res, idx = index._apply_index({'truth': True},
                                      resultset=IISet([1, 2, 3]))
        self.assertEqual(list(res), [1, 3])

        res, idx = index._apply_index({'truth': False},
                                      resultset=IISet([1, 2, 3]))
        self.assertEqual(list(res), [2])

        res, idx = index._apply_index({'truth': True},
                                      resultset=IISet([1, 2, 99]))
        self.assertEqual(list(res), [1])

        res, idx = index._apply_index({'truth': False},
                                      resultset=IISet([1, 2, 99]))
        self.assertEqual(list(res), [2])

    def test_index_many_true(self):
        index = self._makeOne()
        for i in range(0, 100):
            obj = Dummy(i, i < 80 and True or False)
            index._index_object(obj.id, obj, attr='truth')
        self.assertEqual(list(index._index), list(range(80, 100)))
        self.assertEqual(len(index._unindex), 100)

        res, idx = index._apply_index({'truth': True})
        self.assertEqual(list(res), list(range(0, 80)))
        res, idx = index._apply_index({'truth': False})
        self.assertEqual(list(res), list(range(80, 100)))

    def test_index_many_false(self):
        index = self._makeOne()
        for i in range(0, 100):
            obj = Dummy(i, i >= 80 and True or False)
            index._index_object(obj.id, obj, attr='truth')
        self.assertEqual(list(index._index), list(range(80, 100)))
        self.assertEqual(len(index._unindex), 100)

        res, idx = index._apply_index({'truth': False})
        self.assertEqual(list(res), list(range(0, 80)))
        res, idx = index._apply_index({'truth': True})
        self.assertEqual(list(res), list(range(80, 100)))

    def test_index_many_change(self):
        index = self._makeOne()

        def add(i, value):
            obj = Dummy(i, value)
            index._index_object(obj.id, obj, attr='truth')

        # First lets index only True values
        for i in range(0, 4):
            add(i, True)
        self.assertEqual(list(index._index), [])
        self.assertEqual(len(index._unindex), 4)
        # Now add an equal number of False values
        for i in range(4, 8):
            add(i, False)
        self.assertEqual(list(index._index), list(range(4, 8)))
        self.assertEqual(len(index._unindex), 8)
        # Once False gets to be more than 60% of the indexed set, we switch
        add(8, False)
        self.assertEqual(list(index._index), list(range(4, 9)))
        add(9, False)
        self.assertEqual(list(index._index), list(range(0, 4)))
        res, idx = index._apply_index({'truth': True})
        self.assertEqual(list(res), list(range(0, 4)))
        res, idx = index._apply_index({'truth': False})
        self.assertEqual(list(res), list(range(4, 10)))
        # and we can again switch if the percentages change again
        for i in range(6, 10):
            index.unindex_object(i)
        self.assertEqual(list(index._index), list(range(4, 6)))
        self.assertEqual(len(index._unindex), 6)
        res, idx = index._apply_index({'truth': True})
        self.assertEqual(list(res), list(range(0, 4)))
        res, idx = index._apply_index({'truth': False})
        self.assertEqual(list(res), list(range(4, 6)))

    def test_items(self):
        index = self._makeOne()
        # test empty
        items = dict(index.items())
        self.assertEqual(len(items[True]), 0)
        self.assertEqual(len(items[False]), 0)
        # test few trues
        for i in range(0, 20):
            obj = Dummy(i, i < 5 and True or False)
            index._index_object(obj.id, obj, attr='truth')
        items = dict(index.items())
        self.assertEqual(len(items[True]), 5)
        self.assertEqual(len(items[False]), 15)
        # test many trues
        for i in range(7, 20):
            index.unindex_object(i)
        items = dict(index.items())
        self.assertEqual(len(items[True]), 5)
        self.assertEqual(len(items[False]), 2)

    def test_histogram(self):
        index = self._makeOne()
        # test empty
        hist = index.histogram()
        self.assertEqual(hist[True], 0)
        self.assertEqual(hist[False], 0)
        # test few trues
        for i in range(0, 20):
            obj = Dummy(i, i < 5 and True or False)
            index._index_object(obj.id, obj, attr='truth')
        hist = index.histogram()
        self.assertEqual(hist[True], 5)
        self.assertEqual(hist[False], 15)

    def test_reindexation_when_index_reversed(self):
        index = self._makeOne()
        obj1 = Dummy(1, False)
        index._index_object(obj1.id, obj1, attr='truth')
        obj2 = Dummy(2, False)
        self.assertTrue(index._index_value)
        index._index_object(obj2.id, obj2, attr='truth')
        obj3 = Dummy(3, True)
        index._index_object(obj3.id, obj3, attr='truth')
        obj4 = Dummy(4, True)
        index._index_object(obj4.id, obj4, attr='truth')
        obj1.truth = True
        index._index_object(obj1.id, obj1, attr='truth')
        self.assertFalse(index._index_value)

        res = index._apply_index({'truth': True})[0]
        self.assertEqual(list(index._index), [2])
        self.assertEqual(list(res), [1, 3, 4])

    def test_getCounter(self):
        index = self._makeOne()

        self.assertEqual(index.getCounter(), 0)

        obj = Dummy(1, True)
        index.index_object(obj.id, obj)
        self.assertEqual(index.getCounter(), 1)

        index.unindex_object(obj.id)
        self.assertEqual(index.getCounter(), 2)

        # unknown id
        index.unindex_object(1234)
        self.assertEqual(index.getCounter(), 2)

        # clear is a change
        index.clear()
        self.assertEqual(index.getCounter(), 3)
