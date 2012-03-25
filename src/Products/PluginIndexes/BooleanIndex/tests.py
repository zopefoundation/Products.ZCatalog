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

        res, idx = index._apply_index({'truth': True}, resultset=IISet([]))
        self.assertEqual(idx, ('truth', ))
        self.assertEqual(list(res), [])

        res, idx = index._apply_index({'truth': True}, resultset=IISet([2]))
        self.assertEqual(idx, ('truth', ))
        self.assertEqual(list(res), [])

        res, idx = index._apply_index({'truth': True}, resultset=IISet([1]))
        self.assertEqual(idx, ('truth', ))
        self.assertEqual(list(res), [1])

        res, idx = index._apply_index({'truth': True}, resultset=IISet([1, 2]))
        self.assertEqual(idx, ('truth', ))
        self.assertEqual(list(res), [1])

        res, idx = index._apply_index({'truth': False},
                                      resultset=IISet([1, 2]))
        self.assertEqual(idx, ('truth', ))
        self.assertEqual(list(res), [2])

    def test_index_many_true(self):
        index = self._makeOne()
        for i in range(0, 100):
            obj = Dummy(i, i < 80 and True or False)
            index._index_object(obj.id, obj, attr='truth')
        self.assertEqual(list(index._index), range(80, 100))
        self.assertEqual(len(index._unindex), 100)

        res, idx = index._apply_index({'truth': True})
        self.assertEqual(list(res), range(0, 80))
        res, idx = index._apply_index({'truth': False})
        self.assertEqual(list(res), range(80, 100))

    def test_index_many_false(self):
        index = self._makeOne()
        for i in range(0, 100):
            obj = Dummy(i, i >= 80 and True or False)
            index._index_object(obj.id, obj, attr='truth')
        self.assertEqual(list(index._index), range(80, 100))
        self.assertEqual(len(index._unindex), 100)

        res, idx = index._apply_index({'truth': False})
        self.assertEqual(list(res), range(0, 80))
        res, idx = index._apply_index({'truth': True})
        self.assertEqual(list(res), range(80, 100))

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
        self.assertEqual(list(index._index), range(4, 8))
        self.assertEqual(len(index._unindex), 8)
        # Once False gets to be more than 60% of the indexed set, we switch
        add(8, False)
        self.assertEqual(list(index._index), range(4, 9))
        add(9, False)
        self.assertEqual(list(index._index), range(0, 4))
        res, idx = index._apply_index({'truth': True})
        self.assertEqual(list(res), range(0, 4))
        res, idx = index._apply_index({'truth': False})
        self.assertEqual(list(res), range(4, 10))
        # and we can again switch if the percentages change again
        for i in range(6, 10):
            index.unindex_object(i)
        self.assertEqual(list(index._index), range(4, 6))
        self.assertEqual(len(index._unindex), 6)
        res, idx = index._apply_index({'truth': True})
        self.assertEqual(list(res), range(0, 4))
        res, idx = index._apply_index({'truth': False})
        self.assertEqual(list(res), range(4, 6))

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

    def test_migration(self):
        index = self._makeOne()
        for i in range(0, 100):
            obj = Dummy(i, i < 80 and True or False)
            index._index_object(obj.id, obj, attr='truth')
        # now hack the state to match what we had before
        delattr(index, '_index_length')
        delattr(index, '_index_value')
        # we had True values in _index even though there was more of them
        index._index.clear()
        index._index.update(range(0, 80))
        # the length only kept track of the _index
        index._length.change(-20)
        # remove one to trigger migration
        index.unindex_object(99)
        self.assertEqual(index._length.value, 99)
        self.assertEqual(index._index_value, 0)
        self.assertEqual(index._index_length.value, 19)
        self.assertEqual(list(index._index), range(80, 99))
