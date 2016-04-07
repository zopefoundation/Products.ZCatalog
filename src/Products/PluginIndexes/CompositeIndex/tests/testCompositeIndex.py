import unittest

import random

from time import time

from BTrees.IIBTree import weightedIntersection

from Products.PluginIndexes.FieldIndex.FieldIndex import FieldIndex
from Products.PluginIndexes.KeywordIndex.KeywordIndex import KeywordIndex
from Products.PluginIndexes.CompositeIndex.CompositeIndex import CompositeIndex
from Products.PluginIndexes.interfaces import ILimitedResultIndex

import sys
import logging

logger = logging.getLogger('zope.testCompositeIndex')

states = ['published', 'pending', 'private', 'intranet']
types = ['Document', 'News', 'File', 'Image']
default_pages = [True, False, False, False, False, False]
keywords = map(lambda x: 'subject_%s' % x, range(6))


class TestObject(object):

    def __init__(self, id, portal_type, review_state,
                 is_default_page=False, subject=[]):
        self.id = id
        self.portal_type = portal_type
        self.review_state = review_state
        self.is_default_page = is_default_page
        self.subject = subject

    def getPhysicalPath(self):
        return ['', self.id, ]

    def __repr__(self):
        return ('< %s, %s, %s, %s, %s >' %
                (self.id, self.portal_type, self.review_state,
                 self.is_default_page, self.subject))


class RandomTestObject(TestObject):

    def __init__(self, id):

        i = random.randint(0, len(types)-1)
        portal_type = types[i]

        i = random.randint(0, len(states)-1)
        review_state = states[i]

        i = random.randint(0, len(default_pages)-1)
        is_default_page = default_pages[i]

        subject = random.sample(keywords, random.randint(1, len(keywords)))

        super(RandomTestObject, self).__init__(id, portal_type,
                                               review_state, is_default_page,
                                               subject)


class CompositeIndexTests(unittest.TestCase):

    def setUp(self):

        self._index = CompositeIndex('comp01',
                                     extra=[{'id': 'portal_type',
                                             'meta_type': 'FieldIndex',
                                             'attributes': ''},
                                            {'id': 'review_state',
                                             'meta_type': 'FieldIndex',
                                             'attributes': ''},
                                            {'id': 'is_default_page',
                                             'meta_type': 'FieldIndex',
                                             'attributes': ''},
                                            {'id': 'subject',
                                             'meta_type': 'KeywordIndex',
                                             'attributes': ''}
                                            ])

        self._field_indexes = (FieldIndex('review_state'),
                               FieldIndex('portal_type'),
                               FieldIndex('is_default_page'),
                               KeywordIndex('subject'))

    def _defaultSearch(self, req, expectedValues=None):

        rs = None
        for index in self._field_indexes:
            limit_result = ILimitedResultIndex.providedBy(index)
            if limit_result:
                r = index._apply_index(req, rs)
            else:
                r = index._apply_index(req)

            if r is not None:
                r, u = r
                w, rs = weightedIntersection(rs, r)
                if not rs:
                    break
        if not rs:
            return set()

        if hasattr(rs, 'keys'):
            rs = rs.keys()

        return set(rs)

    def _compositeSearch(self, req, expectedValues=None):
        query = self._index.make_query(req)
        rs = None
        r = self._index._apply_index(query)

        if r is not None:
            r, u = r
            w, rs = weightedIntersection(rs, r)
        if not rs:
            return set()

        if hasattr(rs, 'keys'):
            rs = rs.keys()

        return set(rs)

    def enableLog(self):
        logger.root.setLevel(logging.INFO)
        logger.root.addHandler(logging.StreamHandler(sys.stdout))

    def _populateIndexes(self, k, v):
        self._index.index_object(k, v)
        for index in self._field_indexes:
            index.index_object(k, v)

    def printIndexInfo(self):
        def info(index):
            size = index.indexSize()
            n_obj = index.numObjects()
            ratio = float(size)/float(n_obj)
            logger.info('<id: %s S: %s N: %s R: %.3f pm>' %
                   (index.id, size, n_obj, ratio*1000))

        info(self._index)
        for index in self._field_indexes:
            info(index)

    def _clearIndexes(self):
        self._index.clear()
        for index in self._field_indexes:
            index.clear()

    def testPerformance(self):
        self.enableLog()

        lengths = [10000, ]

        queries = [
            {'portal_type': {'query': 'Document'},
             'review_state': {'query': 'pending'}},
            {'portal_type': {'query': 'Document'},
             'subject': {'query': ['subject_1', 'subject_3']}},
            {'portal_type': {'query': 'Document'},
             'subject': {'query': 'subject_2'}},
            {'portal_type': {'query': 'Document'},
             'is_default_page': {'query': False}},
            {'review_state': {'query': 'pending'},
             'is_default_page': {'query': False}},
            {'portal_type': {'query': 'Document'},
             'review_state': {'query': 'pending'},
             'is_default_page': {'query': False}},
            {'portal_type': {'query': 'Document'},
             'review_state': {'query': 'pending'},
             'is_default_page': {'query': True}},
            {'portal_type': {'query': 'Document'},
             'review_state': {'query': 'pending'},
             'is_default_page': {'query': False},
             'subject': {'query': ['subject_2', 'subject_3'],
                         'operator': 'or'}},
            {'portal_type': {'query': 'Document'},
             'review_state': {'query': 'pending'},
             'is_default_page': {'query': True},
             'subject': {'query': ['subject_2', 'subject_3'],
                         'operator': 'or'}},
            ]

        def profileSearch(query, verbose=False):

            st = time()
            res1 = self._defaultSearch(query)
            duration1 = (time()-st)*1000
            if verbose:
                logger.info("atomic:    %s hits in %3.2fms" %
                            (len(res1), duration1))

            st = time()
            res2 = self._compositeSearch(query)
            duration2 = (time()-st)*1000
            if verbose:
                logger.info("composite: %s hits in %3.2fms" %
                            (len(res2), duration2))

            if verbose:
                logger.info('[composite/atomic] factor %3.2f\n' %
                            (duration1/duration2,))

            # composite search must be faster than default search
            assert duration2 < duration1

            # is result identical
            self.assertEqual(len(res1), len(res2))
            self.assertEqual(res1, res2)

        for l in lengths:
            self._clearIndexes()
            logger.info('************************************\n'
                        'indexing %s objects' % l)
            for i in range(l):
                name = '%s' % i
                obj = RandomTestObject(name)
                self._populateIndexes(i, obj)
            logger.info('indexing finished\n')
            self.printIndexInfo()
            logger.info('\nstart queries')
            for query in queries:
                logger.info("query %s" % query.keys())
                # warming up indexes
                profileSearch(query)
                # in memory measure
                profileSearch(query, verbose=True)
            logger.info('queries finished')

        logger.info('************************************')

    def testSearch(self):

        obj = TestObject('obj1', 'Document', 'pending', subject=('subject_1'))
        self._populateIndexes(1, obj)

        obj = TestObject('obj2', 'News', 'pending', subject=('subject_2'))
        self._populateIndexes(2, obj)

        obj = TestObject('obj3', 'News', 'visible',
                         subject=('subject_1', 'subject_2'))
        self._populateIndexes(3, obj)

        queries = [
            {'review_state': {'query': 'pending'},
             'portal_type': {'query': 'Document'}},
            {'review_state': {'query': ('pending', 'visible')},
             'portal_type': {'query': 'News'}},
            {'review_state': {'query': 'pending'},
             'portal_type': {'query': ('News', 'Document')}},
            {'review_state': {'query': ('pending', 'visible')},
             'portal_type': {'query': ('News', 'Document')},
             'is_default_page': {'query': False}},
            {'review_state': {'query': ('pending', 'visible')},
             'portal_type': {'query': ('News', 'Document')},
             'is_default_page': {'query': False},
             'subject': {'query': ('subject_1', 'subject_2'),
                         'operator': 'or'}}
            ]

        for query in queries:

            res1 = self._defaultSearch(query)
            res2 = self._compositeSearch(query)
            self.assertEqual(res1, res2)


def test_suite():
    return unittest.TestSuite((
        unittest.makeSuite(CompositeIndexTests),
        ))
