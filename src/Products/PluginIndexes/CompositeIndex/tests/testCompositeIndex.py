import unittest

import random

from time import time

from BTrees.IIBTree import weightedIntersection

from Products.PluginIndexes.FieldIndex.FieldIndex import FieldIndex
from Products.PluginIndexes.KeywordIndex.KeywordIndex import KeywordIndex
from Products.PluginIndexes.BooleanIndex.BooleanIndex import BooleanIndex
from Products.PluginIndexes.CompositeIndex.CompositeIndex import CompositeIndex
from Products.PluginIndexes.interfaces import ILimitedResultIndex

import sys
import logging

logger = logging.getLogger('zope.testCompositeIndex')

states = ['published', 'pending', 'private', 'intranet']
types = ['Document', 'News', 'File', 'Image']
default_pages = [True, False, False, False, False, False]
subjects = list(map(lambda x: 'subject_%s' % x, range(6)))
keywords = list(map(lambda x: 'keyword_%s' % x, range(6)))


class TestObject(object):

    def __init__(self, id, portal_type, review_state,
                 is_default_page=False, subject=(), keyword=()):
        self.id = id
        self.portal_type = portal_type
        self.review_state = review_state
        self.is_default_page = is_default_page
        self.subject = subject
        self.keyword = keyword

    def getPhysicalPath(self):
        return ['', self.id, ]

    def __repr__(self):
        return ('< %s, %s, %s, %s, %s , %s>' %
                (self.id, self.portal_type, self.review_state,
                 self.is_default_page, self.subject, self.keyword))


class RandomTestObject(TestObject):

    def __init__(self, id):

        i = random.randint(0, len(types) - 1)
        portal_type = types[i]

        i = random.randint(0, len(states) - 1)
        review_state = states[i]

        i = random.randint(0, len(default_pages) - 1)
        is_default_page = default_pages[i]

        subject = random.sample(subjects, random.randint(1, len(subjects)))
        keyword = random.sample(keywords, random.randint(1, len(keywords)))

        super(RandomTestObject, self).__init__(id, portal_type,
                                               review_state, is_default_page,
                                               subject, keyword)


# Pseudo ContentLayer class to support quick
# unit tests (skip performance tests)
class PseudoLayer(object):

    @classmethod
    def setUp(cls):
        pass

    @classmethod
    def tearDown(cls):
        pass


class CompositeIndexTestMixin(object):

    def setUp(self):
        self._indexes = [FieldIndex('review_state'),
                         FieldIndex('portal_type'),
                         BooleanIndex('is_default_page'),
                         KeywordIndex('subject',
                                      extra={
                                          'indexed_attrs':
                                          'keyword,subject'}
                                      ),
                         CompositeIndex('comp01',
                                        extra=[{'id': 'portal_type',
                                                'meta_type': 'FieldIndex',
                                                'attributes': ''},
                                               {'id': 'review_state',
                                                'meta_type': 'FieldIndex',
                                                'attributes': ''},
                                               {'id': 'is_default_page',
                                                'meta_type': 'BooleanIndex',
                                                'attributes': ''},
                                               {'id': 'subject',
                                                'meta_type': 'KeywordIndex',
                                                'attributes':
                                                'keyword,subject'}
                                               ])
                         ]

    def getIndex(self, name):
        for idx in self._indexes:
            if idx.id == name:
                return idx

    def defaultSearch(self, req, expectedValues=None, verbose=False):

        rs = None
        for index in self._indexes:
            st = time()
            duration = (time() - st) * 1000

            limit_result = ILimitedResultIndex.providedBy(index)
            if limit_result:
                r = index._apply_index(req, rs)
            else:
                r = index._apply_index(req)
            duration = (time() - st) * 1000

            if r is not None:
                r, u = r
                w, rs = weightedIntersection(rs, r)
                if not rs:
                    break

            if verbose and (index.id in req):
                logger.info("index %s: %s hits in %3.2fms" %
                            (index.id, r and len(r) or 0, duration))

        if not rs:
            return set()

        if hasattr(rs, 'keys'):
            rs = rs.keys()

        return set(rs)

    def compositeSearch(self, req, expectedValues=None, verbose=False):
        comp_index = self.getIndex('comp01')
        query = comp_index.make_query(req)

        # catch successful?
        self.assertTrue('comp01' in query)

        return self.defaultSearch(query,
                                  expectedValues=expectedValues,
                                  verbose=verbose)

    def enableLog(self):
        logger.root.setLevel(logging.INFO)
        logger.root.addHandler(logging.StreamHandler(sys.stdout))

    def populateIndexes(self, k, v):
        for index in self._indexes:
            index.index_object(k, v)

    def printIndexInfo(self):
        def info(index):
            size = index.indexSize()
            n_obj = index.numObjects()
            ratio = float(size) / float(n_obj)
            logger.info('<id: %15s unique keys: '
                        '%3s  length: %5s  ratio: %6.3f pm>' %
                        (index.id, size, n_obj, ratio * 1000))
            return ratio

        for index in self._indexes:
            info(index)

    def clearIndexes(self):
        for index in self._indexes:
            index.clear()


class CompositeIndexPerformanceTest(CompositeIndexTestMixin,
                                    unittest.TestCase):
    layer = PseudoLayer

    def testPerformance(self):
        self.enableLog()

        lengths = [10000, ]

        queries = [('query01_default_two_indexes',
                    {'portal_type': {'query': 'Document'},
                     'review_state': {'query': 'pending'}}),
                   ('query02_default_two_indexes',
                    {'portal_type': {'query': 'Document'},
                     'subject': {'query': 'subject_2'}}),
                   ('query02_default_two_indexes_zero_hits',
                    {'portal_type': {'query': 'Document'},
                     'subject': {'query': ['keyword_1', 'keyword_2']}}),
                   ('query03_default_two_indexes',
                    {'portal_type': {'query': 'Document'},
                     'subject': {'query': ['subject_1', 'subject_3']}}),
                   ('query04_default_two_indexes',
                    {'portal_type': {'query': 'Document'},
                     'is_default_page': {'query': False}}),
                   ('query05_default_two_indexes',
                    {'portal_type': {'query': 'Document'},
                     'is_default_page': {'query': True}}),
                   ('query06_default_two_indexes',
                    {'review_state': {'query': 'pending'},
                     'is_default_page': {'query': False}}),
                   ('query07_default_three_indexes',
                    {'portal_type': {'query': 'Document'},
                     'review_state': {'query': 'pending'},
                     'is_default_page': {'query': False}}),
                   ('query08_default_three_indexes',
                    {'portal_type': {'query': 'Document'},
                     'review_state': {'query': 'pending'},
                     'is_default_page': {'query': True}}),
                   ('query09_default_four_indexes',
                    {'portal_type': {'query': 'Document'},
                     'review_state': {'query': 'pending'},
                     'is_default_page': {'query': True},
                     'subject': {'query': ['subject_2', 'subject_3'],
                                 'operator': 'or'}}),
                   ('query10_and_operator_four_indexes',
                    {'portal_type': {'query': 'Document'},
                     'review_state': {'query': 'pending'},
                     'is_default_page': {'query': True},
                     'subject': {'query': ['subject_1', 'subject_3'],
                                 'operator': 'and'}}),
                   ('query11_and_operator_four_indexes',
                    {'portal_type': {'query': ('Document', 'News')},
                     'review_state': {'query': 'pending'},
                     'is_default_page': {'query': True},
                     'subject': {'query': ['subject_1', 'subject_3'],
                                 'operator': 'and'}}),
                   ('query12_not_operator_four_indexes',
                    {'portal_type': {'not': 'Document'},
                     'review_state': {'query': 'pending'},
                     'is_default_page': {'query': True},
                     'subject': {'query': ['subject_2', 'subject_3'],
                                 'operator': 'or'}}),
                   ('query13_not_operator_four_indexes',
                    {'portal_type': {'query': 'Document'},
                     'review_state': {'not': ('pending', 'visible')},
                     'is_default_page': {'query': True},
                     'subject': {'query': ['subject_2', 'subject_3']}}),
                   ]

        def profileSearch(query, warmup=False, verbose=False):

            st = time()
            res1 = self.defaultSearch(query, verbose=False)
            duration1 = (time() - st) * 1000

            if verbose:
                logger.info("atomic:    %s hits in %3.2fms" %
                            (len(res1), duration1))

            st = time()
            res2 = self.compositeSearch(query, verbose=False)
            duration2 = (time() - st) * 1000

            if verbose:
                logger.info("composite: %s hits in %3.2fms" %
                            (len(res2), duration2))

            if verbose:
                logger.info('[composite/atomic] factor %3.2f' %
                            (duration1 / duration2,))

            if not warmup:
                # if length of result is greater than zero composite
                # search must be roughly faster than default search
                if res1 and res2:
                    assert 0.5 * duration2 < duration1, (duration2, duration1)

            # is result identical?
            self.assertEqual(len(res1), len(res2), '%s != %s for %s' %
                             (len(res1), len(res2), query))
            self.assertEqual(res1, res2)

        for l in lengths:
            self.clearIndexes()
            logger.info('************************************\n'
                        'indexing %s objects' % l)

            for i in range(l):
                name = '%s' % i
                obj = RandomTestObject(name)
                self.populateIndexes(i, obj)

            logger.info('indexing finished\n')

            self.printIndexInfo()

            logger.info('\nstart queries')

            # warming up indexes
            logger.info("warming up indexes")
            for name, query in queries:
                profileSearch(query, warmup=True)

            # in memory measure
            logger.info("in memory measure")
            for name, query in queries:
                logger.info("\nquery: %s" % name)
                profileSearch(query, verbose=True)

            logger.info('\nqueries finished')

        logger.info('************************************')


class CompositeIndexTest(CompositeIndexTestMixin, unittest.TestCase):

    def testSearch(self):

        obj = TestObject('obj_1', 'Document', 'pending', subject=('subject_1'))
        obj = TestObject('obj_2', 'News', 'pending', subject=('subject_2'))
        obj = TestObject('obj_3', 'News', 'visible',
                         subject=('subject_1', 'subject_2'))
        obj = TestObject('obj_4', 'News', 'visible',
                         subject=('subject_1', 'subject_2'),
                         keyword=('keyword_1', ))
        self.populateIndexes(4, obj)

        queries = [{'review_state': {'query': 'pending'},
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
                                'operator': 'or'}},
                   {'review_state': {'query': ('pending', 'visible')},
                    'portal_type': {'query': ('News', 'Document')},
                    'is_default_page': {'query': False},
                    'subject': {'query': ('subject_1', 'subject_2'),
                                'operator': 'or'}},
                   {'review_state': {'query': ('pending', 'visible')},
                    'portal_type': {'query': ('News', 'Document')},
                    'is_default_page': {'query': False},
                    'subject': {'query': ('subject_1', 'subject_2'),
                                'operator': 'and'}},
                   {'review_state': {'not': ('pending', 'visible')},
                    'portal_type': {'query': ('News', 'Document')},
                    'is_default_page': {'query': False},
                    'subject': {'query': ('keyword_1',)}},
                   ]

        for query in queries:

            res1 = self.defaultSearch(query)
            res2 = self.compositeSearch(query)
            # is result identical?
            self.assertEqual(len(res1), len(res2), '%s != %s for %s' %
                             (len(res1), len(res2), query))
            self.assertEqual(res1, res2)
