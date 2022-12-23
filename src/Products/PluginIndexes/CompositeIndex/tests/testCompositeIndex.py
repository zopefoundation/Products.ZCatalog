import logging
import random
import sys
import unittest
from time import time

from BTrees.IIBTree import weightedIntersection

from Products.PluginIndexes.BooleanIndex.BooleanIndex import BooleanIndex
from Products.PluginIndexes.CompositeIndex.CompositeIndex import CompositeIndex
from Products.PluginIndexes.FieldIndex.FieldIndex import FieldIndex
from Products.PluginIndexes.interfaces import ILimitedResultIndex
from Products.PluginIndexes.KeywordIndex.KeywordIndex import KeywordIndex


logger = logging.getLogger('zope.testCompositeIndex')

states = ['published', 'pending', 'private', 'intranet']
types = ['Document', 'News', 'File', 'Image']
default_pages = [True, False, False, False, False, False]
subjects = list(map(lambda x: f'subject_{x}', range(6)))
keywords = list(map(lambda x: f'keyword_{x}', range(6)))


class TestObject:

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
        return ('< {id}, {portal_type}, {review_state},\
        {is_default_page}, {subject} , {keyword}>'.format(
            id=self.id,
            portal_type=self.portal_type,
            review_state=self.review_state,
            is_default_page=self.is_default_page,
            subject=self.subject,
            keyword=self.keyword))


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

        super().__init__(id, portal_type,
                         review_state, is_default_page,
                         subject, keyword)


# Pseudo ContentLayer class to support quick
# unit tests (skip performance tests)
class PseudoLayer:

    @classmethod
    def setUp(cls):
        pass

    @classmethod
    def tearDown(cls):
        pass


class CompositeIndexTestMixin:

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
                logger.info('index %s: %s hits in %3.2fms',
                            index.id, r and len(r) or 0, duration)

        if not rs:
            return set()

        try:
            rs = rs.keys()
        except AttributeError:
            pass

        return set(rs)

    def compositeSearch(self, req, expectedValues=None, verbose=False):
        comp_index = self.getIndex('comp01')
        query = comp_index.make_query(req)

        # catch successful?
        self.assertIn('comp01', query)

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
                        '%3s  length: %5s  ratio: %6.3f pm>',
                        index.id, size, n_obj, ratio * 1000)
            return ratio

        for index in self._indexes:
            info(index)

    def clearIndexes(self):
        for index in self._indexes:
            index.clear()


class CompositeIndexPerformanceTest(CompositeIndexTestMixin,
                                    unittest.TestCase):
    layer = PseudoLayer

    @unittest.skipIf(
        sys.platform.startswith('win'),
        'Time() is not well resolved in Windows.'
        ' Use time.perf_count()')
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
                logger.info('atomic:    %s hits in %3.2fms',
                            len(res1), duration1)

            st = time()
            res2 = self.compositeSearch(query, verbose=False)
            duration2 = (time() - st) * 1000

            if verbose:
                logger.info('composite: %s hits in %3.2fms',
                            len(res2), duration2)

            if verbose:
                logger.info('[composite/atomic] factor %3.2f',
                            duration1 / duration2,)

            if not warmup:
                # if length of result is greater than zero composite
                # search must be roughly faster than default search
                if res1 and res2:
                    self.assertLess(
                        0.4 * duration2,
                        duration1,
                        (duration2, duration1, query))

            # is result identical?
            self.assertEqual(len(res1), len(res2), '{} != {} for {}'.format(
                             len(res1), len(res2), query))
            self.assertEqual(res1, res2)

        for length in lengths:
            self.clearIndexes()
            logger.info('************************************\n'
                        'indexing %s objects', length)

            for i in range(length):
                name = str(i)
                obj = RandomTestObject(name)
                self.populateIndexes(i, obj)

            logger.info('indexing finished\n')

            self.printIndexInfo()

            logger.info('\nstart queries')

            # warming up indexes
            logger.info('warming up indexes')
            for name, query in queries:
                profileSearch(query, warmup=True)

            # in memory measure
            logger.info('in memory measure')
            for name, query in queries:
                logger.info('\nquery: %s', name)
                profileSearch(query, verbose=True)

            logger.info('\nqueries finished')

        logger.info('************************************')


class CompositeIndexTest(CompositeIndexTestMixin, unittest.TestCase):

    def testSearch(self):

        obj = TestObject('obj_1', 'Document', 'pending', subject=('subject_1'))
        self.populateIndexes(1, obj)
        obj = TestObject('obj_2', 'News', 'pending', subject=('subject_2'))
        self.populateIndexes(2, obj)
        obj = TestObject('obj_3', 'News', 'visible',
                         subject=('subject_1', 'subject_2'))
        self.populateIndexes(3, obj)
        obj = TestObject('obj_4', 'Event', 'private',
                         subject=('subject_1', 'subject_2'),
                         keyword=('keyword_1', ))
        self.populateIndexes(4, obj)

        queries = [
            # query on two attributes
            {'review_state': {'query': 'pending'},
             'portal_type': {'query': 'Document'}},
            # query on two attributes with 'or' operator
            {'review_state': {'query': ('pending', 'visible')},
             'portal_type': {'query': 'News'}},
            # query on two attributeswith one 'not' operator
            {'review_state': {'query': ('pending', 'visible')},
             'subject': {'query': 'subject_1', 'not': 'subject_2'}},
            # query on two attributes with 'or' operator
            {'review_state': {'query': 'pending'},
             'portal_type': {'query': ('News', 'Document')}},
            # query on three attributes
            {'review_state': {'query': ('pending', 'visible')},
             'portal_type': {'query': ('News', 'Document')},
             'is_default_page': {'query': False}},
            # query on four attributes with explicit 'or' operator
            {'review_state': {'query': ('pending', 'visible')},
             'portal_type': {'query': ('News', 'Document')},
             'is_default_page': {'query': False},
             'subject': {'query': ('subject_1', 'subject_2'),
                         'operator': 'or'}},
            # query on four attributes with explicit 'or' operator
            {'review_state': {'query': ('pending', 'visible')},
             'portal_type': {'query': ('News', 'Document')},
             'is_default_page': {'query': False},
             'subject': {'query': ('subject_1', 'subject_2'),
                         'operator': 'or'}},
            # query on four attributes with with explicit 'and' operator
            {'review_state': {'query': ('pending', 'visible')},
             'portal_type': {'query': ('News', 'Document')},
             'is_default_page': {'query': False},
             'subject': {'query': ('subject_1', 'subject_2'),
                         'operator': 'and'}},
            # query on five attributes with
            {'review_state': {'not': ('pending', 'visible')},
             'portal_type': {'query': ('News', 'Document')},
             'is_default_page': {'query': False},
             'subject': {'query': ('subject_1', )},
             'keyword': {'query': ('keyword_1',)}},
        ]

        for query in queries:
            res1 = self.defaultSearch(query)
            res2 = self.compositeSearch(query)
            # is result identical?
            self.assertEqual(len(res1), len(res2), '{} != {} for {}'.format(
                len(res1), len(res2), query))

            self.assertEqual(res1, res2)

    def testMakeQuery(self):

        ci = CompositeIndex(
            'ci',
            extra=[
                dict(id='fi', meta_type='FieldIndex', attributes=('fi',)),
                dict(id='ki', meta_type='KeywordIndex', attributes=('ki',)),
                dict(id='bi', meta_type='BooleanIndex', attributes=('bi',)),
            ]
        )

        # avoid premature return of `make_query`
        ci._length.change(1)

        #
        # 'range' parameter not supported
        query = dict(fi=dict(query=(1, 3), range='min:max'),
                     ki=dict(query=(10, 11)))

        result = ci.make_query(query)

        # should return original query
        self.assertEqual(result, query)
        #

        # 'and' operator not supported
        query = dict(fi=dict(query=(1, 3)),
                     ki=dict(query=(10, 11), operator='and'))

        result = ci.make_query(query)

        # should return original query
        self.assertEqual(result, query)

        #
        # regular query
        query = dict(fi=dict(query=(1, 2, 3)),
                     ki=dict(query=(10, 11)))

        result = ci.make_query(query)

        expect = {'ci': {'query': ((('fi', 1), ('ki', 10)),
                                   (('fi', 1), ('ki', 11)),
                                   (('fi', 2), ('ki', 10)),
                                   (('fi', 2), ('ki', 11)),
                                   (('fi', 3), ('ki', 10)),
                                   (('fi', 3), ('ki', 11)))}}

        self.assertEqual(set(result['ci']['query']),
                         set(expect['ci']['query']))

        #
        # 'not' parameter in query
        query = dict(fi=dict(query=(1, 2, 3)),
                     ki={'query': (10, 11), 'not': 15})

        result = ci.make_query(query)

        expect = {'ci': {'not': ((('fi', 1), ('ki', 15)),
                                 (('fi', 2), ('ki', 15)),
                                 (('fi', 3), ('ki', 15))),
                         'query': ((('fi', 1), ('ki', 10)),
                                   (('fi', 1), ('ki', 11)),
                                   (('fi', 2), ('ki', 10)),
                                   (('fi', 2), ('ki', 11)),
                                   (('fi', 3), ('ki', 10)),
                                   (('fi', 3), ('ki', 11)))}}

        # sequence of result is not deterministic.
        # Therefore, we use type 'set' for comparison.
        self.assertEqual(set(result['ci']['not']),
                         set(expect['ci']['not']))
        self.assertEqual(set(result['ci']['query']),
                         set(expect['ci']['query']))

        #
        # 'pure not' is not supported. The affected attribute cannot
        # be used to optimize the query.

        # In the case of two attributes the query cannot be optimized.
        query = dict(fi=dict(query=(1, 2, 3)),
                     ki={'not': (15, 16)})

        result = ci.make_query(query)

        # should return original query
        self.assertEqual(result, query)

        # In the case of three attributes only two attributes can
        # be used to optimize the query.
        query = dict(fi={'query': (1, 2, 3)},
                     ki={'not': (15, 16)},
                     bi={'query': (True,)})

        result = ci.make_query(query)

        expect = {'ci': {'query': ((('fi', 1), ('bi', 1)),
                                   (('fi', 2), ('bi', 1)),
                                   (('fi', 3), ('bi', 1)))},
                  'ki': {'not': (15, 16)}}

        # sequence of result is not deterministic.
        # Therefore, we use type 'set' for comparison.
        self.assertEqual(set(result['ki']['not']),
                         set(expect['ki']['not']))
        self.assertEqual(set(result['ci']['query']),
                         set(expect['ci']['query']))

        #
        # 'pure not' query on any attribute is also not supported
        query = dict(fi={'not': 1},
                     ki={'not': (15, 16)})

        result = ci.make_query(query)

        # should return original query
        self.assertEqual(result, query)
