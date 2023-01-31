##############################################################################
#
# Copyright (c) 2010 Zope Foundation and Contributors.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE
#
##############################################################################

import os
import os.path
import time
from _thread import allocate_lock
from collections import namedtuple
from logging import getLogger
from os import environ

from Acquisition import aq_base
from Acquisition import aq_parent
from zope.dottedname.resolve import resolve

from Products.PluginIndexes.interfaces import IDateRangeIndex
from Products.PluginIndexes.interfaces import ILimitedResultIndex
from Products.PluginIndexes.interfaces import IUniqueValueIndex


MAX_DISTINCT_VALUES = 10
REFRESH_RATE = 100
VALUE_INDEX_KEY = 'VALUE_INDEXES'

Duration = namedtuple('Duration', ['start', 'end'])
IndexMeasurement = namedtuple('IndexMeasurement',
                              ['name', 'duration', 'limit'])
Benchmark = namedtuple('Benchmark', ['duration', 'hits', 'limit'])
RecentQuery = namedtuple('RecentQuery', ['duration', 'details'])
Report = namedtuple('Report', ['hits', 'duration', 'last'])

logger = getLogger('Products.ZCatalog')


class NestedDict:
    """Holds a structure of two nested dicts."""

    @classmethod
    def get(cls, key):
        outer = cls.value.get(key, None)
        if outer is None:
            cls.set(key, {})
            outer = cls.value[key]
        return outer

    @classmethod
    def set(cls, key, value):
        with cls.lock:
            cls.value[key] = value

    @classmethod
    def clear(cls):
        with cls.lock:
            cls.value = {}

    @classmethod
    def get_entry(cls, key, key2):
        outer = cls.get(key)
        inner = outer.get(key2, None)
        if inner is None:
            cls.set_entry(key, key2, {})
            inner = outer.get(key2)
        return inner

    @classmethod
    def set_entry(cls, key, key2, value):
        outer = cls.get(key)
        with cls.lock:
            outer[key2] = value

    @classmethod
    def clear_entry(cls, key):
        cls.set(key, {})


class PriorityMap(NestedDict):
    """This holds a structure of nested dicts.

    The outer dict is a mapping of catalog id to plans. The inner dict holds
    a query key to Benchmark mapping.
    """

    lock = allocate_lock()
    value = {}

    @classmethod
    def get_value(cls):
        return cls.value.copy()

    @classmethod
    def load_default(cls):
        location = environ.get('ZCATALOGQUERYPLAN')
        if location:
            try:
                pmap = resolve(location)
                cls.load_pmap(location, pmap)
            except ImportError:
                logger.warning(f'could not load priority map from {location}')

    @classmethod
    def load_from_path(cls, path):
        path = os.path.abspath(path)
        _globals = {}
        _locals = {}

        with open(path, 'rb') as fd:
            exec(fd.read(), _globals, _locals)

        pmap = _locals['queryplan'].copy()
        cls.load_pmap(path, pmap)

    @classmethod
    def load_pmap(cls, location, pmap):
        logger.info('loaded priority %d map(s) from %s',
                    len(pmap), location)
        # Convert the simple benchmark tuples to namedtuples
        new_plan = {}
        for cid, plan in pmap.items():
            new_plan[cid] = {}
            for querykey, details in plan.items():
                new_plan[cid][querykey] = {}
                if isinstance(details, (frozenset, set)):
                    new_plan[cid][querykey] = details
                else:
                    for indexname, benchmark in details.items():
                        new_plan[cid][querykey][indexname] = \
                            Benchmark(*benchmark)
        with cls.lock:
            cls.value = new_plan


class Reports(NestedDict):
    """This holds a structure of nested dicts.

    The outer dict is a mapping of catalog id to reports. The inner dict holds
    a query key to Report mapping.
    """

    lock = allocate_lock()
    value = {}


class CatalogPlan:
    """Catalog plan class to measure and identify catalog queries and plan
    their execution.
    """

    def __init__(self, catalog, query=None, threshold=0.1):
        self.catalog = catalog
        self.cid = self.get_id()
        querykey_to_index = {}
        for index in self.catalog.indexes.values():
            for querykey in self.catalog._get_index_query_names(index):
                querykey_to_index[querykey] = index.getId()
        self.querykey_to_index = querykey_to_index
        self.query = query
        self.key = self.make_key(query)
        self.benchmark = {}
        self.threshold = threshold
        self.init_timer()

    def get_id(self):
        parent = aq_parent(self.catalog)
        path = getattr(aq_base(parent), 'getPhysicalPath', None)
        if path is None:
            path = ('', 'NonPersistentCatalog')
        else:
            path = tuple(parent.getPhysicalPath())
        return path

    def init_timer(self):
        self.res = []
        self.start_time = None
        self.interim = {}
        self.stop_time = None
        self.duration = None

    def valueindexes(self):
        indexes = self.catalog.indexes

        # This function determines all indexes whose values should be respected
        # in the report key. The number of unique values for the index needs to
        # be lower than the MAX_DISTINCT_VALUES watermark.

        # Ideally who would only consider those indexes with a small
        # number of unique values, where the number of items for each value
        # differs a lot. If the number of items per value is similar, the
        # duration of a query is likely similar as well. However, calculating
        # all the value indexes with the number of items per value is
        # quite slow. Therefore, we do not make this distinction.
        value_indexes = PriorityMap.get_entry(self.cid, VALUE_INDEX_KEY)
        if isinstance(value_indexes, (frozenset, set)):
            # Since this is an optimization only, slightly outdated results
            # based on index changes in the running process can be ignored.
            return value_indexes

        value_indexes = set()
        for name, index in indexes.items():
            if IUniqueValueIndex.providedBy(index):

                # DateRangeIndex is unsuitable for this purpose
                if IDateRangeIndex.providedBy(index):
                    continue

                # the size of an UniqueValueIndex is typically equal to the
                # number of unique values
                isize = index.indexSize()
                if isize >= MAX_DISTINCT_VALUES:
                    continue

                value_indexes.add(name)

        value_indexes = frozenset(value_indexes)
        PriorityMap.set_entry(self.cid, VALUE_INDEX_KEY, value_indexes)
        return value_indexes

    def make_key(self, query):
        if not query:
            return None

        valueindexes = self.valueindexes()
        key = keys = query.keys()

        values = [name for name in keys if name in valueindexes]
        if values:
            # If we have indexes whose values should be considered, we first
            # preserve all normal indexes and then add the keys whose values
            # matter including their value into the key
            key = [name for name in keys if name not in values]
            for name in values:
                v = query.get(name, [])
                # We need to make sure the key is immutable,
                # repr() is an easy way to do this without imposing
                # restrictions on the types of values.
                key.append((name, repr(v)))
        notkeys = [
            name for name in key
            if isinstance(query.get(name), dict) and "not" in query[name]
        ]
        if notkeys:
            key = [name for name in key if name not in notkeys]
            key.extend([(name, "not") for name in notkeys])
        # Workaround: Python only sorts on identical types.
        tuple_keys = set(key) - {
            x for x in key if not isinstance(x, tuple)}

        str_keys = set(key) - tuple_keys
        return tuple(sorted(str_keys)) + tuple(sorted(tuple_keys))

    def plan(self):
        benchmark = PriorityMap.get_entry(self.cid, self.key)
        if not benchmark:
            return None

        # sort indexes on (limited result index, mean search time)
        # skip internal ('#') bookkeeping records
        ranking = sorted(
            [((value.limit, value.duration), name)
             for name, value in benchmark.items() if '#' not in name])
        return [r[1] for r in ranking]

    def start(self):
        self.init_timer()
        self.start_time = time.time()

    def start_split(self, name):
        self.interim[name] = Duration(time.time(), None)

    def stop_split(self, name, result=None, limit=False):
        current = time.time()
        start_time, stop_time = self.interim.get(name, Duration(None, None))
        self.interim[name] = Duration(start_time, current)
        dt = current - start_time
        self.res.append(IndexMeasurement(
            name=name, duration=dt, limit=limit))

        if name.startswith('sort_on'):
            # sort_on isn't an index. We only do time reporting on it
            return

        # remember index's hits, search time and calls
        benchmark = self.benchmark
        if name not in benchmark:
            benchmark[name] = Benchmark(duration=dt,
                                        hits=1, limit=limit)
        else:
            duration, hits, limit = benchmark[name]
            duration = ((duration * hits) + dt) / float(hits + 1)
            # reset adaption
            if hits % REFRESH_RATE == 0:
                hits = 0
            hits += 1
            benchmark[name] = Benchmark(duration, hits, limit)

    def stop(self):
        self.end_time = time.time()
        self.duration = self.end_time - self.start_time
        # Make absolutely sure we never omit query keys from the plan
        current = PriorityMap.get_entry(self.cid, self.key)
        for key in self.query.keys():
            key = self.querykey_to_index.get(key, key)
            if key not in self.benchmark.keys():
                if current and key in current:
                    self.benchmark[key] = Benchmark(*current[key])
                else:
                    if key in self.catalog.indexes:
                        index = self.catalog.indexes[key]
                        self.benchmark[key] = Benchmark(
                            0, 0, ILimitedResultIndex.providedBy(index)
                        )
                    else:
                        self.benchmark[key] = Benchmark(0, 0, False)
        PriorityMap.set_entry(self.cid, self.key, self.benchmark)
        self.log()

    def log(self):
        # result of stopwatch
        total = self.duration
        if total < self.threshold:
            return

        key = self.key
        recent = RecentQuery(duration=total, details=self.res)

        previous = Reports.get_entry(self.cid, key)
        if previous:
            counter, mean, last = previous
            mean = (mean * counter + total) / float(counter + 1)
            Reports.set_entry(self.cid, key, Report(counter + 1, mean, recent))
        else:
            Reports.set_entry(self.cid, key, Report(1, total, recent))

    def reset(self):
        Reports.clear_entry(self.cid)

    def report(self):
        """Returns a statistic report of catalog queries as list of dicts.
        The duration is provided in millisecond.
        """
        rval = []
        for key, report in Reports.get(self.cid).items():
            last = report.last
            info = {
                'query': key,
                'counter': report.hits,
                'duration': report.duration * 1000,
                'last': {'duration': last.duration * 1000,
                         'details': [dict(id=d.name,
                                          duration=d.duration * 1000)
                                     for d in last.details],
                         },
            }
            rval.append(info)

        return rval


# Make sure we provide test isolation
from zope.testing.cleanup import addCleanUp  # NOQA


addCleanUp(PriorityMap.clear)
addCleanUp(Reports.clear)
del addCleanUp
