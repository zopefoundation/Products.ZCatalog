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

from Acquisition import aq_base
from Acquisition import aq_parent

from DateTime import DateTime
from plone.memoize.volatile import DontCache
from Products.PluginIndexes.interfaces import IIndexCounter
from Products.PluginIndexes.interfaces import IDateRangeIndex
from Products.PluginIndexes.interfaces import IDateIndex
from plone.memoize import ram


class CatalogCacheKey(object):
    def __init__(self, catalog, query=None):

        self.catalog = catalog
        self.cid = self.get_id()
        self.query = query
        self.key = self.make_key(query)

    def get_id(self):
        catalog = self.catalog
        parent = aq_parent(catalog)
        path = getattr(aq_base(parent), 'getPhysicalPath', None)
        if path is None:
            path = ('', 'NonPersistentCatalog')
        else:
            path = tuple(parent.getPhysicalPath())
        return path

    def make_key(self, query):

        if query is None:
            return None

        catalog = self.catalog

        def skip(name, value):
            if name in ['b_start', 'b_size']:
                return True
            elif catalog._get_sort_attr('on', {name: value}):
                return True
            elif catalog._get_sort_attr('limit', {name: value}):
                return True
            elif catalog._get_sort_attr('order', {name: value}):
                return True
            return False

        keys = []
        for name, value in query.items():
            if name in catalog.indexes:
                index = catalog.getIndex(name)
                if IIndexCounter.providedBy(index):
                    ck = index.getCounterKey()
                else:
                    # cache key invalidation cannot be supported if
                    # any index of query cannot be tested for changes
                    return None
            elif skip(name, value):
                # applying the query to indexes is invariant of
                # sort or pagination options
                continue
            else:
                # return None if query has a nonexistent index key
                return None

            if isinstance(value, dict):
                kvl = []
                for k, v in value.items():
                    v = self._convert_datum(index, v)
                    kvl.append((k, v))
                value = frozenset(kvl)

            else:
                value = self._convert_datum(index, value)

            keys.append((name, value, ck))

        key = frozenset(keys)
        cache_key = (self.cid, key)
        return cache_key

    def _convert_datum(self, index, value):

        def convert_datetime(dt):

            if IDateRangeIndex.providedBy(index):
                term = index._convertDateTime(dt)
            elif IDateIndex.providedBy(index):
                term = index._convert(dt)
            else:
                term = value

            return term

        if isinstance(value, (list, tuple)):
            res = []
            for v in value:
                if isinstance(v, DateTime):
                    v = convert_datetime(v)
                res.append(v)
            res.sort()

        elif isinstance(value, DateTime):
            res = convert_datetime(value)

        else:
            res = (value,)

        if not isinstance(res, tuple):
            res = tuple(res)

        return res


# plone memoize cache key
def _apply_query_plan_cachekey(method, catalog, plan, query):
    cc = CatalogCacheKey(catalog, query)
    if cc.key is None:
        raise DontCache
    return cc.key


def cache(fun):
    @ram.cache(_apply_query_plan_cachekey)
    def decorator(*args, **kwargs):
        return fun(*args, **kwargs)
    return decorator


# Make sure we provide test isolation, works only for ramcache
def _get_cache():
    cache_adapter = ram.store_in_cache(_apply_query_plan_cachekey)
    if hasattr(cache_adapter, 'ramcache'):
        return cache_adapter.ramcache
    else:
        raise AttributeError('Only ramcache supported for testing')


def _cache_clear():
    ram_cache = _get_cache()
    ram_cache.invalidateAll()

from zope.testing.cleanup import addCleanUp  # NOQA
addCleanUp(_cache_clear)
del addCleanUp
