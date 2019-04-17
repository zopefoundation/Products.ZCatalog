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
import logging
import transaction

from Acquisition import aq_base
from Acquisition import aq_parent

from DateTime import DateTime
from Products.PluginIndexes.interfaces import IIndexCounter
from Products.PluginIndexes.interfaces import IDateRangeIndex
from Products.PluginIndexes.interfaces import IDateIndex

from plone.memoize import ram

from ZODB.utils import p64, z64
from ZODB.POSException import ReadConflictError
from functools import wraps
from six.moves._thread import allocate_lock

LOG = logging.getLogger('Zope.ZCatalog.cache')
_marker = (z64, z64)


class DontCache(Exception):
    pass


class CatalogQueryKey(object):
    def __init__(self, catalog, query=None):

        self.catalog = catalog
        self.cid = self.get_id()
        self.query = query

    def __call__(self):
        return self.key

    @property
    def key(self):
        return self.make_key(self.query)

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
            raise DontCache

        catalog = self.catalog

        # check high-water mark of catalog's zodb connection
        try:
            zodb_storage = catalog._p_jar._storage
            zodb_storage._start
        except AttributeError:
            raise DontCache

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
                    if (
                        not index._p_jar or zodb_storage is not
                        index._p_jar._storage
                    ):
                        # paranoid check; if the catalog and the indexes
                        # are not managed in the same storage, no
                        # transaction aware cache key can be generated.
                        raise DontCache

                    counter = index.getCounter()
                else:
                    # cache key invalidation cannot be supported if
                    # any index of query cannot be tested for updates
                    raise DontCache
            elif skip(name, value):
                # applying the query to indexes is invariant of
                # sort or pagination options
                continue
            else:
                # raise DontCache if query has a nonexistent index key
                raise DontCache

            if isinstance(value, dict):
                kvl = []
                for k, v in value.items():
                    v = self._convert_datum(index, v)
                    kvl.append((k, v))
                value = frozenset(kvl)

            else:
                value = self._convert_datum(index, value)

            keys.append((name, value, counter))

        key = frozenset(keys)
        query_key = (self.cid, key)
        return query_key

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


class QueryCacheManager(object):
    def __init__(self, cache=dict()):
        self.cache = cache

    def __call__(self, func):

        @wraps(func)
        def decorated(catalog, plan, query):
            try:
                query_key = CatalogQueryKey(catalog, query).key
            except DontCache:
                return func(catalog, plan, query)

            key = '{0}.{1}:{2}'.format(func.__module__,
                                       func.__name__, query_key)

            # convert key to 64 bit hash (not really required)
            oid = p64(hash(key) & ((1 << 64) - 1))

            tca = TransactionalCacheAdapter(catalog, self.cache)

            try:
                value = tca[oid]
                # HIT
            except KeyError:
                value = func(catalog, plan, query)
                tca[oid] = value
                # MISS & SET

            tca.registerAfterCommitHook()

            return value

        return decorated


class TransactionalCacheAdapter(object):
    """ """
    lock = allocate_lock()

    def __init__(self, instance, cache):

        self.cache_adapter = cache

        # get thread isolated local buffer/cache
        buffer_id = '_v_{0}_buffer'.format(self.__class__.__name__)
        try:
            self._cache = getattr(instance, buffer_id)
        except AttributeError:
            setattr(instance, buffer_id, {})
            self._cache = getattr(instance, buffer_id)

        # commit buffer
        self._uncommitted = {}

        self._zodb_storage = instance._p_jar._storage
        self._start = self._zodb_storage._start
        self._tid = z64
        self._registered = False

    def __getitem__(self, oid):
        """ """
        try:
            # try to get value from local or shared cache
            (_oid, serial, value) = self._load(oid)
        except KeyError:
            raise KeyError(oid)

        return value

    def __setitem__(self, oid, value):
        """ """
        self._cache[oid] = self._uncommitted[oid] = (oid, z64, value)

    def _load(self, oid):
        """ """

        if oid not in self._cache:
            try:
                (_oid, serial) = self.cache_adapter[oid]

                if _oid != z64 and _oid != oid:
                    # should never happen
                    raise ValueError

                if serial > self._start:
                    # we have a non-current revision of cache entry
                    raise ReadConflictError

                ckey = str(hash((_oid, serial)))
                value = self.cache_adapter[ckey]
                self._cache[oid] = (oid, serial, value)

                # cache HIT
                return self._cache[oid]

            except KeyError:
                # cache MISS
                raise

        (_oid, serial, value) = self._cache[oid]

        if serial > self._start:
            # we have a non-current revision of cache entry
            raise ReadConflictError

        return self._cache[oid]

    def _prepare(self):
        """
        """

        for oid in self._uncommitted:
            (_oid, serial) = self.cache_adapter.get(
                oid, default=_marker)

            if _oid != z64 and oid != _oid:
                # should never happen
                raise ValueError

            if serial > self._start:
                # should never happen; we have a non-current
                # revision of cache entry
                LOG.debug('{0}._prepare: non-current revision'
                          ' of cache entry detected'.format(
                              self.__class__.__name__))

                raise ReadConflictError

        # get lastTransaction of catalog's zodb connection
        self._tid = self._zodb_storage.lastTransaction()

    def _commit(self):
        """
        """

        # store uncommited values into shared cache
        for oid in self._uncommitted:
            (_oid, serial, value) = self._uncommitted[oid]

            self.cache_adapter[oid] = (oid, self._tid)

            ckey = str(hash((oid, self._tid)))
            self.cache_adapter[ckey] = value

        self._cleanup()

    def _abort(self):
        """
        """
        self._cleanup()

    def _cleanup(self):
        """
        """
        self._uncommitted.clear()
        self._cache.clear()

    def commit_hook(self, success, args=()):

        # if commit failed -> return
        if not success:
            self._abort()
            return

        # no new data to commit -> return
        if not self._uncommitted:
            self._cleanup()
            return

        try:
            with self.lock:
                self._prepare()
                self._commit()
        except Exception:
            self._abort()
            LOG.debug('Store of cache value failed')
            return

    def registerAfterCommitHook(self):
        """
        """
        if not self._registered:
            transaction.get().addAfterCommitHook(self.commit_hook)
            self._registered = True


# plone memoize marker
def _apply_query_plan_cachekey():
    pass


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

# cache decorator for catalog._apply_query_plan
cache = QueryCacheManager(cache=ram.store_in_cache(_apply_query_plan_cachekey))
