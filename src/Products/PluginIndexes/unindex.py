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

import sys
from logging import getLogger
from weakref import WeakKeyDictionary

from Acquisition import aq_base
from Acquisition import aq_get
from Acquisition import aq_inner
from Acquisition import aq_parent
from BTrees.IIBTree import IISet
from BTrees.IIBTree import IITreeSet
from BTrees.IIBTree import difference
from BTrees.IIBTree import intersection
from BTrees.IIBTree import multiunion
from BTrees.IOBTree import IOBTree
from BTrees.Length import Length
from BTrees.OOBTree import OOBTree
from OFS.SimpleItem import SimpleItem
from ZODB.POSException import ConflictError
from zope.interface import implementer

from Products.PluginIndexes.cache import RequestCache
from Products.PluginIndexes.interfaces import ILimitedResultIndex
from Products.PluginIndexes.interfaces import IQueryIndex
from Products.PluginIndexes.interfaces import IRequestCacheIndex
from Products.PluginIndexes.interfaces import ISortIndex
from Products.PluginIndexes.interfaces import IUniqueValueIndex
from Products.PluginIndexes.util import safe_callable
from Products.ZCatalog.query import IndexQuery


_marker = []
LOG = getLogger('Zope.UnIndex')


@implementer(ILimitedResultIndex, IQueryIndex, IUniqueValueIndex,
             ISortIndex, IRequestCacheIndex)
class UnIndex(SimpleItem):
    """Simple forward and reverse index.
    """

    zmi_icon = 'fas fa-info-circle'
    _counter = None
    operators = ('or', 'and')
    useOperator = 'or'
    query_options = ()

    def __init__(self, id, ignore_ex=None, call_methods=None,
                 extra=None, caller=None):
        """Create an unindex

        UnIndexes are indexes that contain two index components, the
        forward index (like plain index objects) and an inverted
        index.  The inverted index is so that objects can be unindexed
        even when the old value of the object is not known.

        e.g.

        self._index = {datum:[documentId1, documentId2]}
        self._unindex = {documentId:datum}

        The arguments are:

          'id' -- the name of the item attribute to index.  This is
          either an attribute name or a record key.

          'ignore_ex' -- should be set to true if you want the index
          to ignore exceptions raised while indexing instead of
          propagating them.

          'call_methods' -- should be set to true if you want the index
          to call the attribute 'id' (note: 'id' should be callable!)
          You will also need to pass in an object in the index and
          unindex methods for this to work.

          'extra' -- a mapping object that keeps additional
          index-related parameters - subitem 'indexed_attrs'
          can be string with comma separated attribute names or
          a list

          'caller' -- reference to the calling object (usually
          a (Z)Catalog instance
        """

        def _get(o, k, default):
            """ return a value for a given key of a dict/record 'o' """
            if isinstance(o, dict):
                return o.get(k, default)
            else:
                return getattr(o, k, default)

        self.id = id
        self.ignore_ex = ignore_ex  # currently unimplemented
        self.call_methods = call_methods

        # allow index to index multiple attributes
        ia = _get(extra, 'indexed_attrs', id)
        if isinstance(ia, str):
            self.indexed_attrs = ia.split(',')
        else:
            self.indexed_attrs = list(ia)
        self.indexed_attrs = [
            attr.strip() for attr in self.indexed_attrs if attr]
        if not self.indexed_attrs:
            self.indexed_attrs = [id]

        self.clear()

    def __len__(self):
        return self._length()

    def getId(self):
        return self.id

    def clear(self):
        self._length = Length()
        self._index = OOBTree()
        self._unindex = IOBTree()

        if self._counter is None:
            self._counter = Length()
        else:
            self._increment_counter()

    def __nonzero__(self):
        return not not self._unindex

    def histogram(self):
        """Return a mapping which provides a histogram of the number of
        elements found at each point in the index.
        """
        histogram = {}
        for item in self._index.items():
            if isinstance(item, int):
                entry = 1  # "set" length is 1
            else:
                key, value = item
                entry = len(value)
            histogram[entry] = histogram.get(entry, 0) + 1
        return histogram

    def referencedObjects(self):
        """Generate a list of IDs for which we have referenced objects."""
        return self._unindex.keys()

    def getEntryForObject(self, documentId, default=_marker):
        """Takes a document ID and returns all the information we have
        on that specific object.
        """
        if default is _marker:
            return self._unindex.get(documentId)
        return self._unindex.get(documentId, default)

    def removeForwardIndexEntry(self, entry, documentId):
        """Take the entry provided and remove any reference to documentId
        in its entry in the index.
        """
        indexRow = self._index.get(entry, _marker)
        if indexRow is not _marker:
            try:
                indexRow.remove(documentId)
                if not indexRow:
                    del self._index[entry]
                    self._length.change(-1)
            except ConflictError:
                raise
            except AttributeError:
                # index row is an int
                try:
                    del self._index[entry]
                except KeyError:
                    # swallow KeyError because it was probably
                    # removed and then _length AttributeError raised
                    pass
                if isinstance(self.__len__, Length):
                    self._length = self.__len__
                    del self.__len__
                self._length.change(-1)
            except Exception:
                LOG.error('%(context)s: unindex_object could not remove '
                          'documentId %(doc_id)s from index %(index)r.  This '
                          'should not happen.', dict(
                              context=self.__class__.__name__,
                              doc_id=documentId,
                              index=self.id),
                          exc_info=sys.exc_info())
        else:
            LOG.error('%(context)s: unindex_object tried to '
                      'retrieve set %(entry)r from index %(index)r '
                      'but couldn\'t.  This should not happen.', dict(
                          context=self.__class__.__name__,
                          entry=entry,
                          index=self.id))

    def insertForwardIndexEntry(self, entry, documentId):
        """Take the entry provided and put it in the correct place
        in the forward index.

        This will also deal with creating the entire row if necessary.
        """
        indexRow = self._index.get(entry, _marker)

        # Make sure there's actually a row there already. If not, create
        # a set and stuff it in first.
        if indexRow is _marker:
            # We always use a set to avoid getting conflict errors on
            # multiple threads adding a new row at the same time
            self._index[entry] = IITreeSet((documentId, ))
            self._length.change(1)
        else:
            try:
                indexRow.insert(documentId)
            except AttributeError:
                # Inline migration: index row with one element was an int at
                # first (before Zope 2.13).
                indexRow = IITreeSet((indexRow, documentId))
                self._index[entry] = indexRow

    def index_object(self, documentId, obj, threshold=None):
        """ wrapper to handle indexing of multiple attributes """

        fields = self.getIndexSourceNames()
        res = 0
        for attr in fields:
            res += self._index_object(documentId, obj, threshold, attr)

        if res > 0:
            self._increment_counter()

        return res > 0

    def _index_object(self, documentId, obj, threshold=None, attr=''):
        """ index and object 'obj' with integer id 'documentId'"""
        returnStatus = 0

        # First we need to see if there's anything interesting to look at
        datum = self._get_object_datum(obj, attr)
        if datum is None:
            # Remove previous index if it exists
            oldDatum = self._unindex.get(documentId, _marker)
            if oldDatum:
                self.removeForwardIndexEntry(oldDatum, documentId)
                del self._unindex[documentId]
            return 0

        datum = self._convert(datum, default=_marker)

        # We don't want to do anything that we don't have to here, so we'll
        # check to see if the new and existing information is the same.
        oldDatum = self._unindex.get(documentId, _marker)
        if datum != oldDatum:
            if oldDatum is not _marker:
                self.removeForwardIndexEntry(oldDatum, documentId)
                if datum is _marker:
                    try:
                        del self._unindex[documentId]
                    except ConflictError:
                        raise
                    except Exception:
                        LOG.error('%(context)s: oldDatum was there, '
                                  'now it\'s not for documentId %(doc_id)s '
                                  'from index %(index)r.  This '
                                  'should not happen.', dict(
                                      context=self.__class__.__name__,
                                      doc_id=documentId,
                                      index=self.id),
                                  exc_info=sys.exc_info())

            if datum is not _marker:
                self.insertForwardIndexEntry(datum, documentId)
                self._unindex[documentId] = datum

            returnStatus = 1

        return returnStatus

    def _get_object_datum(self, obj, attr):
        # self.id is the name of the index, which is also the name of the
        # attribute we're interested in.  If the attribute is callable,
        # we'll do so.
        try:
            datum = getattr(obj, attr)
            if safe_callable(datum):
                datum = datum()
        except (AttributeError, TypeError):
            datum = _marker
        return datum

    def _increment_counter(self):
        if self._counter is None:
            self._counter = Length()
        self._counter.change(1)

    def getCounter(self):
        """Return a counter which is increased on index changes"""
        return self._counter is not None and self._counter() or 0

    def numObjects(self):
        """Return the number of indexed objects."""
        return len(self._unindex)

    def indexSize(self):
        """Return the size of the index in terms of distinct values."""
        return len(self)

    def unindex_object(self, documentId):
        """ Unindex the object with integer id 'documentId' and don't
        raise an exception if we fail
        """
        unindexRecord = self._unindex.get(documentId, _marker)
        if unindexRecord is _marker:
            return None

        self._increment_counter()

        self.removeForwardIndexEntry(unindexRecord, documentId)
        try:
            del self._unindex[documentId]
        except ConflictError:
            raise
        except Exception:
            LOG.debug('%(context)s: attempt to unindex nonexistent '
                      'documentId %(doc_id)s from index %(index)r. This '
                      'should not happen.', dict(
                          context=self.__class__.__name__,
                          doc_id=documentId,
                          index=self.id),
                      exc_info=True)

    def _apply_not(self, not_parm, resultset=None):
        index = self._index
        setlist = []
        for k in not_parm:
            s = index.get(k, None)
            if s is None:
                continue
            elif isinstance(s, int):
                s = IISet((s, ))
            setlist.append(s)
        return multiunion(setlist)

    def _convert(self, value, default=None):
        return value

    def getRequestCache(self):
        """returns dict for caching per request for interim results
        of an index search. Returns 'None' if no REQUEST attribute
        is available"""

        cache = None
        REQUEST = aq_get(self, 'REQUEST', None)
        if hasattr(REQUEST, "get"):
            cache_container = REQUEST.get("__catalog_cache__")
            if cache_container is None:
                # we use a `WeakKeyDictionary` (rather than the
                #   request directly) to avoid the type of problem
                #   described in
                #   "https://community.plone.org/t/potential-memory-corruption-during-migration-plone-4-2-5-2/11655/11"
                cache_container = REQUEST["__catalog_cache__"] \
                    = WeakKeyDictionary()
            # we use the parent (of type `Products.ZCatalog.Catalog.Catalog`)
            #  as key to facilitate invalidation via its method
            #  in the future
            catalog = aq_base(aq_parent(aq_inner(self)))
            if catalog is not None:
                cache = cache_container.get(catalog, None)
                if cache is None:
                    cache = cache_container[catalog] = RequestCache()

        return cache

    def getRequestCacheKey(self, record, resultset=None):
        """returns an unique key of a search record"""
        params = []

        # record operator (or, and)
        params.append(('operator', record.operator))

        # not / exclude operator
        not_value = record.get('not', None)
        if not_value is not None:
            not_value = frozenset(not_value)
            params.append(('not', not_value))

        # record options
        for op in ['range', 'usage']:
            op_value = record.get(op, None)
            if op_value is not None:
                params.append((op, op_value))

        # record keys
        rec_keys = frozenset(record.keys)
        params.append(('keys', rec_keys))

        # build record identifier
        rid = frozenset(params)

        # unique index identifier
        iid = '_{}_{}_{}'.format(self.__class__.__name__,
                                 self.id, self.getCounter())
        return (iid, rid)

    def _apply_index(self, request, resultset=None):
        """Apply the index to query parameters given in the request arg.

        If the query does not match the index, return None, otherwise
        return a tuple of (result, used_attributes), where used_attributes
        is again a tuple with the names of all used data fields.

        If not `None`, the resultset argument
        indicates that the search result is relevant only on this set,
        i.e. everything outside resultset is of no importance.
        The index can use this information for optimizations.
        """
        record = IndexQuery(request, self.id, self.query_options,
                            self.operators, self.useOperator)
        if record.keys is None:
            return None
        return (self.query_index(record, resultset=resultset), (self.id, ))

    def query_index(self, record, resultset=None):
        """Search the index with the given IndexQuery object.

        If not `None`, the resultset argument
        indicates that the search result is relevant only on this set,
        i.e. everything outside resultset is of no importance.
        The index can use this information for optimizations.
        """
        index = self._index
        r = None
        opr = None

        # not / exclude parameter
        not_parm = record.get('not', None)

        operator = record.operator

        cachekey = None
        cache = self.getRequestCache()
        if cache is not None:
            cachekey = self.getRequestCacheKey(record)
            if cachekey is not None:
                cached = None
                if operator == 'or':
                    cached = cache.get(cachekey, None)
                else:
                    cached_setlist = cache.get(cachekey, None)
                    if cached_setlist is not None:
                        r = resultset
                        for s in cached_setlist:
                            # the result is bound by the resultset
                            r = intersection(r, s)
                            # If intersection, we can't possibly get a
                            # smaller result
                            if not r:
                                break
                        cached = r

                if cached is not None:
                    if isinstance(cached, int):
                        cached = IISet((cached, ))

                    if not_parm:
                        not_parm = list(map(self._convert, not_parm))
                        exclude = self._apply_not(not_parm, resultset)
                        cached = difference(cached, exclude)

                    return cached

        # Range parameter
        range_parm = record.get('range', None)
        if range_parm:
            opr = 'range'
            opr_args = []
            if range_parm.find('min') > -1:
                opr_args.append('min')
            if range_parm.find('max') > -1:
                opr_args.append('max')

        if record.get('usage', None):
            # see if any usage params are sent to field
            opr = record.usage.lower().split(':')
            opr, opr_args = opr[0], opr[1:]

        # not query
        if not record.keys and not_parm:
            # convert into indexed format
            not_parm = list(map(self._convert, not_parm))
            # we have only a 'not' query
            # shortcut/optimization if we have no 'opr' (i.e. no range)
            if resultset is not None and opr is None:
                i_not_parm = self._apply_not(not_parm, resultset)
                if i_not_parm:
                    return difference(resultset, i_not_parm)
            record.keys = list(index)
            for parm in not_parm:
                try:
                    record.keys.remove(parm)
                except ValueError:
                    pass
        else:
            # convert query arguments into indexed format
            record.keys = list(map(self._convert, record.keys))

        if opr == 'range':  # range search
            if 'min' in opr_args:
                lo = min(record.keys)
            else:
                lo = None
            if 'max' in opr_args:
                hi = max(record.keys)
            else:
                hi = None
            if hi:
                setlist = index.values(lo, hi)
            else:
                setlist = index.values(lo)

            # If we only use one key, intersect and return immediately
            if len(setlist) == 1:
                result = setlist[0]
                if isinstance(result, int):
                    result = IISet((result,))

                if cachekey is not None:
                    if operator == 'or':
                        cache[cachekey] = result
                    else:
                        cache[cachekey] = [result]

                if not_parm:
                    exclude = self._apply_not(not_parm, resultset)
                    result = difference(result, exclude)
                return result

            if operator == 'or':
                tmp = []
                for s in setlist:
                    if isinstance(s, int):
                        s = IISet((s,))
                    tmp.append(s)
                r = multiunion(tmp)

                if cachekey is not None:
                    cache[cachekey] = r
            else:
                # For intersection, sort with smallest data set first
                tmp = []
                for s in setlist:
                    if isinstance(s, int):
                        s = IISet((s,))
                    tmp.append(s)
                if len(tmp) > 2:
                    setlist = sorted(tmp, key=len)
                else:
                    setlist = tmp

                # 'r' is not invariant of resultset. Thus, we
                # have to remember 'setlist'
                if cachekey is not None:
                    cache[cachekey] = setlist

                r = resultset
                for s in setlist:
                    # the result is bound by the resultset
                    r = intersection(r, s)
                    # If intersection, we can't possibly get a smaller result
                    if not r:
                        break

        else:  # not a range search
            # Filter duplicates
            setlist = []
            for k in record.keys:
                if k is None:
                    # Prevent None from being looked up. None doesn't
                    # have a valid ordering definition compared to any
                    # other object. BTrees 4.0+ will throw a TypeError
                    # "object has default comparison".
                    continue
                try:
                    s = index.get(k, None)
                except TypeError:
                    # key is not valid for this Btree so the value is None
                    LOG.error(
                        '%(context)s: query_index tried '
                        'to look up key %(key)r from index %(index)r '
                        'but key was of the wrong type.', dict(
                            context=self.__class__.__name__,
                            key=k,
                            index=self.id,
                        )
                    )
                    s = None
                # If None, try to bail early
                if s is None:
                    if operator == 'or':
                        # If union, we can possibly get a bigger result
                        continue
                    # If intersection, we can't possibly get a smaller result
                    if cachekey is not None:
                        # If operator is 'and', we have to cache a list of
                        # IISet objects
                        cache[cachekey] = [IISet()]
                    return IISet()
                elif isinstance(s, int):
                    s = IISet((s,))
                setlist.append(s)

            # If we only use one key return immediately
            if len(setlist) == 1:
                result = setlist[0]
                if isinstance(result, int):
                    result = IISet((result,))

                if cachekey is not None:
                    if operator == 'or':
                        cache[cachekey] = result
                    else:
                        cache[cachekey] = [result]

                if not_parm:
                    exclude = self._apply_not(not_parm, resultset)
                    result = difference(result, exclude)
                return result

            if operator == 'or':
                # If we already get a small result set passed in, intersecting
                # the various indexes with it and doing the union later is
                # faster than creating a multiunion first.

                if resultset is not None and len(resultset) < 200:
                    smalllist = []
                    for s in setlist:
                        smalllist.append(intersection(resultset, s))
                    r = multiunion(smalllist)

                    # 'r' is not invariant of resultset.  Thus, we
                    # have to remember the union of 'setlist'. But
                    # this is maybe a performance killer. So we do not cache.
                    # if cachekey is not None:
                    #    cache[cachekey] = multiunion(setlist)

                else:
                    r = multiunion(setlist)
                    if cachekey is not None:
                        cache[cachekey] = r
            else:
                # For intersection, sort with smallest data set first
                if len(setlist) > 2:
                    setlist = sorted(setlist, key=len)

                # 'r' is not invariant of resultset. Thus, we
                # have to remember the union of 'setlist'
                if cachekey is not None:
                    cache[cachekey] = setlist

                r = resultset
                for s in setlist:
                    r = intersection(r, s)
                    # If intersection, we can't possibly get a smaller result
                    if not r:
                        break

        if isinstance(r, int):
            r = IISet((r, ))
        if r is None:
            return IISet()
        if not_parm:
            exclude = self._apply_not(not_parm, resultset)
            r = difference(r, exclude)
        return r

    def hasUniqueValuesFor(self, name):
        """has unique values for column name"""
        if name == self.id:
            return 1
        return 0

    def getIndexSourceNames(self):
        """Return sequence of indexed attributes."""
        return getattr(self, 'indexed_attrs', [self.id])

    def getIndexQueryNames(self):
        """Indicate that this index applies to queries for the index's name."""
        return (self.id,)

    def uniqueValues(self, name=None, withLengths=0):
        """returns the unique values for name

        if withLengths is true, returns a sequence of
        tuples of (value, length)
        """
        if name is None:
            name = self.id
        elif name != self.id:
            return

        if not withLengths:
            for key in self._index.keys():
                yield key
        else:
            for key, value in self._index.items():
                if isinstance(value, int):
                    yield (key, 1)
                else:
                    yield (key, len(value))

    def keyForDocument(self, id):
        # This method is superseded by documentToKeyMap
        return self._unindex[id]

    def documentToKeyMap(self):
        return self._unindex

    def items(self):
        items = []
        for k, v in self._index.items():
            if isinstance(v, int):
                v = IISet((v,))
            items.append((k, v))
        return items
