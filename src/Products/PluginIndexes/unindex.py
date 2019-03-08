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

from logging import getLogger
import sys

from Acquisition import (
    aq_inner,
    aq_parent,
    aq_get,
)
from BTrees.IIBTree import (
    difference,
    intersection,
    IITreeSet,
    IISet,
    multiunion,
)
from BTrees.IOBTree import IOBTree
from BTrees.Length import Length
from BTrees.OOBTree import OOBTree
from OFS.SimpleItem import SimpleItem
from ZODB.POSException import ConflictError
from zope.interface import implementer

from Products.PluginIndexes.cache import RequestCache
from Products.PluginIndexes.interfaces import (
    ILimitedResultIndex,
    IQueryIndex,
    ISortIndex,
    IUniqueValueIndex,
    IRequestCacheIndex,
)
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

    # Set to `False` in derived indexes that know that each
    #  document is indexed under at most 1 value
    #  as is e.g. the case for a `FieldIndex`.
    #  This makes "not/exclude" term searches more efficient
    potentially_multivalued = True

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
          uninded methods for this to work.

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
            # Prevent None from being indexed. None doesn't have a valid
            # ordering definition compared to any other object.
            # BTrees 4.0+ will throw a TypeError
            # "object has default comparison" and won't let it be indexed.
            return 0

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
                        LOG.error('Should not happen: oldDatum was there, '
                                  'now its not, for document: %s', documentId)

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
            LOG.debug('Attempt to unindex nonexistent document'
                      ' with id %s', documentId, exc_info=True)

    def _convert(self, value, default=None):
        return value

    def getRequestCache(self):
        """returns dict for caching per request for interim results
        of an index search. Returns 'None' if no REQUEST attribute
        is available"""

        cache = None
        REQUEST = aq_get(self, 'REQUEST', None)
        if REQUEST is not None:
            catalog = aq_parent(aq_parent(aq_inner(self)))
            if catalog is not None:
                # unique catalog identifier
                key = '_catalogcache_{0}_{1}'.format(
                    catalog.getId(), id(catalog))
                cache = REQUEST.get(key, None)
                if cache is None:
                    cache = REQUEST[key] = RequestCache()

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
        iid = '_{0}_{1}_{2}'.format(self.__class__.__name__,
                                    self.id, self.getCounter())
        return (iid, rid)

    def _apply_index(self, request, resultset=None):
        """Apply the index to query parameters given in the request arg.

        If the query does not match the index, return None, otherwise
        return a tuple of (result, used_attributes), where used_attributes
        is again a tuple with the names of all used data fields.
        """
        record = IndexQuery(request, self.id, self.query_options,
                            self.operators, self.useOperator)
        if record.keys is None:
            return None
        return (self.query_index(record, resultset=resultset), (self.id, ))

    def query_index(self, query, resultset=None):
        """Search the index with the given `IndexQuery` object *query*.

        If *resultset* is not `None`, then intersect the primary result
        with *resultset*.
        """
        primary_result = None
        # do we need this caching? It is promising only
        #  when the same subquery is performed at least twice
        #  in the same request
        #  On the other hand, it significantly complicates the code.
        cachekey = None
        cache = self.getRequestCache()
        if cache is not None:
            cachekey = self.getRequestCacheKey(query)
            primary_result = cache.get(cachekey)
        if primary_result is None:
            combiner_info = self.get_combiner_info(query)
            primary_result = combiner_info["sets"]
            if len(primary_result) > 1:
                operator = combiner_info["operator"]
                if operator == "or":
                    # Note: `len(resultset)` can be expensive
                    if resultset is not None and len(resultset) < 200:
                        # if *resultset* is small, intersecting first
                        # is more efficient then intersecting after
                        # the `multiunion`
                        # We cannot cache in this case
                        primary_result = [
                            intersection(resultset, s)
                            for s in primary_result]
                        resultset = None  # intersection already done
                        cachekey = None  # do not cache
                    primary_result = multiunion(primary_result),
                else:  # "and"
                    # For intersection, sort with smallest data set first
                    #   Note: "len" can be expensive
                    primary_result = sorted(primary_result, key=len)
            if primary_result:
                # handle "excludes"
                # Note: in the original, the unexcluded partial
                #   result was cached. This caches
                #   the excluded result
                excludes = combiner_info["exclude_sets"]
                if excludes:
                    excludes = multiunion(excludes)
                    primary_result = [
                        difference(s, excludes) for s in primary_result]
            if cachekey:  # we should cache
                cache[cachekey] = primary_result
        r = resultset if primary_result else None
        for s in primary_result:
            r = intersection(r, s)
        if r is None:
            r = IISet()
        return r

    # `ISimpleCombinerIndex`
    #  Such an index is characterized by the fact that its search result
    #  is the combination of a sequence of sets with
    #  a single `operator` (either `"and"` or `"or"`), optionally
    #  with some excluded sets specified by `exclude_sets`.
    def get_combiner_info(self, query):
        """preprocess *query* and return partial results.

        The result is a dict with keys `operator`, `sets` and `exclude_sets`.

        The search result is: "operator(*sets) - OR(*exclude_sets)".
        We do not perform this combination here to allow for
        outside optimizations.
        """
        index = self._index
        # Normalize
        normalize = self._convert
        keys = [normalize(k) for k in query.keys] or None
        not_keys = [normalize(k) for k in query.get("not", ())]
        # check for range
        opr = None
        range_param = query.get("range", None)
        if range_param is not None:
            opr = "range"
            opr_args = [tag for tag in ("min", "max") if tag in range_param]
        elif query.get("usage", None):  # another way to specify a range
            opr = query.usage.lower().split(":")
            opr, opr_args = opr[0], opr[1:]
        if opr == "range":
            lo = min(keys) if "min" in opr_args else None
            hi = max(keys) if "max" in opr_args else None
            keys = index.keys(lo, hi)  # Note: `keys` handles `None` correctly
        operator = query.operator
        result = dict(operator=operator, sets=[], exclude_sets=[])
        if keys is None:  # no keys have been specified
            if not_keys:  # pure not
                keys = index.keys()
            else:
                keys = ()

        # perform the lookups
        def lookup(operator, keys, result):
            for k in keys:
                try:
                    s = index.get(k)  # key of wrong type is not in the index
                except TypeError:
                    s = None
                if s is None:
                    if operator == "or":
                        continue  # missing `or` term
                    # missing `and` term -- result empty
                    result[:] = []
                    break
                elif isinstance(s, int):
                    # old style index
                    s = IISet((s,))
                result.append(s)
        lookup(operator,
               (k for k in keys if k not in not_keys),
               result["sets"]
               )
        if not_keys and self.potentially_multivalued and result["sets"]:
            lookup("or", not_keys, result["exclude_sets"])
        return result

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
