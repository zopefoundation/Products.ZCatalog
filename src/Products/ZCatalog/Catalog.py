##############################################################################
#
# Copyright (c) 2002 Zope Foundation and Contributors.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE
#
##############################################################################

import types
import logging
import warnings
from bisect import bisect
from random import randint

import Acquisition
from Acquisition import aq_base
from Acquisition import aq_parent
import ExtensionClass
from Missing import MV
from Persistence import Persistent
from Products.PluginIndexes.interfaces import ILimitedResultIndex

import BTrees.Length
from BTrees.IIBTree import intersection, IISet
from BTrees.IIBTree import weightedIntersection
from BTrees.OIBTree import OIBTree
from BTrees.IOBTree import IOBTree
from Lazy import LazyMap, LazyCat, LazyValues
from CatalogBrains import AbstractCatalogBrain, NoBrainer
from .plan import CatalogPlan

LOG = logging.getLogger('Zope.ZCatalog')


def safe_callable(ob):
    # Works with ExtensionClasses and Acquisition.
    if hasattr(ob, '__class__'):
        return hasattr(ob, '__call__') or isinstance(ob, types.ClassType)
    else:
        return callable(ob)


class CatalogError(Exception):
    pass


class Catalog(Persistent, Acquisition.Implicit, ExtensionClass.Base):
    """ An Object Catalog

    An Object Catalog maintains a table of object metadata, and a
    series of manageable indexes to quickly search for objects
    (references in the metadata) that satisfy a search query.

    This class is not Zope specific, and can be used in any python
    program to build catalogs of objects.  Note that it does require
    the objects to be Persistent, and thus must be used with ZODB3.
    """

    _v_brains = NoBrainer

    def __init__(self, vocabulary=None, brains=None):
        # Catalogs no longer care about vocabularies and lexicons
        # so the vocabulary argument is ignored. (Casey)

        self.schema = {}    # mapping from attribute name to column number
        self.names = ()     # sequence of column names
        self.indexes = {}   # maping from index name to index object

        # The catalog maintains a BTree of object meta_data for
        # convenient display on result pages.  meta_data attributes
        # are turned into brain objects and returned by
        # searchResults.  The indexing machinery indexes all records
        # by an integer id (rid). self.data is a mapping from the
        # integer id to the meta_data, self.uids is a mapping of the
        # object unique identifier to the rid, and self.paths is a
        # mapping of the rid to the unique identifier.

        self.clear()

        if brains is not None:
            self._v_brains = brains

        self.updateBrains()

    def __len__(self):
        return self._length()

    def clear(self):
        """ clear catalog """

        self.data = IOBTree()  # mapping of rid to meta_data
        self.uids = OIBTree()  # mapping of uid to rid
        self.paths = IOBTree()  # mapping of rid to uid
        self._length = BTrees.Length.Length()

        for index in self.indexes.keys():
            self.getIndex(index).clear()

    def updateBrains(self):
        self.useBrains(self._v_brains)

    def __getitem__(self, index, ttype=type(())):
        """
        Returns instances of self._v_brains, or whatever is passed
        into self.useBrains.
        """
        if type(index) is ttype:
            # then it contains a score...
            normalized_score, score, key = index
            r=self._v_result_class(self.data[key]).__of__(aq_parent(self))
            r.data_record_id_ = key
            r.data_record_score_ = score
            r.data_record_normalized_score_ = normalized_score
        else:
            # otherwise no score, set all scores to 1
            r=self._v_result_class(self.data[index]).__of__(aq_parent(self))
            r.data_record_id_ = index
            r.data_record_score_ = 1
            r.data_record_normalized_score_ = 1
        return r

    def __setstate__(self, state):
        """ initialize your brains.  This method is called when the
        catalog is first activated (from the persistent storage) """
        Persistent.__setstate__(self, state)
        self.updateBrains()

    def useBrains(self, brains):
        """ Sets up the Catalog to return an object (ala ZTables) that
        is created on the fly from the tuple stored in the self.data
        Btree.
        """

        class mybrains(AbstractCatalogBrain, brains):
            pass

        scopy = self.schema.copy()

        scopy['data_record_id_']=len(self.schema.keys())
        scopy['data_record_score_']=len(self.schema.keys())+1
        scopy['data_record_normalized_score_']=len(self.schema.keys())+2

        mybrains.__record_schema__ = scopy

        self._v_brains = brains
        self._v_result_class = mybrains

    def addColumn(self, name, default_value=None):
        """
        adds a row to the meta data schema
        """

        schema = self.schema
        names = list(self.names)

        if name != name.strip():
            # Someone could have mistakenly added a space at the end
            # of the input field.
            LOG.warn("stripped space from new column %r -> %r", name,
                     name.strip())
            name = name.strip()

        if name in schema:
            raise CatalogError('The column %s already exists' % name)

        if name[0] == '_':
            raise CatalogError('Cannot cache fields beginning with "_"')

        values = schema.values()
        if values:
            schema[name] = max(values) + 1
        else:
            schema[name] = 0
        names.append(name)

        if default_value in (None, ''):
            default_value = MV

        for key, value in self.data.items():
            rec = list(value)
            rec.append(default_value)
            self.data[key] = tuple(rec)

        self.names = tuple(names)
        self.schema = schema

        # new column? update the brain
        self.updateBrains()

        self._p_changed = 1 # why?

    def delColumn(self, name):
        """
        deletes a row from the meta data schema
        """
        names = list(self.names)
        _index = names.index(name)

        if not name in self.schema:
            LOG.error('delColumn attempted to delete nonexistent '
                      'column %s.' % str(name))
            return

        del names[_index]

        # rebuild the schema
        i = 0
        schema = {}
        for name in names:
            schema[name] = i
            i = i + 1

        self.schema = schema
        self.names = tuple(names)

        # update the brain
        self.updateBrains()

        # remove the column value from each record
        for key, value in self.data.items():
            rec = list(value)
            del rec[_index]
            self.data[key] = tuple(rec)

    def addIndex(self, name, index_type):
        """Create a new index, given a name and a index_type.

        Old format: index_type was a string, 'FieldIndex' 'TextIndex' or
        'KeywordIndex' is no longer valid; the actual index must be
        instantiated and passed in to addIndex.

        New format: index_type is the actual index object to be stored.
        """

        if name in self.indexes:
            raise CatalogError('The index %s already exists' % name)

        if name.startswith('_'):
            raise CatalogError('Cannot index fields beginning with "_"')

        if not name:
            raise CatalogError('Name of index is empty')

        if name != name.strip():
            # Someone could have mistakenly added a space at the end
            # of the input field.
            LOG.warn("stripped space from new index %r -> %r", name,
                     name.strip())
            name = name.strip()

        indexes = self.indexes

        if isinstance(index_type, str):
            raise TypeError("Catalog addIndex now requires the index type to"
                            "be resolved prior to adding; create the proper "
                            "index in the caller.")

        indexes[name] = index_type
        self.indexes = indexes

    def delIndex(self, name):
        """ deletes an index """

        if not name in self.indexes:
            raise CatalogError('The index %s does not exist' % name)

        indexes = self.indexes
        del indexes[name]
        self.indexes = indexes

    def getIndex(self, name):
        """ get an index wrapped in the catalog """
        return self.indexes[name].__of__(self)

    def updateMetadata(self, object, uid, index):
        """ Given an object and a uid, update the column data for the
        uid with the object data iff the object has changed """
        data = self.data
        newDataRecord = self.recordify(object)

        if index is None:
            index = getattr(self, '_v_nextid', 0)
            if index % 4000 == 0:
                index = randint(-2000000000, 2000000000)
            while not data.insert(index, newDataRecord):
                index = randint(-2000000000, 2000000000)

            # We want ids to be somewhat random, but there are
            # advantages for having some ids generated
            # sequentially when many catalog updates are done at
            # once, such as when reindexing or bulk indexing.
            # We allocate ids sequentially using a volatile base,
            # so different threads get different bases. This
            # further reduces conflict and reduces churn in
            # here and it result sets when bulk indexing.
            self._v_nextid = index + 1
        else:
            if data.get(index, 0) != newDataRecord:
                data[index] = newDataRecord
        return index

    # the cataloging API

    def catalogObject(self, object, uid, threshold=None, idxs=None,
                      update_metadata=1):
        """
        Adds an object to the Catalog by iteratively applying it to
        all indexes.

        'object' is the object to be cataloged

        'uid' is the unique Catalog identifier for this object

        If 'idxs' is specified (as a sequence), apply the object only
        to the named indexes.

        If 'update_metadata' is true (the default), also update metadata for
        the object.  If the object is new to the catalog, this flag has
        no effect (metadata is always created for new objects).

        """

        if idxs is None:
            idxs = []

        index = self.uids.get(uid, None)

        if index is None:  # we are inserting new data
            index = self.updateMetadata(object, uid, None)
            self._length.change(1)
            self.uids[uid] = index
            self.paths[index] = uid

        elif update_metadata:  # we are updating and we need to update metadata
            self.updateMetadata(object, uid, index)

        # do indexing
        total = 0

        if idxs == []:
            use_indexes = self.indexes.keys()
        else:
            use_indexes = idxs

        for name in use_indexes:
            x = self.getIndex(name)
            if hasattr(x, 'index_object'):
                blah = x.index_object(index, object, threshold)
                total = total + blah
            else:
                LOG.error('catalogObject was passed bad index '
                          'object %s.' % str(x))

        return total

    def uncatalogObject(self, uid):
        """
        Uncatalog and object from the Catalog.  and 'uid' is a unique
        Catalog identifier

        Note, the uid must be the same as when the object was
        catalogued, otherwise it will not get removed from the catalog

        This method should not raise an exception if the uid cannot
        be found in the catalog.

        """
        data = self.data
        uids = self.uids
        paths = self.paths
        indexes = self.indexes.keys()
        rid = uids.get(uid, None)

        if rid is not None:
            for name in indexes:
                x = self.getIndex(name)
                if hasattr(x, 'unindex_object'):
                    x.unindex_object(rid)
            del data[rid]
            del paths[rid]
            del uids[uid]
            self._length.change(-1)

        else:
            LOG.error('uncatalogObject unsuccessfully '
                      'attempted to uncatalog an object '
                      'with a uid of %s. ' % str(uid))


    def uniqueValuesFor(self, name):
        """ return unique values for FieldIndex name """
        return self.getIndex(name).uniqueValues()

    def hasuid(self, uid):
        """ return the rid if catalog contains an object with uid """
        return self.uids.get(uid)

    def recordify(self, object):
        """ turns an object into a record tuple """
        record = []
        # the unique id is always the first element
        for x in self.names:
            attr = getattr(object, x, MV)
            if (attr is not MV and safe_callable(attr)):
                attr = attr()
            record.append(attr)
        return tuple(record)

    def instantiate(self, record):
        r = self._v_result_class(record[1])
        r.data_record_id_ = record[0]
        return r.__of__(self)

    def getMetadataForRID(self, rid):
        record = self.data[rid]
        result = {}
        for (key, pos) in self.schema.items():
            result[key] = record[pos]
        return result

    def getIndexDataForRID(self, rid):
        result = {}
        for name in self.indexes.keys():
            result[name] = self.getIndex(name).getEntryForObject(rid, "")
        return result

    # This is the Catalog search engine. Most of the heavy lifting happens
    # below

    def make_query(self, request):
        # This is a bit of a mess, but the ZCatalog API has traditionally
        # supported passing in query restrictions in almost arbitary ways
        real_req = None
        if isinstance(request, dict):
            query = request.copy()
        elif isinstance(request, CatalogSearchArgumentsMap):
            query = {}
            query.update(request.keywords)
            real_req = request.request
            if isinstance(real_req, dict):
                query.update(real_req)
                real_req = None
        else:
            real_req = request

        if real_req:
            warnings.warn('You have specified a query using either a request '
                          'object or a mixture of a query dict and keyword '
                          'arguments. Please use only a simple query dict. '
                          'Your query contained "%s". This support is '
                          'deprecated and will be removed in Zope 2.14.' %
                          repr(real_req), DeprecationWarning, stacklevel=4)

            known_keys = query.keys()
            # The request has too many places where an index restriction
            # might be specified. Putting all of request.form,
            # request.other, ... into the query isn't what we want.
            # So we iterate over all known indexes instead and see if they
            # are in the request.
            for iid in self.indexes.keys():
                if iid in known_keys:
                    continue
                value = real_req.get(iid)
                if value:
                    query[iid] = value
        return query

    def _get_index_query_names(self, index):
        if hasattr(index, 'getIndexQueryNames'):
            return index.getIndexQueryNames()
        return (index.getId(),)

    def _sorted_search_indexes(self, query):
        # Simple implementation doing no ordering.
        query_keys = query.keys()
        order = []
        for name, index in self.indexes.items():
            for attr in self._get_index_query_names(index):
                if attr in query_keys:
                    order.append((ILimitedResultIndex.providedBy(index), name))
                    break
        order.sort()
        return [i[1] for i in order]

    def _limit_sequence(self, sequence, slen, b_start=0, b_size=None,
                        switched_reverse=False):
        if b_size is not None:
            sequence = sequence[b_start:b_start + b_size]
            if slen:
                slen = len(sequence)
        if switched_reverse:
            sequence.reverse()
        return (sequence, slen)

    def search(self, query, sort_index=None, reverse=0, limit=None, merge=1):
        """Iterate through the indexes, applying the query to each one. If
        merge is true then return a lazy result set (sorted if appropriate)
        otherwise return the raw (possibly scored) results for later merging.
        Limit is used in conjuntion with sorting or scored results to inform
        the catalog how many results you are really interested in. The catalog
        can then use optimizations to save time and memory. The number of
        results is not guaranteed to fall within the limit however, you should
        still slice or batch the results as usual."""

        rs = None # resultset

        # Indexes fulfill a fairly large contract here. We hand each
        # index the query mapping we are given (which may be composed
        # of some combination of web request, kw mappings or plain old dicts)
        # and the index decides what to do with it. If the index finds work
        # for itself in the query, it returns the results and a tuple of
        # the attributes that were used. If the index finds nothing for it
        # to do then it returns None.

        # Canonicalize the request into a sensible query before passing it on
        query = self.make_query(query)

        cr = self.getCatalogPlan(query)
        cr.start()

        plan = cr.plan()
        if not plan:
            plan = self._sorted_search_indexes(query)

        indexes = self.indexes.keys()
        for i in plan:
            if i not in indexes:
                # We can have bogus keys or the plan can contain index names
                # that have been removed in the meantime
                continue

            index = self.getIndex(i)
            _apply_index = getattr(index, "_apply_index", None)
            if _apply_index is None:
                continue

            cr.start_split(i)
            limit_result = ILimitedResultIndex.providedBy(index)
            if limit_result:
                r = _apply_index(query, rs)
            else:
                r = _apply_index(query)

            if r is not None:
                r, u = r
                # Short circuit if empty result
                # BBB: We can remove the "r is not None" check in Zope 2.14
                # once we don't need to support the "return everything" case
                # anymore
                if r is not None and not r:
                    cr.stop_split(i, result=None, limit=limit_result)
                    return LazyCat([])

                # provide detailed info about the pure intersection time
                intersect_id = i + '#intersection'
                cr.start_split(intersect_id)
                # weightedIntersection preserves the values from any mappings
                # we get, as some indexes don't return simple sets
                if hasattr(rs, 'items') or hasattr(r, 'items'):
                    _, rs = weightedIntersection(rs, r)
                else:
                    rs = intersection(rs, r)

                cr.stop_split(intersect_id)

                # consider the time it takes to intersect the index result with
                # the total resultset to be part of the index time
                cr.stop_split(i, result=r, limit=limit_result)
                if not rs:
                    break
            else:
                cr.stop_split(i, result=None, limit=limit_result)

        # Try to deduce the sort limit from batching arguments
        b_start = int(query.get('b_start', 0))
        b_size = query.get('b_size', None)
        if b_size is not None:
            b_size = int(b_size)

        if b_size is not None:
            limit = b_start + b_size
        elif limit and b_size is None:
            b_size = limit

        if rs is None:
            # None of the indexes found anything to do with the query
            # We take this to mean that the query was empty (an empty filter)
            # and so we return everything in the catalog
            warnings.warn('Your query %s produced no query restriction. '
                          'Currently the entire catalog content is returned. '
                          'In Zope 2.14 this will result in an empty LazyCat '
                          'to be returned.' % repr(cr.make_key(query)),
                          DeprecationWarning, stacklevel=3)

            rlen = len(self)
            if sort_index is None:
                sequence, slen = self._limit_sequence(self.data.items(), rlen,
                    b_start, b_size)
                result = LazyMap(self.instantiate, sequence, slen,
                    actual_result_count=rlen)
            else:
                cr.start_split('sort_on')
                result = self.sortResults(
                    self.data, sort_index, reverse, limit, merge,
                        actual_result_count=rlen, b_start=b_start,
                        b_size=b_size)
                cr.stop_split('sort_on', None)
        elif rs:
            # We got some results from the indexes.
            # Sort and convert to sequences.
            # XXX: The check for 'values' is really stupid since we call
            # items() and *not* values()
            rlen = len(rs)
            if sort_index is None and hasattr(rs, 'items'):
                # having a 'items' means we have a data structure with
                # scores.  Build a new result set, sort it by score, reverse
                # it, compute the normalized score, and Lazify it.

                if not merge:
                    # Don't bother to sort here, return a list of
                    # three tuples to be passed later to mergeResults
                    # note that data_record_normalized_score_ cannot be
                    # calculated and will always be 1 in this case
                    getitem = self.__getitem__
                    result = [(score, (1, score, rid), getitem)
                            for rid, score in rs.items()]
                else:
                    cr.start_split('sort_on')

                    rs = rs.byValue(0) # sort it by score
                    max = float(rs[0][0])

                    # Here we define our getter function inline so that
                    # we can conveniently store the max value as a default arg
                    # and make the normalized score computation lazy
                    def getScoredResult(item, max=max, self=self):
                        """
                        Returns instances of self._v_brains, or whatever is
                        passed into self.useBrains.
                        """
                        score, key = item
                        r=self._v_result_class(self.data[key])\
                              .__of__(aq_parent(self))
                        r.data_record_id_ = key
                        r.data_record_score_ = score
                        r.data_record_normalized_score_ = int(100. * score / max)
                        return r

                    sequence, slen = self._limit_sequence(rs, rlen, b_start,
                        b_size)
                    result = LazyMap(getScoredResult, sequence, slen,
                        actual_result_count=rlen)
                    cr.stop_split('sort_on', None)

            elif sort_index is None and not hasattr(rs, 'values'):
                # no scores
                if hasattr(rs, 'keys'):
                    rs = rs.keys()
                sequence, slen = self._limit_sequence(rs, rlen, b_start,
                    b_size)
                result = LazyMap(self.__getitem__, sequence, slen,
                    actual_result_count=rlen)
            else:
                # sort.  If there are scores, then this block is not
                # reached, therefore 'sort-on' does not happen in the
                # context of a text index query.  This should probably
                # sort by relevance first, then the 'sort-on' attribute.
                cr.start_split('sort_on')
                result = self.sortResults(rs, sort_index, reverse, limit,
                    merge, actual_result_count=rlen, b_start=b_start,
                    b_size=b_size)
                cr.stop_split('sort_on', None)
        else:
            # Empty result set
            result = LazyCat([])
        cr.stop()
        return result

    def sortResults(self, rs, sort_index, reverse=0, limit=None, merge=1,
                    actual_result_count=None, b_start=0, b_size=None):
        # Sort a result set using a sort index. Return a lazy
        # result set in sorted order if merge is true otherwise
        # returns a list of (sortkey, uid, getter_function) tuples
        #
        # The two 'for' loops in here contribute a significant
        # proportion of the time to perform an indexed search.
        # Try to avoid all non-local attribute lookup inside
        # those loops.
        _intersection = intersection
        _self__getitem__ = self.__getitem__
        index_key_map = sort_index.documentToKeyMap()
        _None = None
        _keyerror = KeyError
        result = []
        append = result.append
        if hasattr(rs, 'keys'):
            rs = rs.keys()
        if actual_result_count is None:
            rlen = len(rs)
            actual_result_count = rlen
        else:
            rlen = actual_result_count

        # don't limit to more than what we have
        if limit is not None and limit >= rlen:
            limit = rlen

        # if we want a batch from the end of the resultset, reverse sorting
        # order and limit it, then reverse the resultset again
        switched_reverse = False
        if b_size and b_start and b_start > rlen / 2:
            reverse = not reverse
            switched_reverse = True
            b_end = b_start + b_size
            if b_end >= rlen:
                overrun = rlen - b_end
                if b_start >= rlen:
                    # bail out, we are outside the possible range
                    return LazyCat([], 0, actual_result_count)
                else:
                    b_size += overrun
                b_start = 0
            else:
                b_start = rlen - b_end
            limit = b_start + b_size

        if merge and limit is None and (
            rlen > (len(sort_index) * (rlen / 100 + 1))):
            # The result set is much larger than the sorted index,
            # so iterate over the sorted index for speed.
            # This is rarely exercised in practice...

            length = 0

            try:
                intersection(rs, IISet(()))
            except TypeError:
                # rs is not an object in the IIBTree family.
                # Try to turn rs into an IISet.
                rs = IISet(rs)

            for k, intset in sort_index.items():
                # We have an index that has a set of values for
                # each sort key, so we intersect with each set and
                # get a sorted sequence of the intersections.
                intset = _intersection(rs, intset)
                if intset:
                    keys = getattr(intset, 'keys', _None)
                    if keys is not _None:
                        # Is this ever true?
                        intset = keys()
                    length += len(intset)
                    append((k, intset, _self__getitem__))
                    # Note that sort keys are unique.

            if reverse:
                result.sort(reverse=True)
            else:
                result.sort()
            sequence, slen = self._limit_sequence(result, length, b_start,
                b_size, switched_reverse)
            result = LazyCat(LazyValues(sequence), slen, actual_result_count)
        elif limit is None or (limit * 4 > rlen):
            # Iterate over the result set getting sort keys from the index
            for did in rs:
                try:
                    key = index_key_map[did]
                except _keyerror:
                    # This document is not in the sort key index, skip it.
                    pass
                else:
                    append((key, did, _self__getitem__))
                    # The reference back to __getitem__ is used in case
                    # we do not merge now and need to intermingle the
                    # results with those of other catalogs while avoiding
                    # the cost of instantiating a LazyMap per result
            if merge:
                if reverse:
                    result.sort(reverse=True)
                else:
                    result.sort()
                if limit is not None:
                    result = result[:limit]
                sequence, _ = self._limit_sequence(result, 0, b_start, b_size,
                    switched_reverse)
                result = LazyValues(sequence)
                result.actual_result_count = actual_result_count
            else:
                sequence, _ = self._limit_sequence(result, 0, b_start, b_size,
                    switched_reverse)
                return sequence
        elif reverse:
            # Limit/sort results using N-Best algorithm
            # This is faster for large sets then a full sort
            # And uses far less memory
            keys = []
            n = 0
            worst = None
            for did in rs:
                try:
                    key = index_key_map[did]
                except _keyerror:
                    # This document is not in the sort key index, skip it.
                    pass
                else:
                    if n >= limit and key <= worst:
                        continue
                    i = bisect(keys, key)
                    keys.insert(i, key)
                    result.insert(i, (key, did, _self__getitem__))
                    if n == limit:
                        del keys[0], result[0]
                    else:
                        n += 1
                    worst = keys[0]
            result.reverse()
            if merge:
                sequence, _ = self._limit_sequence(result, 0, b_start, b_size,
                    switched_reverse)
                result = LazyValues(sequence)
                result.actual_result_count = actual_result_count
            else:
                sequence, _ = self._limit_sequence(result, 0, b_start, b_size,
                    switched_reverse)
                return sequence
        elif not reverse:
            # Limit/sort results using N-Best algorithm in reverse (N-Worst?)
            keys = []
            n = 0
            best = None
            for did in rs:
                try:
                    key = index_key_map[did]
                except _keyerror:
                    # This document is not in the sort key index, skip it.
                    pass
                else:
                    if n >= limit and key >= best:
                        continue
                    i = bisect(keys, key)
                    keys.insert(i, key)
                    result.insert(i, (key, did, _self__getitem__))
                    if n == limit:
                        del keys[-1], result[-1]
                    else:
                        n += 1
                    best = keys[-1]
            if merge:
                sequence, _ = self._limit_sequence(result, 0, b_start, b_size,
                    switched_reverse)
                result = LazyValues(sequence)
                result.actual_result_count = actual_result_count
            else:
                sequence, _ = self._limit_sequence(result, 0, b_start, b_size,
                    switched_reverse)
                return sequence

        return LazyMap(self.__getitem__, result, len(result),
            actual_result_count=actual_result_count)

    def _get_sort_attr(self, attr, kw):
        """Helper function to find sort-on or sort-order."""
        # There are three different ways to find the attribute:
        # 1. kw[sort-attr]
        # 2. self.sort-attr
        # 3. kw[sort_attr]
        # kw may be a dict or an ExtensionClass MultiMapping, which
        # differ in what get() returns with no default value.
        name = "sort-%s" % attr
        val = kw.get(name, None)
        if val is not None:
            return val
        val = getattr(self, name, None)
        if val is not None:
            return val
        return kw.get("sort_%s" % attr, None)

    def _getSortIndex(self, args):
        """Returns a search index object or None."""
        sort_index_name = self._get_sort_attr("on", args)
        if sort_index_name is not None:
            # self.indexes is always a dict, so get() w/ 1 arg works
            sort_index = self.indexes.get(sort_index_name)
            if sort_index is None:
                raise CatalogError('Unknown sort_on index (%s)' %
                                   sort_index_name)
            else:
                if not hasattr(sort_index, 'documentToKeyMap'):
                    raise CatalogError(
                        'The index chosen for sort_on (%s) is not capable of '
                        'being used as a sort index.' % sort_index_name)
            return sort_index
        else:
            return None

    def searchResults(self, REQUEST=None, used=None, _merge=1, **kw):
        # You should pass in a simple dictionary as the request argument,
        # which only contains the relevant query.
        # The used argument is deprecated and is ignored
        if REQUEST is None and not kw:
            # Try to acquire request if we get no args for bw compat
            warnings.warn('Calling searchResults without a query argument nor '
                          'keyword arguments is deprecated. In Zope 2.14 the '
                          'query will no longer be automatically taken from '
                          'the acquired request.',
                          DeprecationWarning, stacklevel=3)
            REQUEST = getattr(self, 'REQUEST', None)
        if isinstance(REQUEST, dict) and not kw:
            # short cut for the best practice
            args = REQUEST
        else:
            args = CatalogSearchArgumentsMap(REQUEST, kw)
        sort_index = self._getSortIndex(args)
        sort_limit = self._get_sort_attr('limit', args)
        reverse = 0
        if sort_index is not None:
            order = self._get_sort_attr("order", args)
            if (isinstance(order, str) and
                order.lower() in ('reverse', 'descending')):
                reverse = 1
        # Perform searches with indexes and sort_index
        return self.search(args, sort_index, reverse, sort_limit, _merge)

    __call__ = searchResults

    def getCatalogPlan(self, query=None):
        """Query time reporting and planning.
        """
        parent = aq_base(aq_parent(self))
        threshold = getattr(parent, 'long_query_time', 0.1)
        return CatalogPlan(self, query, threshold)


class CatalogSearchArgumentsMap(object):
    """Multimap catalog arguments coming simultaneously from keywords
    and request.

    BBB: Values that are empty strings are treated as non-existent. This is
    to ignore empty values, thereby ignoring empty form fields to be
    consistent with hysterical behavior. This is deprecated and can be changed
    in Zope 2.14.
    """

    def __init__(self, request, keywords):
        self.request = request or {}
        self.keywords = keywords or {}

    def __getitem__(self, key):
        marker = []
        v = self.keywords.get(key, marker)
        if v is marker or v == '':
            v = self.request[key]
        if v == '':
            raise KeyError(key)
        return v

    def get(self, key, default=None):
        try:
            v = self[key]
        except KeyError:
            return default
        else:
            return v

    def has_key(self, key):
        try:
            self[key]
        except KeyError:
            return False
        else:
            return True

    def __contains__(self, name):
        return self.has_key(name)


def mergeResults(results, has_sort_keys, reverse):
    """Sort/merge sub-results, generating a flat sequence.

    results is a list of result set sequences, all with or without sort keys
    """
    if not has_sort_keys:
        return LazyCat(results)
    else:
        # Concatenate the catalog results into one list and sort it
        # Each result record consists of a list of tuples with three values:
        # (sortkey, docid, catalog__getitem__)
        combined = []
        if len(results) > 1:
            for r in results:
                combined.extend(r)
        elif len(results) == 1:
            combined = results[0]
        else:
            return []
        if reverse:
            combined.sort(reverse=True)
        else:
            combined.sort()
        return LazyMap(lambda rec: rec[2](rec[1]), combined, len(combined))
