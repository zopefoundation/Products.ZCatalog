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

import logging
from bisect import bisect
from collections import defaultdict
from functools import cmp_to_key
from operator import itemgetter
from random import randint

import Acquisition
import BTrees.Length
import ExtensionClass
from Acquisition import aq_base
from Acquisition import aq_parent
from BTrees.IIBTree import IISet
from BTrees.IIBTree import intersection
from BTrees.IIBTree import weightedIntersection
from BTrees.IOBTree import IOBTree
from BTrees.OIBTree import OIBTree
from Missing import MV
from Persistence import Persistent
from ZTUtils.Lazy import LazyCat
from ZTUtils.Lazy import LazyMap
from ZTUtils.Lazy import LazyValues

from Products.PluginIndexes.interfaces import ILimitedResultIndex
from Products.PluginIndexes.interfaces import IQueryIndex
from Products.PluginIndexes.interfaces import ITransposeQuery
from Products.PluginIndexes.util import safe_callable
from Products.ZCatalog.CatalogBrains import AbstractCatalogBrain
from Products.ZCatalog.CatalogBrains import NoBrainer
from Products.ZCatalog.plan import CatalogPlan
from Products.ZCatalog.ProgressHandler import ZLogHandler
from Products.ZCatalog.query import IndexQuery


LOG = logging.getLogger('Zope.ZCatalog')


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

        self.schema = {}   # mapping from attribute name to column number
        self.names = ()    # sequence of column names
        self.indexes = {}  # mapping from index name to index object

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

        for index in self.indexes:
            self.getIndex(index).clear()

    def updateBrains(self):
        self.useBrains(self._v_brains)

    def __getitem__(self, index):
        """
        Returns instances of self._v_brains, or whatever is passed
        into self.useBrains.
        """
        if isinstance(index, tuple):
            # then it contains a score...
            normalized_score, score, key = index
        else:
            # otherwise no score, set all scores to 1
            normalized_score, score, key = (1, 1, index)
        return self.instantiate((key, self.data[key]),
                                score_data=(score, normalized_score))

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

        class mybrains(AbstractCatalogBrain, brains):  # NOQA
            pass

        scopy = self.schema.copy()

        schema_len = len(self.schema.keys())
        scopy['data_record_id_'] = schema_len
        scopy['data_record_score_'] = schema_len + 1
        scopy['data_record_normalized_score_'] = schema_len + 2

        mybrains.__record_schema__ = scopy

        self._v_brains = brains
        self._v_result_class = mybrains

    def addColumn(self, name, default_value=None, threshold=10000):
        """Adds a row to the meta data schema"""
        schema = self.schema
        names = list(self.names)
        threshold = threshold if threshold is not None else 10000

        if name != name.strip():
            # Someone could have mistakenly added a space at the end
            # of the input field.
            LOG.warning('stripped space from new column %r -> %r', name,
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

        if len(self):
            pghandler = ZLogHandler(threshold)
            pghandler.init('Adding %s column' % name, len(self))
            for i, (key, value) in enumerate(self.data.iteritems()):
                pghandler.report(i)
                self.data[key] = value + (default_value, )
            pghandler.finish()

        self.names = tuple(names)
        self.schema = schema

        # new column? update the brain
        self.updateBrains()

    def delColumn(self, name, threshold=10000):
        """Deletes a row from the meta data schema"""
        names = list(self.names)
        _index = names.index(name)
        threshold = threshold if threshold is not None else 10000

        if name not in self.schema:
            LOG.error('delColumn attempted to delete nonexistent '
                      'column %s.', str(name))
            return

        del names[_index]

        # rebuild the schema
        schema = {}
        for i, name in enumerate(names):
            schema[name] = i

        self.schema = schema
        self.names = tuple(names)

        # update the brain
        self.updateBrains()

        # remove the column value from each record
        if len(self):
            _next_index = _index + 1
            pghandler = ZLogHandler(threshold)
            pghandler.init('Deleting %s column' % name, len(self))
            for i, (key, value) in enumerate(self.data.iteritems()):
                pghandler.report(i)
                self.data[key] = value[:_index] + value[_next_index:]
            pghandler.finish()

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
            LOG.warning('stripped space from new index %r -> %r', name,
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

        if name not in self.indexes:
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
                      update_metadata=True):
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

        if index is None:
            # we are inserting new data
            index = self.updateMetadata(object, uid, None)
            self._length.change(1)
            self.uids[uid] = index
            self.paths[index] = uid
        elif update_metadata:
            # we are updating and we need to update metadata
            self.updateMetadata(object, uid, index)

        # do indexing
        total = 0

        if idxs == []:
            use_indexes = self.indexes.keys()
        else:
            use_indexes = set(idxs)
            for iid in self.indexes:
                x = self.getIndex(iid)
                if ITransposeQuery.providedBy(x):
                    # supported index names for query optimization
                    names = x.getIndexNames()
                    intersec = use_indexes.intersection(names)
                    # add current index for indexing if supported index
                    # names are member of idxs
                    if intersec:
                        use_indexes.update([iid])

            use_indexes = list(use_indexes)

        for name in use_indexes:
            x = self.getIndex(name)
            if hasattr(x, 'index_object'):
                blah = x.index_object(index, object, threshold)
                total = total + blah
            else:
                LOG.error('catalogObject was passed bad index '
                          'object %s.', str(x))

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
                      'with a uid of %s. ', str(uid))

    def uniqueValuesFor(self, name):
        """ return unique values for FieldIndex name """
        return tuple(self.getIndex(name).uniqueValues())

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

    def _maintain_zodb_cache(self):
        parent = aq_parent(self)
        if hasattr(aq_base(parent), 'maintain_zodb_cache'):
            parent.maintain_zodb_cache()

    def instantiate(self, record, score_data=None):
        """ internal method: create and initialise search result object.
        record should be a tuple of (document RID, metadata columns tuple),
        score_data can be a tuple of (scode, normalized score) or be omitted"""
        self._maintain_zodb_cache()
        key, data = record
        klass = self._v_result_class
        if score_data:
            score, normalized_score = score_data
            schema_len = len(klass.__record_schema__)
            if schema_len == len(data) + 3:
                # if we have complete data, create in a single pass
                data = tuple(data) + (key, score, normalized_score)
                return klass(data).__of__(aq_parent(self))
        r = klass(data)
        r.data_record_id_ = key
        if score_data:
            # preserved during refactoring for compatibility reasons:
            # can only be reached if score_data is present,
            # but schema length is not equal to len(data) + 3
            # no known use cases
            r.data_record_score_ = score
            r.data_record_normalized_score_ = normalized_score
            return r.__of__(aq_parent(self))
        return r.__of__(self)

    def getMetadataForRID(self, rid):
        record = self.data[rid]
        result = {}
        for (key, pos) in self.schema.items():
            result[key] = record[pos]
        return result

    def getIndexDataForRID(self, rid):
        result = {}
        for name in self.indexes:
            result[name] = self.getIndex(name).getEntryForObject(rid, "")
        return result

    def merge_query_args(self, query=None, **kw):
        if not kw and isinstance(query, dict):
            # Short cut for the best practice.
            return query

        merged_query = {}
        if isinstance(query, dict):
            merged_query.update(query)
        merged_query.update(kw)
        return merged_query

    def make_query(self, query):
        for iid in self.indexes:
            index = self.getIndex(iid)
            if ITransposeQuery.providedBy(index):
                query = index.make_query(query)

        # Canonicalize tuple/list query arguments.
        new_query = {}
        for key, value in query.items():
            if isinstance(value, (list, tuple)):
                new_query[key] = list(sorted(value))
            else:
                new_query[key] = value

        return new_query

    def _get_index_query_names(self, index):
        if hasattr(index, 'getIndexQueryNames'):
            return index.getIndexQueryNames()
        return (index.getId(),)

    def _sort_limit_arguments(self, query, sort_index, reverse, limit):
        b_start = int(query.get('b_start', 0))
        b_size = query.get('b_size', None)
        if b_size is not None:
            b_size = int(b_size)

        if b_size is not None:
            limit = b_start + b_size
        elif limit and b_size is None:
            b_size = limit

        if sort_index is None:
            sort_report_name = None
        else:
            if isinstance(sort_index, list):
                sort_name = '-'.join(i.getId() for i in sort_index)
            else:
                sort_name = sort_index.getId()
            if isinstance(reverse, list):
                reverse_name = '-'.join(
                    'desc' if r else 'asc' for r in reverse)
            else:
                reverse_name = 'desc' if reverse else 'asc'
            sort_report_name = 'sort_on#' + sort_name + '#' + reverse_name
            if limit is not None:
                sort_report_name += '#limit-%s' % limit
        return (b_start, b_size, limit, sort_report_name)

    def _sorted_search_indexes(self, query):
        # Simple implementation ordering only by limited result support
        query_keys = query.keys()
        order = []
        for name, index in self.indexes.items():
            for attr in self._get_index_query_names(index):
                if attr in query_keys:
                    order.append((ILimitedResultIndex.providedBy(index), name))
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

    def _search_index(self, cr, index_id, query, rs):
        cr.start_split(index_id)

        index_rs = None
        index = self.getIndex(index_id)
        limit_result = ILimitedResultIndex.providedBy(index)

        if IQueryIndex.providedBy(index):
            index_query = IndexQuery(query, index.id, index.query_options,
                                     index.operators, index.useOperator)
            if index_query.keys is not None:
                index_rs = index.query_index(index_query, rs)
        else:
            if limit_result:
                index_result = index._apply_index(query, rs)
            else:
                index_result = index._apply_index(query)

            # Parse (resultset, used_attributes) index return value.
            if index_result:
                index_rs, _ = index_result

        if not index_rs:
            # Short circuit if empty index result.
            rs = None
        else:
            # Provide detailed info about the pure intersection time.
            intersect_id = index_id + '#intersection'
            cr.start_split(intersect_id)
            # weightedIntersection preserves the values from any mappings
            # we get, as some indexes don't return simple sets.
            if hasattr(rs, 'items') or hasattr(index_rs, 'items'):
                _, rs = weightedIntersection(rs, index_rs)
            else:
                rs = intersection(rs, index_rs)

            cr.stop_split(intersect_id)

        # Consider the time it takes to intersect the index result
        # with the total result set to be part of the index time.
        cr.stop_split(index_id, result=index_rs, limit=limit_result)

        return rs

    def search(self, query,
               sort_index=None, reverse=False, limit=None, merge=True):
        """Iterate through the indexes, applying the query to each one. If
        merge is true then return a lazy result set (sorted if appropriate)
        otherwise return the raw (possibly scored) results for later merging.
        Limit is used in conjunction with sorting or scored results to inform
        the catalog how many results you are really interested in. The catalog
        can then use optimizations to save time and memory. The number of
        results is not guaranteed to fall within the limit however, you should
        still slice or batch the results as usual."""

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

        rs = None  # result set
        for index_id in plan:
            # The actual core loop over all indices.
            if index_id not in self.indexes:
                # We can have bogus keys or the plan can contain index names
                # that have been removed in the meantime.
                continue

            rs = self._search_index(cr, index_id, query, rs)
            if not rs:
                break

        if not rs:
            # None of the indexes found anything to do with the query.
            result = LazyCat([])
            cr.stop()
            return result

        # Try to deduce the sort limit from batching arguments.
        b_start, b_size, limit, sort_report_name = self._sort_limit_arguments(
            query, sort_index, reverse, limit)

        # We got some results from the indexes, sort and convert to sequences.
        rlen = len(rs)
        if sort_index is None and hasattr(rs, 'items'):
            # Having a 'items' means we have a data structure with
            # scores. Build a new result set, sort it by score, reverse
            # it, compute the normalized score, and Lazify it.

            if not merge:
                # Don't bother to sort here, return a list of
                # three tuples to be passed later to mergeResults.
                # Note that data_record_normalized_score_ cannot be
                # calculated and will always be 1 in this case.
                result = [(score, (1, score, rid), self.__getitem__)
                          for rid, score in rs.items()]
            else:
                cr.start_split('sort_on#score')

                # Sort it by score.
                rs = rs.byValue(0)
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
                    norm_score = int(100.0 * score / max)
                    return self.instantiate((key, self.data[key]),
                                            score_data=(score, norm_score))

                sequence, slen = self._limit_sequence(
                    rs, rlen, b_start, b_size)
                result = LazyMap(getScoredResult, sequence, slen,
                                 actual_result_count=rlen)
                cr.stop_split('sort_on#score', None)

        elif sort_index is None and not hasattr(rs, 'values'):
            # no scores
            if hasattr(rs, 'keys'):
                rs = rs.keys()
            sequence, slen = self._limit_sequence(
                rs, rlen, b_start, b_size)
            result = LazyMap(self.__getitem__, sequence, slen,
                             actual_result_count=rlen)
        else:
            # Sort. If there are scores, then this block is not
            # reached, therefore 'sort-on' does not happen in the
            # context of a text index query.  This should probably
            # sort by relevance first, then the 'sort-on' attribute.
            cr.start_split(sort_report_name)
            result = self.sortResults(
                rs, sort_index, reverse, limit, merge,
                actual_result_count=rlen, b_start=b_start, b_size=b_size)
            cr.stop_split(sort_report_name, None)

        cr.stop()
        return result

    def _sort_iterate_index(self, actual_result_count, result, rs,
                            limit, merge, reverse,
                            sort_index, sort_index_length, sort_spec,
                            second_indexes_key_map):
        # The result set is much larger than the sorted index,
        # so iterate over the sorted index for speed.
        # TODO: len(sort_index) isn't actually what we want for a keyword
        # index, as it's only the unique values, not the documents.
        # Don't use this case while using limit, as we return results of
        # non-flattened intsets, and would have to merge/unflattened those
        # before limiting.
        length = 0
        try:
            intersection(rs, IISet(()))
        except TypeError:
            # rs is not an object in the IIBTree family.
            # Try to turn rs into an IISet.
            rs = IISet(rs)

        if sort_index_length == 1:
            for k, intset in sort_index.items():
                # We have an index that has a set of values for
                # each sort key, so we intersect with each set and
                # get a sorted sequence of the intersections.
                intset = intersection(rs, intset)
                if intset:
                    keys = getattr(intset, 'keys', None)
                    if keys is not None:
                        # Is this ever true?
                        intset = keys()
                    length += len(intset)
                    result.append((k, intset, self.__getitem__))
            result.sort(reverse=reverse)
        else:
            for k, intset in sort_index.items():
                # We have an index that has a set of values for
                # each sort key, so we intersect with each set and
                # get a sorted sequence of the intersections.
                intset = intersection(rs, intset)
                if intset:
                    keys = getattr(intset, 'keys', None)
                    if keys is not None:
                        # Is this ever true?
                        intset = keys()
                    length += len(intset)
                    # sort on secondary index
                    keysets = defaultdict(list)
                    for i in intset:
                        full_key = (k, )
                        for km in second_indexes_key_map:
                            try:
                                full_key += (km[i], )
                            except KeyError:
                                pass
                        keysets[full_key].append(i)
                    for k2, v2 in keysets.items():
                        result.append((k2, v2, self.__getitem__))
            result = multisort(result, sort_spec)

        return (actual_result_count, length, result)

    def _sort_iterate_resultset(self, actual_result_count, result, rs,
                                limit, merge, reverse,
                                sort_index, sort_index_length, sort_spec,
                                second_indexes_key_map):
        # Iterate over the result set getting sort keys from the index.
        # If we are interested in at least 25% or more of the result set,
        # the N-Best algorithm is slower, so we iterate over all.
        index_key_map = sort_index.documentToKeyMap()

        if sort_index_length == 1:
            for did in rs:
                try:
                    key = index_key_map[did]
                except KeyError:
                    # This document is not in the sort key index, skip it.
                    actual_result_count -= 1
                else:
                    # The reference back to __getitem__ is used in case
                    # we do not merge now and need to intermingle the
                    # results with those of other catalogs while avoiding
                    # the cost of instantiating a LazyMap per result
                    result.append((key, did, self.__getitem__))
            if merge:
                result = sorted(
                    result,
                    key=lambda x: (0,) if x[0] is None else x,
                    reverse=reverse)
        else:
            for did in rs:
                try:
                    full_key = (index_key_map[did], )
                    for km in second_indexes_key_map:
                        full_key += (km[did], )
                except KeyError:
                    # This document is not in the sort key index, skip it.
                    actual_result_count -= 1
                else:
                    result.append((full_key, did, self.__getitem__))
            if merge:
                result = multisort(result, sort_spec)

        if merge and limit is not None:
            result = result[:limit]

        return (actual_result_count, 0, result)

    def _sort_nbest(self, actual_result_count, result, rs,
                    limit, merge, reverse,
                    sort_index, sort_index_length, sort_spec,
                    second_indexes_key_map):
        # Limit / sort results using N-Best algorithm
        # This is faster for large sets then a full sort
        # And uses far less memory
        index_key_map = sort_index.documentToKeyMap()
        keys = []
        n = 0
        worst = None
        if sort_index_length == 1:
            for did in rs:
                try:
                    key = index_key_map[did]
                except KeyError:
                    # This document is not in the sort key index, skip it.
                    actual_result_count -= 1
                else:
                    if n >= limit and key <= worst:
                        continue
                    i = bisect(keys, key)
                    keys.insert(i, key)
                    result.insert(i, (key, did, self.__getitem__))
                    if n == limit:
                        del keys[0], result[0]
                    else:
                        n += 1
                    worst = keys[0]
            result.reverse()
        else:
            for did in rs:
                try:
                    key = index_key_map[did]
                    full_key = (key, )
                    for km in second_indexes_key_map:
                        full_key += (km[did], )
                except KeyError:
                    # This document is not in the sort key index, skip it.
                    actual_result_count -= 1
                else:
                    if n >= limit and key <= worst:
                        continue
                    i = bisect(keys, key)
                    keys.insert(i, key)
                    result.insert(i, (full_key, did, self.__getitem__))
                    if n == limit:
                        del keys[0], result[0]
                    else:
                        n += 1
                    worst = keys[0]
            result = multisort(result, sort_spec)

        return (actual_result_count, 0, result)

    def _sort_nbest_reverse(self, actual_result_count, result, rs,
                            limit, merge, reverse,
                            sort_index, sort_index_length, sort_spec,
                            second_indexes_key_map):
        # Limit / sort results using N-Best algorithm in reverse (N-Worst?)
        index_key_map = sort_index.documentToKeyMap()
        keys = []
        n = 0
        best = None
        if sort_index_length == 1:
            for did in rs:
                try:
                    key = index_key_map[did]
                except KeyError:
                    # This document is not in the sort key index, skip it.
                    actual_result_count -= 1
                else:
                    if n >= limit and key >= best:
                        continue
                    i = bisect(keys, key)
                    keys.insert(i, key)
                    result.insert(i, (key, did, self.__getitem__))
                    if n == limit:
                        del keys[-1], result[-1]
                    else:
                        n += 1
                    best = keys[-1]
        else:
            for did in rs:
                try:
                    key = index_key_map[did]
                    full_key = (key, )
                    for km in second_indexes_key_map:
                        full_key += (km[did], )
                except KeyError:
                    # This document is not in the sort key index, skip it.
                    actual_result_count -= 1
                else:
                    if n >= limit and key >= best:
                        continue
                    i = bisect(keys, key)
                    keys.insert(i, key)
                    result.insert(i, (full_key, did, self.__getitem__))
                    if n == limit:
                        del keys[-1], result[-1]
                    else:
                        n += 1
                    best = keys[-1]
            result = multisort(result, sort_spec)

        return (actual_result_count, 0, result)

    def sortResults(self, rs, sort_index,
                    reverse=False, limit=None, merge=True,
                    actual_result_count=None, b_start=0, b_size=None):
        # Sort a result set using one or more sort indexes. Both sort_index
        # and reverse can be lists of indexes and reverse specifications.
        # Return a lazy result set in sorted order if merge is true otherwise
        # returns a list of (sortkey, uid, getter_function) tuples, where
        # sortkey can be a tuple on its own.
        second_indexes = None
        second_indexes_key_map = None
        sort_index_length = 1
        if isinstance(sort_index, list):
            sort_index_length = len(sort_index)
            if sort_index_length > 1:
                second_indexes = sort_index[1:]
                second_indexes_key_map = []
                for si in second_indexes:
                    second_indexes_key_map.append(si.documentToKeyMap())
            sort_index = sort_index[0]

        result = []
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

        # if we want a batch from the end of the result set, reverse sorting
        # order and limit it, then reverse the result set again
        switched_reverse = False
        if b_size and b_start and b_start > rlen / 2:
            if isinstance(reverse, list):
                reverse = [not r for r in reverse]
            else:
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

        # determine sort_spec
        if isinstance(reverse, list):
            sort_spec = [r and -1 or 1 for r in reverse]
            # limit to current maximum of sort indexes
            sort_spec = sort_spec[:sort_index_length]
            # use first sort order for choosing the algorithm
            first_reverse = reverse[0]
        else:
            sort_spec = []
            for i in range(sort_index_length):
                sort_spec.append(reverse and -1 or 1)
            first_reverse = reverse

        # Special first condition, as it changes post-processing.
        iterate_sort_index = (
            merge and limit is None and (
                rlen > (len(sort_index) * (rlen / 100 + 1))))

        # Choose one of the sort algorithms.
        if iterate_sort_index:
            sort_func = self._sort_iterate_index
        elif limit is None or (limit * 4 > rlen) or sort_index_length > 1:
            sort_func = self._sort_iterate_resultset
        elif first_reverse:
            sort_func = self._sort_nbest
        else:
            sort_func = self._sort_nbest_reverse

        actual_result_count, length, result = sort_func(
            actual_result_count, result, rs,
            limit, merge, reverse,
            sort_index, sort_index_length, sort_spec,
            second_indexes_key_map)

        sequence, slen = self._limit_sequence(
            result, length, b_start, b_size, switched_reverse)

        if iterate_sort_index:
            result = LazyCat(LazyValues(sequence), slen, actual_result_count)
        else:
            if not merge:
                return sequence

            result = LazyValues(sequence)
            result.actual_result_count = actual_result_count

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
        """Returns a list of search index objects or None."""
        sort_index_names = self._get_sort_attr("on", args)
        if sort_index_names is not None:
            # self.indexes is always a dict, so get() w/ 1 arg works
            sort_indexes = []
            if not isinstance(sort_index_names, (list, tuple)):
                sort_index_names = [sort_index_names]
            for name in sort_index_names:
                sort_index = self.indexes.get(name)
                if sort_index is None:
                    raise CatalogError('Unknown sort_on index: %s' %
                                       repr(name))
                else:
                    if not hasattr(sort_index, 'documentToKeyMap'):
                        raise CatalogError(
                            'The index chosen for sort_on is '
                            'not capable of being used as a sort index: '
                            '%s' % repr(name))
                sort_indexes.append(sort_index)
            if len(sort_indexes) == 1:
                # be nice and keep the old API intact for single sort_on's
                return sort_indexes[0]
            return sort_indexes
        return None

    def searchResults(self, query=None, _merge=True, **kw):
        # You should pass in a simple dictionary as the first argument,
        # which only contains the relevant query.
        query = self.merge_query_args(query, **kw)
        sort_indexes = self._getSortIndex(query)
        sort_limit = self._get_sort_attr('limit', query)
        reverse = False
        if sort_indexes is not None:
            order = self._get_sort_attr("order", query)
            reverse = []
            if order is None:
                order = ['']
            elif isinstance(order, str):
                order = [order]
            for o in order:
                reverse.append(o.lower() in ('reverse', 'descending'))
            if len(reverse) == 1:
                # be nice and keep the old API intact for single sort_order
                reverse = reverse[0]
        # Perform searches with indexes and sort_index
        return self.search(query, sort_indexes, reverse, sort_limit, _merge)

    __call__ = searchResults

    def getCatalogPlan(self, query=None):
        """Query time reporting and planning.
        """
        parent = aq_base(aq_parent(self))
        threshold = getattr(parent, 'long_query_time', 0.1)
        return CatalogPlan(self, query, threshold)


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


def multisort(items, sort_spec):
    """Sort a list by multiple keys bidirectionally.

    sort_spec is a list of ones and minus ones, with 1 meaning sort normally
    and -1 meaning sort reversed.

    The length of sort_spec must match the length of the first value in each
    list entry given via `items`.
    """
    first = sort_spec[0]
    different = any([True for s in sort_spec if s != first])
    first_reverse = first != 1 and True or False

    # always do an in-place sort. The default sort via key is really fast and
    # the later cmp approach is efficient when getting mostly sorted data.
    items.sort(reverse=first_reverse, key=itemgetter(0))
    if not different:
        # if all keys are sorted in the same direction, we are done
        return items

    comparers = []
    for i, v in enumerate(sort_spec):
        comparers.append((itemgetter(i), v))

    def comparer(left, right):
        for func, order in comparers:
            # emulate cmp
            a = func(left[0])
            b = func(right[0])
            result = ((a > b) - (a < b))
            if result:
                return order * result
        return 0

    items.sort(key=cmp_to_key(comparer))

    return items
