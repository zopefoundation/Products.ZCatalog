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

from cgi import escape
from logging import getLogger
import sys

from Acquisition import aq_base
from Acquisition import aq_inner
from Acquisition import aq_parent
from Acquisition import aq_get

from BTrees.IIBTree import difference
from BTrees.IIBTree import intersection
from BTrees.IIBTree import IITreeSet
from BTrees.IIBTree import IISet
from BTrees.IIBTree import multiunion
from BTrees.IOBTree import IOBTree
from BTrees.Length import Length
from BTrees.OOBTree import OOBTree
from OFS.SimpleItem import SimpleItem
from ZODB.POSException import ConflictError
from zope.interface import implements

from Products.PluginIndexes.common import safe_callable
from Products.PluginIndexes.common.util import parseIndexRequest
from Products.PluginIndexes.interfaces import ILimitedResultIndex
from Products.PluginIndexes.interfaces import ISortIndex
from Products.PluginIndexes.interfaces import IUniqueValueIndex

_marker = []
LOG = getLogger('Zope.UnIndex')

class RequestCache(dict):
    def __str__(self):
        return "<RequestCache %s items>" % len(self)

class UnIndex(SimpleItem):

    """Simple forward and reverse index.
    """
    implements(ILimitedResultIndex, IUniqueValueIndex, ISortIndex)

    def __init__(
        self, id, ignore_ex=None, call_methods=None, extra=None, caller=None):
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

        self.operators = ('or', 'and')
        self.useOperator = 'or'

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
                    # XXX swallow KeyError because it was probably
                    # removed and then _length AttributeError raised
                    pass
                if isinstance(self.__len__, Length):
                    self._length = self.__len__
                    del self.__len__
                self._length.change(-1)
            except Exception:
                LOG.error('%s: unindex_object could not remove '
                          'documentId %s from index %s.  This '
                          'should not happen.' % (self.__class__.__name__,
                           str(documentId), str(self.id)),
                           exc_info=sys.exc_info())
        else:
            LOG.error('%s: unindex_object tried to retrieve set %s '
                      'from index %s but couldn\'t.  This '
                      'should not happen.' % (self.__class__.__name__,
                      repr(entry), str(self.id)))

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
            raise TypeError('None cannot be indexed.')

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
                            'now its not, for document: %s' % documentId)

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

        self.removeForwardIndexEntry(unindexRecord, documentId)
        try:
            del self._unindex[documentId]
        except ConflictError:
            raise
        except Exception:
            LOG.debug('Attempt to unindex nonexistent document'
                      ' with id %s' % documentId, exc_info=True)

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

    def _catalog_cache_key(self, catalog):
        cid = catalog.getId()
        counter = getattr(aq_base(catalog), 'getCounter', None)
        if counter is not None:
            return '%s_%s' % (cid, counter())
        return cid

    def _record_cache_key(self,record):
        params = []

        operator = record.get('operator', self.useOperator)
        if operator != 'or':
            # operator is not supported
            return None

        not_parm = record.get('not', None)
        if not_parm:

            if isinstance(not_parm,list):
                not_parm = tuple(not_parm)
            elif not isinstance(not_parm,tuple):
                not_parm = (not_parm,)

            not_parm = list(kw)
            not_parm.sort()
            not_parm = tuple(not_parm )
            params.append(('not', not_parm))
        
        range_parm = record.get('range', None)
        if range_parm:
            params.append(('range_parm', range_parm))

        usage_parm = record.get('usage', None)
        if usage_parm:
            params.append(('usage_parm', usage_parm))

        kw = record.keys
        if isinstance(kw,list):
            kw = tuple(kw)
        elif not isinstance(kw,tuple):
            kw = (kw,)
             
        kl = list(kw)
        kl.extend(params)
        kl.sort()
        key = tuple(kl)

        return (self.id,key)

    def _apply_index(self, request, resultset=None):
        """Apply the index to query parameters given in the request arg.

        The request argument should be a mapping object.

        If the request does not have a key which matches the "id" of
        the index instance, then None is returned.

        If the request *does* have a key which matches the "id" of
        the index instance, one of a few things can happen:

          - if the value is a blank string, None is returned (in
            order to support requests from web forms where
            you can't tell a blank string from empty).

          - if the value is a nonblank string, turn the value into
            a single-element sequence, and proceed.

          - if the value is a sequence, return a union search.

          - If the value is a dict and contains a key of the form
            '<index>_operator' this overrides the default method
            ('or') to combine search results. Valid values are "or"
            and "and".

        If None is not returned as a result of the abovementioned
        constraints, two objects are returned.  The first object is a
        ResultSet containing the record numbers of the matching
        records.  The second object is a tuple containing the names of
        all data fields used.

        FAQ answer:  to search a Field Index for documents that
        have a blank string as their value, wrap the request value
        up in a tuple ala: request = {'id':('',)}
        """
        record = parseIndexRequest(request, self.id, self.query_options)
        if record.keys is None:
            return None

        index = self._index
        r = None
        opr = None

        # not / exclude parameter
        not_parm = record.get('not', None)
        cachekey = None
        REQUEST = aq_get(self, 'REQUEST', None)
        if REQUEST is not None:
            catalog = aq_parent(aq_parent(aq_inner(self)))
            if catalog is not None:
                key = self._catalog_cache_key(catalog)
                cache = REQUEST.get(key, None)
                cachekey = self._record_cache_key(record)
                if cache is None:
                    cache = REQUEST[key] = RequestCache()
                elif cachekey is not None:
                    cached = cache.get(cachekey, None)
                    if cached is not None:
                        LOG.debug('%s catalog: %s index: %s cachekey: %s -> hit' % (self.__class__.__name__,key, str(self.id), str(cachekey)))
                        if not_parm:
                            not_parm = map(self._convert, not_parm)
                            exclude = self._apply_not(not_parm, resultset)
                            r = difference(cached, exclude)
                            return r, (self.id,)

                        return cached, (self.id,)

        if not record.keys and not_parm:
            # convert into indexed format
            not_parm = map(self._convert, not_parm)
            # we have only a 'not' query
            record.keys = [k for k in index.keys() if k not in not_parm]
        else:
            # convert query arguments into indexed format
            record.keys = map(self._convert, record.keys)

        # experimental code for specifing the operator
        operator = record.get('operator', self.useOperator)
        if not operator in self.operators:
            raise RuntimeError("operator not valid: %s" % escape(operator))

        # Range parameter
        range_parm = record.get('range', None)
        if range_parm:
            opr = "range"
            opr_args = []
            if range_parm.find("min") > -1:
                opr_args.append("min")
            if range_parm.find("max") > -1:
                opr_args.append("max")

        if record.get('usage', None):
            # see if any usage params are sent to field
            opr = record.usage.lower().split(':')
            opr, opr_args = opr[0], opr[1:]

        if opr == "range":  # range search
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
                    cache[cachekey] = result               

                if not_parm:
                    exclude = self._apply_not(not_parm, resultset)
                    result = difference(result, exclude)
                return result, (self.id,)

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
                r = resultset
                for s in setlist:
                    # the result is bound by the resultset
                    r = intersection(r, s)
                    
        else:  # not a range search
            # Filter duplicates
            setlist = []
            for k in record.keys:
                if k is None:
                    raise TypeError('None cannot be in an index.')
                s = index.get(k, None)
                # If None, try to bail early
                if s is None:
                    if operator == 'or':
                        # If union, we can't possibly get a bigger result
                        continue
                    # If intersection, we can't possibly get a smaller result
                    return IISet(), (self.id,)
                elif isinstance(s, int):
                    s = IISet((s,))
                setlist.append(s)

            # If we only use one key return immediately
            if len(setlist) == 1:
                result = setlist[0]
                if isinstance(result, int):
                    result = IISet((result,))

                if cachekey is not None:
                    cache[cachekey] = result

                if not_parm:
                    exclude = self._apply_not(not_parm, resultset)
                    result = difference(result, exclude)
                return result, (self.id,)

            if operator == 'or':
                # If we already get a small result set passed in, intersecting
                # the various indexes with it and doing the union later is
                # faster than creating a multiunion first.
                if cachekey is None and resultset is not None and len(resultset) < 200:
                    smalllist = []
                    for s in setlist:
                        smalllist.append(intersection(resultset, s))
                    r = multiunion(smalllist)
                else:
                    r = multiunion(setlist)
                    if cachekey is not None:
                        cache[cachekey] = r                    
            else:
                # For intersection, sort with smallest data set first
                if len(setlist) > 2:
                    setlist = sorted(setlist, key=len)
                r = resultset
                for s in setlist:
                    r = intersection(r, s)

        if isinstance(r, int):
            r = IISet((r, ))
        if r is None:
            return IISet(), (self.id,)
        if not_parm:
            exclude = self._apply_not(not_parm, resultset)
            r = difference(r, exclude)
        return r, (self.id,)

    def hasUniqueValuesFor(self, name):
        """has unique values for column name"""
        if name == self.id:
            return 1
        return 0

    def getIndexSourceNames(self):
        """ return sequence of indexed attributes """
        # BBB:  older indexes didn't have 'indexed_attrs'
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
            raise StopIteration

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
