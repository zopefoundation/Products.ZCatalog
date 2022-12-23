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

import os

from AccessControl.class_init import InitializeClass
from AccessControl.Permissions import manage_zcatalog_indexes
from AccessControl.Permissions import view
from AccessControl.SecurityInfo import ClassSecurityInfo
from App.Common import package_home
from App.special_dtml import DTMLFile
from BTrees.IIBTree import IITreeSet
from BTrees.IIBTree import difference
from BTrees.IIBTree import intersection
from BTrees.IIBTree import multiunion
from BTrees.IOBTree import IOBTree
from BTrees.Length import Length
from zope.interface import implementer

from Products.PluginIndexes.interfaces import IDateRangeIndex
from Products.PluginIndexes.unindex import UnIndex
from Products.PluginIndexes.util import datetime_to_minutes
from Products.PluginIndexes.util import safe_callable
from Products.ZCatalog.query import IndexQuery


_dtmldir = os.path.join(package_home(globals()), 'dtml')
MAX32 = int(2 ** 31 - 1)


@implementer(IDateRangeIndex)
class DateRangeIndex(UnIndex):

    """Index for date ranges, such as the "effective-expiration" range in CMF.

    Any object may return None for either the start or the end date: for the
    start date, this should be the logical equivalent of "since the beginning
    of time"; for the end date, "until the end of time".

    Therefore, divide the space of indexed objects into four containers:

    - Objects which always match (i.e., they returned None for both);

    - Objects which match after a given time (i.e., they returned None for the
      end date);

    - Objects which match until a given time (i.e., they returned None for the
      start date);

    - Objects which match only during a specific interval.
    """

    security = ClassSecurityInfo()

    meta_type = 'DateRangeIndex'
    query_options = ('query', )

    manage_options = ({'label': 'Properties',
                       'action': 'manage_indexProperties'},
                      )

    since_field = until_field = None

    # int(DateTime('1000/1/1 0:00 GMT-12').millis() / 1000 / 60)
    floor_value = -510162480
    # int(DateTime('2499/12/31 0:00 GMT+12').millis() / 1000 / 60)
    ceiling_value = 278751600
    # precision of indexed time interval in minutes
    precision_value = 1

    def __init__(self, id, since_field=None, until_field=None,
                 caller=None, extra=None, floor_value=None,
                 ceiling_value=None, precision_value=None):

        if extra:
            since_field = extra.since_field
            until_field = extra.until_field
            floor_value = getattr(extra, 'floor_value', None)
            ceiling_value = getattr(extra, 'ceiling_value', None)
            precision_value = getattr(extra, 'precision_value', None)

        self._setId(id)
        self._edit(since_field, until_field, floor_value,
                   ceiling_value, precision_value)
        self.clear()

    @security.protected(view)
    def getSinceField(self):
        """Get the name of the attribute indexed as start date.
        """
        return self._since_field

    @security.protected(view)
    def getUntilField(self):
        """Get the name of the attribute indexed as end date.
        """
        return self._until_field

    @security.protected(view)
    def getFloorValue(self):
        """ """
        return self.floor_value

    @security.protected(view)
    def getCeilingValue(self):
        """ """
        return self.ceiling_value

    @security.protected(view)
    def getPrecisionValue(self):
        """ """
        return self.precision_value

    manage_indexProperties = DTMLFile('manageDateRangeIndex', _dtmldir)

    @security.protected(manage_zcatalog_indexes)
    def manage_edit(self, since_field, until_field, floor_value,
                    ceiling_value, precision_value, REQUEST):
        """ """
        self._edit(since_field, until_field, floor_value, ceiling_value,
                   precision_value)
        REQUEST['RESPONSE'].redirect('{}/manage_main'
                                     '?manage_tabs_message=Updated'.format(
                                         REQUEST.get('URL2')))

    @security.private
    def _edit(self, since_field, until_field, floor_value=None,
              ceiling_value=None, precision_value=None):
        """Update the fields used to compute the range.
        """
        self._since_field = since_field
        self._until_field = until_field
        if floor_value not in (None, ''):
            self.floor_value = int(floor_value)
        if ceiling_value not in (None, ''):
            self.ceiling_value = int(ceiling_value)
        if precision_value not in (None, ''):
            self.precision_value = int(precision_value)

    @security.protected(manage_zcatalog_indexes)
    def clear(self):
        """Start over fresh."""
        self._always = IITreeSet()
        self._since_only = IOBTree()
        self._until_only = IOBTree()
        self._since = IOBTree()
        self._until = IOBTree()
        self._unindex = IOBTree()  # 'datum' will be a tuple of date ints
        self._length = Length()
        if self._counter is None:
            self._counter = Length()
        else:
            self._increment_counter()

    def getEntryForObject(self, documentId, default=None):
        """Get all information contained for the specific object
        identified by 'documentId'.  Return 'default' if not found.
        """
        return self._unindex.get(documentId, default)

    def index_object(self, documentId, obj, threshold=None):
        """Index an object:
        - 'documentId' is the integer ID of the document
        - 'obj' is the object to be indexed
        - ignore threshold
        """
        if self._since_field is None:
            return 0

        since = getattr(obj, self._since_field, None)
        if safe_callable(since):
            since = since()
        since = self._convertDateTime(since)

        until = getattr(obj, self._until_field, None)
        if safe_callable(until):
            until = until()
        until = self._convertDateTime(until)

        datum = (since, until)

        old_datum = self._unindex.get(documentId, None)
        if datum == old_datum:  # No change?  bail out!
            return 0

        self._increment_counter()

        if old_datum is not None:
            old_since, old_until = old_datum
            self._removeForwardIndexEntry(old_since, old_until, documentId)

        self._insertForwardIndexEntry(since, until, documentId)
        self._unindex[documentId] = datum

        return 1

    def unindex_object(self, documentId):
        """Remove the object corresponding to 'documentId' from the index.
        """

        datum = self._unindex.get(documentId, None)
        if datum is None:
            return

        self._increment_counter()

        since, until = datum
        self._removeForwardIndexEntry(since, until, documentId)
        del self._unindex[documentId]

    def uniqueValues(self, name=None, withLengths=0):
        """Return a sequence of unique values for 'name'.

        If 'withLengths' is true, return a sequence of tuples, in
        the form '(value, length)'.
        """
        if name not in (self._since_field, self._until_field):
            return

        if name == self._since_field:
            sets = (self._since, self._since_only)
        else:
            sets = (self._until, self._until_only)

        if not withLengths:
            for s in sets:
                for key in s.keys():
                    yield key
        else:
            for s in sets:
                for key, value in s.items():
                    if isinstance(value, int):
                        yield (key, 1)
                    else:
                        yield (key, len(value))

    def getRequestCacheKey(self, record, resultset=None):
        term = self._convertDateTime(record.keys[0])
        tid = str(term)

        # unique index identifier
        iid = '_{}_{}_{}'.format(self.__class__.__name__,
                                 self.id, self.getCounter())
        # record identifier
        if resultset is None:
            rid = f'_{tid}'
        else:
            rid = f'_inverse_{tid}'

        return (iid, rid)

    def _apply_index(self, request, resultset=None):
        record = IndexQuery(request, self.id, self.query_options,
                            self.operators, self.useOperator)
        if record.keys is None:
            return None
        return (self.query_index(record, resultset=resultset),
                (self._since_field, self._until_field))

    def query_index(self, record, resultset=None):
        cache = self.getRequestCache()
        if cache is not None:
            cachekey = self.getRequestCacheKey(record, resultset)
            cached = cache.get(cachekey, None)
            if cached is not None:
                if resultset is None:
                    return cached
                else:
                    return difference(resultset, cached)

        term = self._convertDateTime(record.keys[0])
        if resultset is None:
            # Aggregate sets for each bucket separately, to avoid
            # large-small union penalties.
            until_only = multiunion(self._until_only.values(term))
            since_only = multiunion(self._since_only.values(None, term))
            until = multiunion(self._until.values(term))
            since = multiunion(self._since.values(None, term))
            bounded = intersection(until, since)

            # Merge from smallest to largest.
            result = multiunion([bounded, until_only, since_only,
                                 self._always])
            if cache is not None:
                cache[cachekey] = result

            return result
        else:
            # Compute the inverse and subtract from res
            until_only = multiunion(self._until_only.values(None, term - 1))
            since_only = multiunion(self._since_only.values(term + 1))
            until = multiunion(self._until.values(None, term - 1))
            since = multiunion(self._since.values(term + 1))

            result = multiunion([since, since_only, until_only, until])
            if cache is not None:
                cache[cachekey] = result

            return difference(resultset, result)

    def _insert_migrate(self, tree, key, value):
        treeset = tree.get(key, None)
        if treeset is None:
            tree[key] = IITreeSet((value, ))
        else:
            if isinstance(treeset, IITreeSet):
                treeset.insert(value)
            elif isinstance(treeset, int):
                tree[key] = IITreeSet((treeset, value))
            else:
                tree[key] = IITreeSet(treeset)
                tree[key].insert(value)

    def _insertForwardIndexEntry(self, since, until, documentId):
        """Insert 'documentId' into the appropriate set based on 'datum'.
        """
        if since is None and until is None:
            self._always.insert(documentId)
        elif since is None:
            self._insert_migrate(self._until_only, until, documentId)
        elif until is None:
            self._insert_migrate(self._since_only, since, documentId)
        else:
            self._insert_migrate(self._since, since, documentId)
            self._insert_migrate(self._until, until, documentId)

    def _remove_delete(self, tree, key, value):
        treeset = tree.get(key, None)
        if treeset is not None:
            if isinstance(treeset, int):
                del tree[key]
            else:
                treeset.remove(value)
                if not treeset:
                    del tree[key]

    def _removeForwardIndexEntry(self, since, until, documentId):
        """Remove 'documentId' from the appropriate set based on 'datum'.
        """
        if since is None and until is None:
            self._always.remove(documentId)
        elif since is None:
            self._remove_delete(self._until_only, until, documentId)
        elif until is None:
            self._remove_delete(self._since_only, since, documentId)
        else:
            self._remove_delete(self._since, since, documentId)
            self._remove_delete(self._until, until, documentId)

    def _convertDateTime(self, value):
        value = datetime_to_minutes(value, self.precision_value)

        if value is None:
            return None

        if (value > self.ceiling_value or value < self.floor_value):
            # handle values outside our specified range
            return None

        return value


InitializeClass(DateRangeIndex)

manage_addDateRangeIndexForm = DTMLFile('addDateRangeIndex', _dtmldir)


def manage_addDateRangeIndex(self, id, extra=None,
                             REQUEST=None, RESPONSE=None, URL3=None):
    """Add a date range index to the catalog, using the incredibly icky
       double-indirection-which-hides-NOTHING.
    """
    return self.manage_addIndex(id, 'DateRangeIndex', extra,
                                REQUEST, RESPONSE, URL3)
