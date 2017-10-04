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

from App.special_dtml import DTMLFile
from BTrees.IIBTree import IIBTree, IITreeSet, IISet
from BTrees.IIBTree import union, intersection, difference
import BTrees.Length
from ZODB.POSException import ConflictError

from Products.PluginIndexes.unindex import _marker
from Products.PluginIndexes.unindex import UnIndex

LOG = getLogger('BooleanIndex.UnIndex')


class BooleanIndex(UnIndex):
    """Index for booleans

       self._index = set([documentId1, documentId2])
       self._unindex = {documentId:[True/False]}

       self._length is the length of the unindex
       self._index_length is the length of the index
       self._index_value is the indexed value

       The document ids in self._index have self._index_value as their value.

       Since there are only two possible values (True/False), the index
       only stores a forward index for the less common value.
       It starts off with the opposite of value of the first document
       and later checks and inverts itself, if more than 60% of all
       documents now have the indexed value. It does the inversion
       at 60% to avoid inverting itself constantly for an index that
       has a roughly equal 50/50 split.
    """

    meta_type = "BooleanIndex"

    manage_options = (
        {'label': 'Settings',
         'action': 'manage_main'},
        {'label': 'Browse',
         'action': 'manage_browse'},
    )

    query_options = ["query"]

    manage = manage_main = DTMLFile('dtml/manageBooleanIndex', globals())
    manage_main._setName('manage_main')
    manage_browse = DTMLFile('../dtml/browseIndex', globals())

    _index_value = 1
    _index_length = None

    def clear(self):
        self._index = IITreeSet()
        self._index_length = BTrees.Length.Length()
        self._index_value = 1
        self._unindex = IIBTree()
        self._length = BTrees.Length.Length()
        if self._counter is None:
            self._counter = BTrees.Length.Length()
        else:
            self._increment_counter()

    def histogram(self):
        """Return a mapping which provides a histogram of the number of
        elements found at each point in the index.
        """
        histogram = {}
        indexed = bool(self._index_value)
        histogram[indexed] = self._index_length.value
        histogram[not indexed] = self._length.value - self._index_length.value
        return histogram

    def _invert_index(self, documentId=None):
        self._index_value = indexed = int(not self._index_value)
        self._index.clear()
        length = 0
        for rid, value in self._unindex.iteritems():
            if value == indexed:
                self._index.add(rid)
                length += 1
        # documentId is the rid of the currently processed object that
        # triggered the invert. in the case of unindexing, the rid hasn't
        # been removed from the unindex yet. While indexing, the rid will
        # be added to the index and unindex after this method is done
        if documentId is not None:
            self._index.remove(documentId)
            length -= 1
        self._index_length = BTrees.Length.Length(length)

    def insertForwardIndexEntry(self, entry, documentId):
        """If the value matches the indexed one, insert into treeset
        """
        # When we get the first entry, decide to index the opposite of what
        # we got, as indexing zero items is fewer than one.
        if self._length.value == 0:
            self._index_value = int(not bool(entry))

        # if the added entry value is index value, insert it into index
        if bool(entry) is bool(self._index_value):
            self._index_length.change(1)
            self._index.insert(documentId)

        # insert value into global unindex (before computing index invert)
        self._unindex[documentId] = entry
        self._length.change(1)

        # is the index (after adding the current entry) larger than 60%
        # of the total length? than switch the indexed value
        if bool(entry) is bool(self._index_value):
            if (self._index_length.value) >= ((self._length.value) * 0.6):
                self._invert_index()

    def removeForwardIndexEntry(self, entry, documentId, check=True):
        """Take the entry provided and remove any reference to documentId
        in its entry in the index.
        """
        if bool(entry) is bool(self._index_value):
            try:
                self._index.remove(documentId)
                self._index_length.change(-1)
            except ConflictError:
                raise
            except Exception:
                LOG.exception(
                    '%s: unindex_object could not remove documentId %s '
                    'from index %s. This should not happen.' % (
                        self.__class__.__name__,
                        str(documentId),
                        str(self.id)))
        elif check:
            # is the index (after removing the current entry) larger than
            # 60% of the total length? than switch the indexed value
            if (self._index_length.value) <= ((self._length.value - 1) * 0.6):
                self._invert_index(documentId)
                return

    def _index_object(self, documentId, obj, threshold=None, attr=''):
        """ index and object 'obj' with integer id 'documentId'"""
        returnStatus = 0

        # First we need to see if there's anything interesting to look at
        datum = self._get_object_datum(obj, attr)

        # Make it boolean, int as an optimization
        if datum is not _marker:
            datum = int(bool(datum))

        # We don't want to do anything that we don't have to here, so we'll
        # check to see if the new and existing information is the same.
        oldDatum = self._unindex.get(documentId, _marker)
        if datum != oldDatum:
            if oldDatum is not _marker:
                self.removeForwardIndexEntry(oldDatum, documentId, check=False)
                if datum is _marker:
                    try:
                        del self._unindex[documentId]
                        self._length.change(-1)
                    except ConflictError:
                        raise
                    except Exception:
                        LOG.error('Should not happen: oldDatum was there, now '
                                  'its not, for document with id %s' %
                                  documentId)

            if datum is not _marker:
                self.insertForwardIndexEntry(datum, documentId)

            returnStatus = 1

        return returnStatus

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
            self._length.change(-1)
        except ConflictError:
            raise
        except Exception:
            LOG.debug('Attempt to unindex nonexistent document'
                      ' with id %s' % documentId, exc_info=True)

    def query_index(self, record, resultset=None):
        index = self._index
        indexed = self._index_value

        for key in record.keys:
            if bool(key) is bool(indexed):
                # If we match the indexed value, check index
                return intersection(index, resultset)
            else:
                # Otherwise, remove from resultset or _unindex
                if resultset is None:
                    return union(difference(self._unindex, index), IISet([]))
                else:
                    return intersection(difference(resultset, index),
                                        self._unindex)
        return IISet()

    def indexSize(self):
        """Return distinct values, as an optimization we always claim 2."""
        return 2

    def items(self):
        # return a list of value to int set of rid tuples
        indexed = self._index_value
        items = [(bool(indexed), self._index)]
        false = IISet()
        for rid, value in self._unindex.iteritems():
            if value != indexed:
                false.add(rid)
        items.append((not bool(indexed), false))
        return items


manage_addBooleanIndexForm = DTMLFile('dtml/addBooleanIndex', globals())


def manage_addBooleanIndex(self, id, extra=None,
                           REQUEST=None, RESPONSE=None, URL3=None):
    """Add a boolean index"""
    return self.manage_addIndex(
        id, 'BooleanIndex', extra=extra,
        REQUEST=REQUEST, RESPONSE=RESPONSE, URL1=URL3)
