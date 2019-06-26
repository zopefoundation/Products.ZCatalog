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

from BTrees.OOBTree import difference
from BTrees.OOBTree import OOSet
from App.special_dtml import DTMLFile
from zope.interface import implementer

from Products.PluginIndexes.unindex import UnIndex
from Products.PluginIndexes.interfaces import (
    IIndexingMissingValue,
    missing,
    IIndexingEmptyValue,
    empty,
)

_marker = []
LOG = getLogger('Zope.KeywordIndex')

try:
    basestring
except NameError:
    # Python 3 compatibility
    basestring = (bytes, str)


@implementer(IIndexingMissingValue, IIndexingEmptyValue)
class KeywordIndex(UnIndex):
    """Like an UnIndex only it indexes sequences of items.

    Searches match any keyword.

    This should have an _apply_index that returns a relevance score
    """
    meta_type = 'KeywordIndex'
    query_options = ('query', 'range', 'not', 'operator')
    manage_options = (
        {'label': 'Settings', 'action': 'manage_main'},
        {'label': 'Browse', 'action': 'manage_browse'},
    )

    def _index_object(self, documentId, obj, threshold=None, attr=''):
        """ index an object 'obj' with integer id 'i'

        Ideally, we've been passed a sequence of some sort that we
        can iterate over. If however, we haven't, we should do something
        useful with the results. In the case of a string, this means
        indexing the entire string as a keyword."""

        # First we need to see if there's anything interesting to look at
        # self.id is the name of the index, which is also the name of the
        # attribute we're interested in.  If the attribute is callable,
        # we'll do so.

        newKeywords = self.get_object_datum(obj, attr)
        oldKeywords = self._unindex.get(documentId, _marker)

        if oldKeywords is _marker:
            # we've got a new document, let's not futz around.
            if self.providesSpecialIndex(newKeywords):
                self.insertSpecialIndexEntry(newKeywords, documentId)
            else:
                try:
                    self.index_objectKeywords(documentId, newKeywords)
                except self.exceptions_treated_as_missing:
                    LOG.error('%(context)s: Unable to insert forward '
                              'index entry for document with id '
                              '%(doc_id)s and keywords %(keywords)r '
                              'for index %{index}r.', dict(
                                  context=self.__class__.__name__,
                                  keywords=newKeywords,
                                  doc_id=documentId,
                                  index=self.id))
                    if self.providesSpecialIndex(missing):
                        newKeywords = missing
                        self.insertSpecialIndexEntry(missing, documentId)
                    else:
                        return 0
        else:
            # we have an existing entry for this document, and we need
            # to figure out if any of the keywords have actually changed
            if self.providesSpecialIndex(oldKeywords) and \
               self.providesSpecialIndex(newKeywords):
                if oldKeywords == newKeywords:
                    return 0
                self.removeSpecialIndexEntry(oldKeywords, documentId)
                self.insertSpecialIndexEntry(newKeywords, documentId)
                self._unindex[documentId] = newKeywords
                return 1

            if self.providesSpecialIndex(oldKeywords):
                self.removeSpecialIndexEntry(oldKeywords, documentId)
                oldSet = OOSet()
            else:
                if not isinstance(oldKeywords, OOSet):
                    oldKeywords = OOSet(oldKeywords)
                oldSet = oldKeywords

            if self.providesSpecialIndex(newKeywords):
                self.insertSpecialIndexEntry(newKeywords, documentId)
                newSet = OOSet()
            else:
                newSet = newKeywords = OOSet(newKeywords)

            try:
                fdiff = difference(oldSet, newSet)
                rdiff = difference(newSet, oldSet)
                if fdiff or rdiff:
                    # if we've got forward or reverse changes
                    if fdiff:
                        self.unindex_objectKeywords(documentId, fdiff)
                    if rdiff:
                        self.index_objectKeywords(documentId, rdiff)
                else:
                    # return if no difference
                    return 0

            except self.exceptions_treated_as_missing:
                LOG.error('%(context)s: Unable to insert forward '
                          'index entry for document with id '
                          '%(doc_id)s and keywords %(keywords)r '
                          'for index %{index}r.', dict(
                              context=self.__class__.__name__,
                              keywords=newKeywords,
                              doc_id=documentId,
                              index=self.id))

                if self.providesSpecialIndex(missing):
                    newKeywords = missing
                    self.insertSpecialIndexEntry(missing, documentId)
                else:
                    del self._unindex[documentId]
                    return 1

        self._unindex[documentId] = newKeywords

        return 1

    def map_value(self, value):
        value = super(KeywordIndex, self).map_value(value)
        if value is not missing:
            # at this place, *value* is expected to be a sequence
            if isinstance(value, basestring):
                value = OOSet((value,))
            if not value and self.providesSpecialIndex(empty):
                value = empty
            else:
                value = OOSet(value)

        return value

    def index_objectKeywords(self, documentId, keywords):
        """ carefully index keywords of object with integer id 'documentId'
        """

        for kw in keywords:
            self.insertForwardIndexEntry(kw, documentId)

    def unindex_objectKeywords(self, documentId, keywords):
        """ carefully unindex keywords of object with integer id 'documentId'
        """

        if keywords is not None:
            for kw in keywords:
                self.removeForwardIndexEntry(kw, documentId)

    def unindex_object(self, documentId):
        """ carefully unindex the object with integer id 'documentId'"""

        keywords = self._unindex.get(documentId, _marker)

        if keywords is _marker:
            return

        self._increment_counter()

        if self.providesSpecialIndex(keywords):
            self.removeSpecialIndexEntry(keywords, documentId)
        else:
            self.unindex_objectKeywords(documentId, keywords)

        del self._unindex[documentId]

    manage = manage_main = DTMLFile('dtml/manageKeywordIndex', globals())
    manage_main._setName('manage_main')
    manage_browse = DTMLFile('../dtml/browseIndex', globals())


manage_addKeywordIndexForm = DTMLFile('dtml/addKeywordIndex', globals())


def manage_addKeywordIndex(self, id, extra=None,
                           REQUEST=None, RESPONSE=None, URL3=None):
    """Add a keyword index"""
    return self.manage_addIndex(id, 'KeywordIndex', extra=extra,
                                REQUEST=REQUEST, RESPONSE=RESPONSE, URL1=URL3)
