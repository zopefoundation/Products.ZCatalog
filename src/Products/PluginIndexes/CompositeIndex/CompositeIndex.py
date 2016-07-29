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

import sys
import logging
import time
import transaction

from Acquisition import aq_parent
from Acquisition import aq_inner
from Persistence import PersistentMapping

from App.special_dtml import DTMLFile

from BTrees.OOBTree import difference
from BTrees.OOBTree import OOSet

from zope.interface import implements

from Products.PluginIndexes.interfaces import ITransposeQuery
from Products.PluginIndexes.common.UnIndex import UnIndex
from Products.PluginIndexes.common.util import parseIndexRequest
from Products.PluginIndexes.common import safe_callable

from itertools import product
from itertools import combinations

_marker = []

LOG = logging.getLogger('CompositeIndex')

QUERY_OPTIONS = {'FieldIndex': ('query', 'range', 'not'),
                 'KeywordIndex': ('query', 'range', 'not', 'operator'),
                 }

MIN_COMPONENTS = 2


class ComponentMapping(PersistentMapping):
    """A persistent wrapper for mapping objects
    recording the order in which items are added. """

    def __init__(self, *args, **kwargs):
        self._keys = []
        PersistentMapping.__init__(self, *args, **kwargs)

    def __delitem__(self, key):
        self._keys.remove(key)
        PersistentMapping.__delitem__(self, key)

    def __setitem__(self, key, item):
        if key not in self._keys:
            self._keys.append(key)
        PersistentMapping.__setitem__(self, key, item)

    def clear(self):
        self._keys = []
        PersistentMapping.clear(self)

    def copy(self):
        cm = ComponentMapping()
        cm.update(self)
        return cm

    def items(self):
        return zip(self._keys, self.values())

    def keys(self):
        return self._keys[:]

    def popitem(self):
        try:
            key = self._keys[-1]
        except IndexError:
            raise KeyError('dictionary is empty')

        val = self[key]
        del self[key]

        return (key, val)

    def setdefault(self, key, failobj=None):
        if key not in self._keys:
            self._keys.append(key)
        return PersistentMapping.setdefault(self, key, failobj)

    def update(self, d):
        for (key, val) in d.items():
            self.__setitem__(key, val)

    def values(self):
        return map(self.get, self._keys)


class Component(object):

    _attributes = ''

    def __init__(self, id, meta_type, attributes):
        self._id = id
        self._meta_type = meta_type
        if attributes:
            self._attributes = attributes

    @property
    def id(self):
        return self._id

    @property
    def meta_type(self):
        return self._meta_type

    @property
    def attributes(self):

        attributes = self._attributes

        if not attributes:
            return [self._id]

        if isinstance(attributes, str):
            return attributes.split(',')

        attributes = list(attributes)

        attributes = [attr.strip() for attr in attributes if attr]

        return attributes

    @property
    def rawAttributes(self):
        return self._attributes

    def __repr__(self):
        return "<id: %s; metatype: %s; attributes: %s>" % \
            (self.id, self.meta_type, self.attributes)


class CompositeIndex(UnIndex):

    """Index for composition of simple fields.
       or sequences of items
    """
    implements(ITransposeQuery)

    meta_type = "CompositeIndex"

    manage_options = (
        {'label': 'Settings',
         'action': 'manage_main',
         'help': ('CompositeIndex', 'CompositeIndex_Settings.stx')},
        {'label': 'Browse',
         'action': 'manage_browse',
         'help': ('CompositeIndex', 'CompositeIndex_Settings.stx')},
    )

    query_options = ("query", "operator")

    def __init__(self, id, ignore_ex=None, call_methods=None,
                 extra=None, caller=None):
        """Create an composite index

        UnIndexes are indexes that contain two index components, the
        forward index (like plain index objects) and an inverted
        index.  The inverted index is so that objects can be unindexed
        even when the old value of the object is not known.

        e.g.

        self._index = {datum:[documentId1, documentId2]}
        self._unindex = {documentId:datum}

        If any item in self._index has a length-one value, the value is an
        integer, and not a set.  There are special cases in the code to deal
        with this.

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
          can be list of dicts with following keys { id, type, attributes }

          'caller' -- reference to the calling object (usually
          a (Z)Catalog instance
        """

        self.id = id
        self.ignore_ex = ignore_ex        # currently unimplimented
        self.call_methods = call_methods

        self.operators = ('or', 'and')
        self.useOperator = 'or'

        # set components
        self._components = ComponentMapping()
        if extra:
            for cdata in extra:
                c_id = cdata['id']
                c_meta_type = cdata['meta_type']
                c_attributes = cdata['attributes']
                self._components[c_id] = Component(c_id, c_meta_type,
                                                   c_attributes)
        self.clear()

    def _apply_index(self, request, resultset=None):
        """ Apply the index to query parameters given in the request arg. """
        record = parseIndexRequest(request, self.id, self.query_options)

        if record.keys is None:
            return None

        return UnIndex._apply_index(self, request, resultset=resultset)

    def index_object(self, documentId, obj, threshold=None):
        """ wrapper to handle indexing of multiple attributes """

        res = self._index_object(documentId, obj, threshold)
        return res

    def _index_object(self, documentId, obj, threshold=None):
        """ index an object 'obj' with integer id 'i'

        Ideally, we've been passed a sequence of some sort that we
        can iterate over. If however, we haven't, we should do something
        useful with the results. In the case of a string, this means
        indexing the entire string as a keyword."""

        # First we need to see if there's anything interesting to look at
        # self.id is the name of the index, which is also the name of the
        # attribute we're interested in.  If the attribute is callable,
        # we'll do so.

        # get permuted keywords
        newKeywords = self._get_object_keywords(obj)

        oldKeywords = self._unindex.get(documentId, None)

        if oldKeywords is None:
            # we've got a new document, let's not futz around.
            try:
                for kw in newKeywords:
                    self.insertForwardIndexEntry(kw, documentId)
                if newKeywords:
                    self._unindex[documentId] = list(newKeywords)
            except TypeError:
                return 0
        else:
            # we have an existing entry for this document, and we need
            # to figure out if any of the keywords have actually changed
            if type(oldKeywords) is not OOSet:
                oldKeywords = OOSet(oldKeywords)
            newKeywords = OOSet(newKeywords)
            fdiff = difference(oldKeywords, newKeywords)
            rdiff = difference(newKeywords, oldKeywords)
            if fdiff or rdiff:
                # if we've got forward or reverse changes
                self._unindex[documentId] = list(newKeywords)
                if fdiff:
                    self.unindex_objectKeywords(documentId, fdiff)
                if rdiff:
                    for kw in rdiff:
                        self.insertForwardIndexEntry(kw, documentId)

        return 1

    def unindex_objectKeywords(self, documentId, keywords):
        """ carefully unindex the object with integer id 'documentId'"""

        if keywords is not None:
            for kw in keywords:
                self.removeForwardIndexEntry(kw, documentId)

    def unindex_object(self, documentId):
        """ carefully unindex the object with integer id 'documentId'"""

        keywords = self._unindex.get(documentId, None)
        self.unindex_objectKeywords(documentId, keywords)
        try:
            del self._unindex[documentId]
        except KeyError:
            LOG.debug('%s: Attempt to unindex nonexistent '
                      'document with id %s' %
                      (self.__class__.__name__, documentId),
                      error=sys.exc_info())

    def _get_object_keywords(self, obj):
        """ returns permutation list of object keywords """

        components = self.getIndexComponents()
        kw_list = []

        for c in components:
            kw = self._get_component_keywords(obj, c)
            # skip if keyword list is empty
            if not kw:
                continue
            kw = tuple([(c.id, k) for k in kw])
            kw_list.append(kw)

        # permute keyword list in order to support any combination and
        # number (n > 1) of components in query
        pkl = []
        c_list = product(*kw_list)

        for c in c_list:
            for r in range(MIN_COMPONENTS, len(c) + 1):
                p = combinations(c, r)
                pkl.extend(p)

        return tuple(pkl)

    def _get_component_keywords(self, obj, component):

        if component.meta_type == 'FieldIndex':
            attr = component.attributes[-1]
            try:
                datum = getattr(obj, attr)
                if safe_callable(datum):
                    datum = datum()
            except (AttributeError, TypeError):
                datum = _marker
            if isinstance(datum, list):
                datum = tuple(datum)
            return (datum,)

        elif component.meta_type == 'KeywordIndex':
            for attr in component.attributes:
                datum = []
                newKeywords = getattr(obj, attr, ())
                if safe_callable(newKeywords):
                    try:
                        newKeywords = newKeywords()
                    except AttributeError:
                        continue
                if not newKeywords and newKeywords is not False:
                    continue
                # Python 2.1 compat isinstance
                elif isinstance(newKeywords, basestring):
                    datum.append(newKeywords)
                else:
                    try:
                        # unique
                        newKeywords = set(newKeywords)
                    except TypeError:
                        # Not a sequence
                        datum.append(newKeywords)
                    else:
                        datum.extend(newKeywords)

            datum.sort()
            return tuple(datum)
        else:
            raise KeyError

    def getIndexComponents(self):
        """ return sequence of indexed attributes """
        return self._components.values()

    def getComponentIndexNames(self):
        """ returns component index names to composite """

        return tuple([c.id for c in self.getIndexComponents()])

    def getComponentIndexAttributes(self):
        """ returns list of attributes of each component index to composite"""

        return tuple([c.attributes for c in self.getIndexComponents()])

    def getIndexNames(self):
        """ returns index names that are catched by query substitution """
        return self.getComponentIndexNames()

    def make_query(self, query):
        """ optimize the query for supported index names """

        try:
            zc = aq_parent(aq_parent(self))
            skip = zc.getProperty('skip_compositeindex', False)
            if skip:
                LOG.debug('%s: skip composite query build %r' %
                          (self.__class__.__name__, zc))
                return query
        except AttributeError:
            pass

        if len(self) == 0:
            LOG.warn('%s is empty, skip composite query build %r' %
                     (self.__class__.__name__, self.id, zc))
            return query

        cquery = query.copy()
        components = self.getIndexComponents()

        # collect components matching query attributes
        # and check them for completeness
        c_records = []
        for c in components:
            query_options = QUERY_OPTIONS[c.meta_type]
            rec = parseIndexRequest(query, c.id, query_options)

            # not supported: 'not' parameter
            not_parm = rec.get('not', None)
            if not rec.keys and not_parm:
                continue

            # not supported: 'and' operator
            operator = rec.get('operator', self.useOperator)
            if rec.keys and operator == 'and':
                continue

            # continue if no keys in query were set
            if rec.keys is None:
                continue

            c_records.append((c.id, rec))

        # return if less than MIN_COMPONENTS query attributes were catched
        if len(c_records) < MIN_COMPONENTS:
            return query

        kw_list = []
        for c_id, rec in c_records:
            kw = rec.keys
            if not kw:
                continue
            if isinstance(kw, list):
                kw = tuple(kw)
            elif not isinstance(kw, tuple):
                kw = (kw,)
            kw = tuple([(c_id, k) for k in kw])
            kw_list.append(kw)

        # permute keyword list
        records = tuple(product(*kw_list))

        # substitude matching query attributes as composite index
        cquery.update({self.id: {'query': records}})

        # delete original matching query attributes from query
        for c_id, rec in c_records:
            if c_id in cquery:
                del cquery[c_id]

        #LOG.debug('%s: query build from %r' % (self.__class__.__name__,
        #                              [(c_id, rec.keys, rec.get('operator'))
        #                                   for c_id, rec in c_records]))

        return cquery

    def addComponent(self, c_id, c_meta_type, c_attributes):
        # Add a component object by 'c_id'.
        if c_id in self._components:
            raise KeyError('A component with this '
                           'name already exists: %s' % c_id)

        self._components[c_id] = Component(c_id,
                                           c_meta_type,
                                           c_attributes)
        self.clear()

    def delComponent(self, c_id):
        # Delete the component object specified by 'c_id'.
        if c_id not in self._components:
            raise KeyError('no such Component:  %s' % c_id)

        del self._components[c_id]

        self.clear()

    def saveComponents(self, components):
        # Change the component object specified by 'c_id'.
        for c in components:
            self.delComponent(c.old_id)
            self.addComponent(c.id, c.meta_type, c.attributes)

    def manage_addComponent(self, c_id, c_meta_type, c_attributes, URL1,
                            REQUEST=None, RESPONSE=None):
        """ add a new component """
        if len(c_id) == 0:
            raise RuntimeError('Length of component ID too short')
        if len(c_meta_type) == 0:
            raise RuntimeError('No component type set')

        self.addComponent(c_id, c_meta_type, c_attributes)

        if RESPONSE:
            RESPONSE.redirect(URL1 + '/manage_main?'
                              'manage_tabs_message=Component%20added')

    def manage_delComponents(self, del_ids=[], URL1=None,
                             REQUEST=None, RESPONSE=None):
        """ delete one or more components """
        if not del_ids:
            raise RuntimeError('No component selected')

        for c_id in del_ids:
            self.delComponent(c_id)

        if RESPONSE:
            RESPONSE.redirect(URL1 + '/manage_main?'
                              'manage_tabs_message=Component(s)%20deleted')

    def manage_saveComponents(self, components, URL1=None,
                              REQUEST=None, RESPONSE=None):
        """ save values of components """

        self.saveComponents(components)

        if RESPONSE:
            RESPONSE.redirect(URL1 + '/manage_main?'
                              'manage_tabs_message=Component(s)%20updated')

    def fastBuild(self, threshold=None):

        if threshold is None:
            threshold = 10000

        zc = aq_parent(aq_parent(aq_inner(self)))
        getIndex = zc._catalog.getIndex
        components = self.getIndexComponents()

        self.clear()

        class pseudoObject(object):
            pass

        counter = 0
        for rid in zc._catalog.paths.keys():
            # pseudo object
            obj = pseudoObject()
            for c in components:
                kw = getIndex(c.id).getEntryForObject(rid, _marker)
                if kw is not _marker:
                    for attr in c.attributes:
                        setattr(obj, attr, kw)

            self.index_object(rid, obj)
            del obj

            counter += 1
            if counter > threshold:
                transaction.savepoint(optimistic=True)
                self._p_jar.cacheGC()
                counter = 0

    def manage_fastBuild(self, threshold=None, URL1=None,
                         REQUEST=None, RESPONSE=None):
        """ fast build index directly via catalog brains and attribute values
            of matching field and keyword indexes """

        tt = time.time()
        ct = time.clock()

        self.fastBuild(threshold)

        tt = time.time() - tt
        ct = time.clock() - ct

        if RESPONSE:
            RESPONSE.redirect(URL1 + '/manage_main?'
                              'manage_tabs_message=ComponentIndex%%20fast%%20'
                              'reindexed%%20in%%20%.3f%%20'
                              'seconds%%20(%.3f%%20cpu)' % (tt, ct))

    manage = manage_main = DTMLFile('dtml/manageCompositeIndex', globals())
    manage_main._setName('manage_main')
    manage_browse = DTMLFile('../dtml/browseIndex', globals())


manage_addCompositeIndexForm = DTMLFile('dtml/addCompositeIndex', globals())


def manage_addCompositeIndex(self, id, extra=None, REQUEST=None,
                             RESPONSE=None, URL3=None):
    """Add a composite index"""
    return self.manage_addIndex(id, 'CompositeIndex', extra=extra,
                                REQUEST=REQUEST, RESPONSE=RESPONSE, URL1=URL3)