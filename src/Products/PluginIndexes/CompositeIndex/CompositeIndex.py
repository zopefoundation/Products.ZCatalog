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
import time
import urllib
from itertools import combinations
from itertools import product
from time import process_time

import transaction
from Acquisition import aq_inner
from Acquisition import aq_parent
from App.special_dtml import DTMLFile
from BTrees.OOBTree import OOSet
from BTrees.OOBTree import difference
from Persistence import PersistentMapping
from zope.interface import implementer

from Products.PluginIndexes.interfaces import ITransposeQuery
from Products.PluginIndexes.KeywordIndex.KeywordIndex import KeywordIndex
from Products.PluginIndexes.unindex import _marker
from Products.ZCatalog.query import IndexQuery


LOG = logging.getLogger('CompositeIndex')

QUERY_OPTIONS = {
    'BooleanIndex': ('query', 'range', 'not'),
    'FieldIndex': ('query', 'range', 'not'),
    'KeywordIndex': ('query', 'range', 'not', 'operator'),
}
QUERY_OPERATORS = {
    'BooleanIndex': (('and', 'or'), 'or'),
    'FieldIndex': (('and', 'or'), 'or'),
    'KeywordIndex': (('and', 'or'), 'or'),
}

MIN_COMPONENTS = 2


def tuple_cast(kw):
    if isinstance(kw, list):
        kw = tuple(kw)
    elif not isinstance(kw, tuple):
        kw = (kw,)
    return kw


def collect(recs, not_cid=None):
    kw_list = []
    for c_id, rec in recs:
        if c_id != not_cid:
            keys = list(rec.keys)
            if not_cid is not None:
                keys.extend(rec.get('not', []))
        elif c_id == not_cid:
            keys = rec.get('not', [])
        if not keys:
            continue
        keys = tuple_cast(keys)
        kw = tuple([(c_id, k) for k in keys])
        kw_list.append(kw)
    return kw_list


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
        return list(map(self.get, self._keys))


class Component:

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
        return ('<id: {0.id}; metatype: {0.meta_type}; '
                'attributes: {0.attributes}>').format(self)


@implementer(ITransposeQuery)
class CompositeIndex(KeywordIndex):

    """Index for composition of simple fields.
       or sequences of items
    """

    meta_type = 'CompositeIndex'

    manage_options = (
        {'label': 'Settings',
         'action': 'manage_main'},
        {'label': 'Browse',
         'action': 'manage_browse'},
    )

    query_options = ('query', 'operator', 'not')

    def __init__(self, id, ignore_ex=None, call_methods=None,
                 extra=None, caller=None):
        """Create an composite index"""

        self.id = id
        self.ignore_ex = ignore_ex        # currently unimplemented
        self.call_methods = call_methods

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

    def _index_object(self, documentId, obj, threshold=None, attr=''):

        # get permuted keywords
        newKeywords = self._get_permuted_keywords(obj)

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
            if not isinstance(oldKeywords, OOSet):
                oldKeywords = OOSet(oldKeywords)
            newKeywords = OOSet(newKeywords)
            fdiff = difference(oldKeywords, newKeywords)
            rdiff = difference(newKeywords, oldKeywords)
            if fdiff or rdiff:
                # if we've got forward or reverse changes
                if newKeywords:
                    self._unindex[documentId] = list(newKeywords)
                else:
                    del self._unindex[documentId]
                if fdiff:
                    self.unindex_objectKeywords(documentId, fdiff)
                if rdiff:
                    for kw in rdiff:
                        self.insertForwardIndexEntry(kw, documentId)
        return 1

    def _get_permuted_keywords(self, obj):
        """ returns permutation tuple of object keywords """

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
            # last attribute is the winner if value is not None
            for attr in component.attributes:
                datum = self._get_object_datum(obj, attr)
                if datum is None:
                    continue
            if datum is None:
                return ()
            if isinstance(datum, list):
                datum = tuple(datum)
            return (datum,)

        elif component.meta_type == 'KeywordIndex':
            # last attribute is the winner
            attr = component.attributes[-1]
            datum = self._get_object_keywords(obj, attr)
            if isinstance(datum, list):
                datum = tuple(datum)
            return datum

        elif component.meta_type == 'BooleanIndex':
            # last attribute is the winner
            attr = component.attributes[-1]
            datum = self._get_object_datum(obj, attr)
            if datum is not _marker:
                datum = int(bool(datum))
            return (datum,)

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
        """ returns index names that are caught by query substitution """
        return self.getComponentIndexNames()

    def make_query(self, query):
        """ optimize the query for supported index names """

        try:
            zc = aq_parent(aq_parent(self))
            skip = zc.getProperty('skip_compositeindex', False)
            if skip:
                LOG.debug('%(context)s: skip composite query build '
                          'for %(zcatalog)r', dict(
                              context=self.__class__.__name__,
                              zcatalog=zc))
                return query
        except AttributeError:
            pass

        if len(self) == 0:
            return query

        cquery = query.copy()
        components = self.getIndexComponents()

        # collect components matching query attributes
        # and check them for completeness
        c_records = []

        # component ids containing 'not' operator
        not_cids = []

        for c in components:
            query_options = QUERY_OPTIONS[c.meta_type]
            query_operators = QUERY_OPERATORS[c.meta_type]
            rec = IndexQuery(query, c.id, query_options,
                             query_operators[0], query_operators[1])
            opr = None

            # not supported: 'range' parameter
            range_parm = rec.get('range', None)
            if range_parm:
                opr = 'range'
            if rec.get('usage', None):
                # see if any usage params are sent to field
                opr = rec.usage.lower().split(':')
            if opr == 'range':
                continue

            # not supported: 'and' operator
            if rec.keys and rec.operator == 'and':
                continue

            # continue if no keys in query were set
            if rec.keys is None:
                continue

            # convert rec keys to int for BooleanIndex
            if c.meta_type == 'BooleanIndex':
                rec.keys = [int(bool(v)) for v in rec.keys[:]]

            # rec with 'not' parameter
            not_parm = rec.get('not', None)
            if not_parm:
                # not supported: 'pure not'
                if len(rec.keys) == 0:
                    continue
                not_cids.append(c.id)
                if c.meta_type == 'BooleanIndex':
                    not_parm = [int(bool(v)) for v in not_parm[:]]
                    rec.set('not', not_parm)

            c_records.append((c.id, rec))

        # return if less than MIN_COMPONENTS query attributes were caught
        if len(c_records) < MIN_COMPONENTS:
            return query

        records = ()
        kw_list = collect(c_records)
        # permute keyword list
        records = tuple(product(*kw_list))

        not_records = set()
        for c_id in not_cids:
            kw_list = collect(c_records, not_cid=c_id)
            if kw_list:
                not_records.update(product(*kw_list))
        # permute keyword list for 'not' operator
        not_records = tuple(not_records)

        # substitute matching query attributes as composite index
        if records and not_records:
            cquery.update({self.id: {'query': records, 'not': not_records}})
        elif records:
            cquery.update({self.id: {'query': records}})
        elif not_records:
            cquery.update({self.id: {'not': not_records}})

        # delete original matching query attributes from query
        for c_id, rec in c_records:
            if c_id in cquery:
                del cquery[c_id]

        return cquery

    def addComponent(self, c_id, c_meta_type, c_attributes):
        # Add a component object by 'c_id'.
        if c_id in self._components:
            raise KeyError('A component with this '
                           'name already exists: {}'.format(c_id))

        self._components[c_id] = Component(c_id,
                                           c_meta_type,
                                           c_attributes)
        self.clear()

    def delComponent(self, c_id):
        # Delete the component object specified by 'c_id'.
        if c_id not in self._components:
            raise KeyError(f'no such Component:  {c_id}')

        del self._components[c_id]

        self.clear()

    def saveComponents(self, components):
        # Change the component object specified by 'c_id'.
        for c in components:
            self.delComponent(c.old_id)
            self.addComponent(c.id, c.meta_type, c.attributes)

        # better safe than sorry
        self.clear()

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

    def manage_delComponents(self, del_ids=(), URL1=None,
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

        class pseudoObject:
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
        ct = process_time()

        self.fastBuild(threshold)

        tt = time.time() - tt
        ct = process_time() - ct

        if RESPONSE:
            msg = ('ComponentIndex fast reindexed '
                   'in {:.3f}s ({:.3f}s cpu time)').format(tt, ct)
            param = urllib.parse.urlencode({'manage_tabs_message': msg})

            RESPONSE.redirect(URL1 + '/manage_main?' + param)

    manage = manage_main = DTMLFile('dtml/manageCompositeIndex', globals())
    manage_main._setName('manage_main')
    manage_browse = DTMLFile('../dtml/browseIndex', globals())


manage_addCompositeIndexForm = DTMLFile('dtml/addCompositeIndex', globals())


def manage_addCompositeIndex(self, id, extra=None, REQUEST=None,
                             RESPONSE=None, URL3=None):
    """Add a composite index"""
    return self.manage_addIndex(id, 'CompositeIndex', extra=extra,
                                REQUEST=REQUEST, RESPONSE=RESPONSE, URL1=URL3)
