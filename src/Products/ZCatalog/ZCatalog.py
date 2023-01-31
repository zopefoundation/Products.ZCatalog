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
""" ZCatalog product
"""

import logging
import operator
import sys
import time
from time import process_time
from urllib.parse import quote

import transaction
from AccessControl.class_init import InitializeClass
from AccessControl.Permission import getPermissionIdentifier
from AccessControl.Permissions import manage_zcatalog_entries
from AccessControl.Permissions import manage_zcatalog_indexes
from AccessControl.Permissions import search_zcatalog
from AccessControl.SecurityInfo import ClassSecurityInfo
from Acquisition import Implicit
from Acquisition import aq_base
from Acquisition import aq_parent
from App.special_dtml import DTMLFile
from DateTime.DateTime import DateTime
from DocumentTemplate._DocumentTemplate import InstanceDict
from DocumentTemplate._DocumentTemplate import TemplateDict
from DocumentTemplate.DT_Util import Eval
from DocumentTemplate.security import RestrictedDTML
from OFS.Folder import Folder
from OFS.ObjectManager import ObjectManager
from Persistence import Persistent
from Products.PageTemplates.PageTemplateFile import PageTemplateFile
from zExceptions import BadRequest
from ZODB.POSException import ConflictError
from zope.interface import implementer
from ZTUtils.Lazy import LazyMap

from Products.PluginIndexes.interfaces import IPluggableIndex
from Products.ZCatalog.Catalog import Catalog
from Products.ZCatalog.Catalog import CatalogError
from Products.ZCatalog.interfaces import IZCatalog
from Products.ZCatalog.plan import PriorityMap
from Products.ZCatalog.ProgressHandler import ZLogHandler
from Products.ZCatalog.ZCatalogIndexes import ZCatalogIndexes


_marker = object()
LOG = logging.getLogger('Zope.ZCatalog')

manage_addZCatalogForm = DTMLFile('dtml/addZCatalog', globals())


def manage_addZCatalog(self, id, title, vocab_id=None, REQUEST=None):
    """Add a ZCatalog object. The vocab_id argument is ignored.
    """
    id = str(id)
    title = str(title)
    c = ZCatalog(id, title, container=self)
    self._setObject(id, c)
    if REQUEST is not None:
        return self.manage_main(self, REQUEST, update_menu=1)


@implementer(IZCatalog)
class ZCatalog(Folder, Persistent, Implicit):
    """ZCatalog object

    A ZCatalog contains arbirary index like references to Zope
    objects.  ZCatalog's can index either 'Field' values of object, or
    'Text' values.

    ZCatalog does not store references to the objects themselves, but
    rather to a unique identifier that defines how to get to the
    object.  In Zope, this unique idenfier is the object's relative
    path to the ZCatalog (since two Zope object's cannot have the same
    URL, this is an excellent unique qualifier in Zope).

    Most of the dirty work is done in the _catalog object, which is an
    instance of the Catalog class.  An interesting feature of this
    class is that it is not Zope specific.  You can use it in any
    Python program to catalog objects.
    """

    security = ClassSecurityInfo()
    security.setPermissionDefault(manage_zcatalog_entries, ('Manager', ))
    security.setPermissionDefault(manage_zcatalog_indexes, ('Manager', ))
    security.setPermissionDefault(search_zcatalog, ('Anonymous', 'Manager'))
    security.declareProtected(search_zcatalog, 'all_meta_types')

    meta_type = 'ZCatalog'
    zmi_icon = 'fa fa-search'

    manage_options = (
        {'label': 'Contents', 'action': 'manage_main'},
        {'label': 'Catalog', 'action': 'manage_catalogView'},
        {'label': 'Indexes', 'action': 'manage_catalogIndexes'},
        {'label': 'Metadata', 'action': 'manage_catalogSchema'},
        {'label': 'Find Objects', 'action': 'manage_catalogFind'},
        {'label': 'Advanced', 'action': 'manage_catalogAdvanced'},
        {'label': 'Query Report', 'action': 'manage_catalogReport'},
        {'label': 'Query Plan', 'action': 'manage_catalogPlan'},
        {'label': 'Security', 'action': 'manage_access'},
        {'label': 'Ownership', 'action': 'manage_owner'},
    )

    security.declareProtected(manage_zcatalog_entries, 'manage_main')

    security.declareProtected(manage_zcatalog_entries, 'manage_catalogView')
    manage_catalogView = DTMLFile('dtml/catalogView', globals())

    security.declareProtected(manage_zcatalog_entries, 'manage_catalogIndexes')
    manage_catalogIndexes = PageTemplateFile('zpt/catalogIndexes', globals())

    security.declareProtected(manage_zcatalog_entries, 'manage_catalogSchema')
    manage_catalogSchema = DTMLFile('dtml/catalogSchema', globals())

    security.declareProtected(manage_zcatalog_entries, 'manage_catalogFind')
    manage_catalogFind = DTMLFile('dtml/catalogFind', globals())

    security.declareProtected(manage_zcatalog_entries,
                              'manage_catalogAdvanced')
    manage_catalogAdvanced = DTMLFile('dtml/catalogAdvanced', globals())

    security.declareProtected(manage_zcatalog_entries, 'manage_catalogReport')
    manage_catalogReport = DTMLFile('dtml/catalogReport', globals())

    security.declareProtected(manage_zcatalog_entries, 'manage_catalogPlan')
    manage_catalogPlan = DTMLFile('dtml/catalogPlan', globals())

    security.declareProtected(manage_zcatalog_entries,
                              'manage_objectInformation')
    manage_objectInformation = DTMLFile('dtml/catalogObjectInformation',
                                        globals())

    # this stuff is so the find machinery works
    meta_types = ()  # Sub-object types that are specific to this object

    Indexes = ZCatalogIndexes()

    threshold = 10000
    long_query_time = 0.1

    # vocabulary and vocab_id are left for backwards
    # compatibility only, they are not used anymore
    vocabulary = None
    vocab_id = ''

    _v_total = 0
    _v_transaction = None

    def __init__(self, id, title='', vocab_id=None, container=None):
        # ZCatalog no longer cares about vocabularies
        # so the vocab_id argument is ignored (Casey)

        if container is not None:
            self = self.__of__(container)
        self.id = id
        self.title = title
        self.threshold = 10000
        self.long_query_time = 0.1  # in seconds
        self._v_total = 0
        self._catalog = Catalog()

    def __len__(self):
        return len(self._catalog)

    @security.protected(manage_zcatalog_entries)
    def manage_edit(self, RESPONSE, URL1, threshold=1000, REQUEST=None):
        """ edit the catalog """
        if not isinstance(threshold, int):
            threshold = int(threshold)
        self.threshold = threshold

        RESPONSE.redirect(
            URL1 + '/manage_main?manage_tabs_message=Catalog%20Changed')

    @security.protected(manage_zcatalog_entries)
    def manage_subbingToggle(self, REQUEST, RESPONSE, URL1):
        """ toggle subtransactions """
        if self.threshold:
            self.threshold = None
        else:
            self.threshold = 10000

        RESPONSE.redirect(
            URL1
            + '/manage_catalogAdvanced?manage_tabs_message=Catalog%20Changed')

    @security.protected(manage_zcatalog_entries)
    def manage_catalogObject(self, REQUEST, RESPONSE, URL1, urls=None):
        """ index Zope object(s) that 'urls' point to """
        if urls:
            if isinstance(urls, str):
                urls = (urls, )

            for url in urls:
                obj = self.resolve_path(url)
                if obj is None and hasattr(self, 'REQUEST'):
                    obj = self.resolve_url(url, REQUEST)
                if obj is not None:
                    self.catalog_object(obj, url)

        RESPONSE.redirect(
            URL1
            + '/manage_catalogView?manage_tabs_message=Object%20Cataloged')

    @security.protected(manage_zcatalog_entries)
    def manage_uncatalogObject(self, REQUEST, RESPONSE, URL1, urls=None):
        """ removes Zope object(s) 'urls' from catalog """

        if urls:
            if isinstance(urls, str):
                urls = (urls, )

            for url in urls:
                self.uncatalog_object(url)

        RESPONSE.redirect(
            URL1
            + '/manage_catalogView?manage_tabs_message=Object%20Uncataloged')

    @security.protected(manage_zcatalog_entries)
    def manage_catalogReindex(self, REQUEST, RESPONSE, URL1):
        """ clear the catalog, then re-index everything """

        elapse = time.time()
        c_elapse = process_time()

        pgthreshold = self._getProgressThreshold()
        handler = (pgthreshold > 0) and ZLogHandler(pgthreshold) or None
        self.refreshCatalog(clear=1, pghandler=handler)

        elapse = time.time() - elapse
        c_elapse = process_time() - c_elapse

        RESPONSE.redirect(
            URL1
            + '/manage_catalogAdvanced?manage_tabs_message='
            + quote('Catalog Updated \n'
                    'Total time: %r\n'
                    'Total CPU time: %r' % (elapse, c_elapse)))

    @security.protected(manage_zcatalog_entries)
    def refreshCatalog(self, clear=0, pghandler=None):
        """ re-index everything we can find """

        cat = self._catalog
        paths = cat.paths.values()
        if clear:
            paths = tuple(paths)
            cat.clear()

        num_objects = len(paths)
        if pghandler:
            pghandler.init('Refreshing catalog: %s' % self.absolute_url(1),
                           num_objects)

        for i in range(num_objects):
            if pghandler:
                pghandler.report(i)

            p = paths[i]
            obj = self.resolve_path(p)
            if obj is None:
                obj = self.resolve_url(p, self.REQUEST)
            if obj is not None:
                try:
                    self.catalog_object(obj, p, pghandler=pghandler)
                except ConflictError:
                    raise
                except Exception:
                    LOG.error('Recataloging object at %s failed', p,
                              exc_info=sys.exc_info())

        if pghandler:
            pghandler.finish()

    @security.protected(manage_zcatalog_entries)
    def manage_catalogClear(self, REQUEST=None, RESPONSE=None, URL1=None):
        """ clears the whole enchilada """
        self._catalog.clear()

        if REQUEST and RESPONSE:
            RESPONSE.redirect(URL1 + (
                '/manage_catalogAdvanced?'
                'manage_tabs_message=Catalog%20Cleared'))

    @security.protected(manage_zcatalog_entries)
    def manage_catalogFoundItems(self, REQUEST, RESPONSE, URL2, URL1,
                                 obj_metatypes=None,
                                 obj_ids=None, obj_searchterm=None,
                                 obj_expr=None, obj_mtime=None,
                                 obj_mspec=None, obj_roles=None,
                                 obj_permission=None):
        """ Find object according to search criteria and Catalog them
        """
        elapse = time.time()
        c_elapse = process_time()

        obj = REQUEST.PARENTS[1]
        path = '/'.join(obj.getPhysicalPath())

        self.ZopeFindAndApply(obj,
                              obj_metatypes=obj_metatypes,
                              obj_ids=obj_ids,
                              obj_searchterm=obj_searchterm,
                              obj_expr=obj_expr,
                              obj_mtime=obj_mtime,
                              obj_mspec=obj_mspec,
                              obj_permission=obj_permission,
                              obj_roles=obj_roles,
                              search_sub=1,
                              REQUEST=REQUEST,
                              apply_func=self.catalog_object,
                              apply_path=path)

        elapse = time.time() - elapse
        c_elapse = process_time() - c_elapse

        RESPONSE.redirect(
            URL1
            + '/manage_catalogView?manage_tabs_message='
            + quote('Catalog Updated\n'
                    'Total time: %r\n'
                    'Total CPU time: %r' % (elapse, c_elapse)))

    @security.protected(manage_zcatalog_entries)
    def manage_addColumn(self, name, REQUEST=None, RESPONSE=None, URL1=None):
        """ add a column """
        self.addColumn(name)

        if REQUEST and RESPONSE:
            RESPONSE.redirect(
                URL1
                + '/manage_catalogSchema?manage_tabs_message=Column%20Added')

    @security.protected(manage_zcatalog_entries)
    def manage_delColumn(self, names, REQUEST=None, RESPONSE=None, URL1=None):
        """ delete a column or some columns """
        if isinstance(names, str):
            names = (names, )

        for name in names:
            self.delColumn(name)

        if REQUEST and RESPONSE:
            RESPONSE.redirect(
                URL1
                + '/manage_catalogSchema?manage_tabs_message=Column%20Deleted')

    @security.protected(manage_zcatalog_entries)
    def manage_addIndex(self, name, type, extra=None,
                        REQUEST=None, RESPONSE=None, URL1=None):
        """add an index """
        self.addIndex(name, type, extra)

        if REQUEST and RESPONSE:
            RESPONSE.redirect(
                URL1
                + '/manage_catalogIndexes?manage_tabs_message=Index%20Added')

    @security.protected(manage_zcatalog_entries)
    def manage_delIndex(self, ids=None,
                        REQUEST=None, RESPONSE=None, URL1=None):
        """ delete an index or some indexes """
        if not ids:
            raise BadRequest('No items specified')

        if isinstance(ids, str):
            ids = (ids, )

        for name in ids:
            self.delIndex(name)

        if REQUEST and RESPONSE:
            RESPONSE.redirect(
                URL1
                + '/manage_catalogIndexes?manage_tabs_message=Index%20Deleted')

    @security.protected(manage_zcatalog_entries)
    def manage_clearIndex(self, ids=None,
                          REQUEST=None, RESPONSE=None, URL1=None):
        """ clear an index or some indexes """
        if not ids:
            raise BadRequest('No items specified')

        if isinstance(ids, str):
            ids = (ids, )

        for name in ids:
            self.clearIndex(name)

        if REQUEST and RESPONSE:
            RESPONSE.redirect(
                URL1
                + '/manage_catalogIndexes?manage_tabs_message=Index%20Cleared')

    @security.protected(manage_zcatalog_entries)
    def reindexIndex(self, name, REQUEST, pghandler=None):
        # This method does the actual reindexing of indexes.
        # `name` can be the name of an index or a list of names.
        idxs = (name, ) if isinstance(name, str) else name
        paths = self._catalog.uids.keys()

        i = 0
        if pghandler:
            pghandler.init(f'reindexing {idxs}', len(paths))

        for p in paths:
            i += 1
            if pghandler:
                pghandler.report(i)

            obj = self.resolve_path(p)
            if obj is None:
                obj = self.resolve_url(p, REQUEST)
            if obj is None:
                LOG.error('reindexIndex could not resolve '
                          'an object from the uid %r.', p)
            else:
                # don't update metadata when only reindexing a single
                # index via the UI
                self.catalog_object(obj, p, idxs=idxs,
                                    update_metadata=0, pghandler=pghandler)

        if pghandler:
            pghandler.finish()

    @security.protected(manage_zcatalog_entries)
    def manage_reindexIndex(self, ids=None, REQUEST=None, RESPONSE=None,
                            URL1=None):
        """Reindex index(es) from a ZCatalog"""
        if not ids:
            raise BadRequest('No items specified')

        pgthreshold = self._getProgressThreshold()
        handler = (pgthreshold > 0) and ZLogHandler(pgthreshold) or None
        self.reindexIndex(ids, REQUEST, handler)

        if REQUEST and RESPONSE:
            RESPONSE.redirect(
                URL1
                + '/manage_catalogIndexes'
                  '?manage_tabs_message=Reindexing%20Performed')

    @security.private
    def maintain_zodb_cache(self):
        # self.threshold represents the number of times that catalog_object
        # needs to be called in order for the catalog to commit
        # a subtransaction.
        if self.threshold is not None:
            # figure out whether or not to commit a subtransaction.
            t = id(transaction.get())
            if t != self._v_transaction:
                self._v_total = 0
            self._v_transaction = t
            self._v_total = self._v_total + 1
            # increment the _v_total counter for this thread only and get a
            # reference to the current transaction.  the _v_total counter is
            # zeroed if we notice that we're in a different transaction than
            # the last one that came by. The semantics here mean that we
            # should GC the cache if our threshhold is exceeded within the
            # boundaries of the current transaction.
            if self._v_total > self.threshold:
                self._p_jar.cacheGC()
                self._v_total = 0
                return True
        return False

    @security.protected(manage_zcatalog_entries)
    def catalog_object(self, obj, uid=None, idxs=None, update_metadata=1,
                       pghandler=None):
        if uid is None:
            try:
                uid = obj.getPhysicalPath
            except AttributeError:
                raise CatalogError(
                    "A cataloged object must support the 'getPhysicalPath' "
                    "method if no unique id is provided when cataloging")
            else:
                uid = '/'.join(uid())
        elif not isinstance(uid, str):
            raise CatalogError('The object unique id must be a string.')

        self._catalog.catalogObject(obj, uid, None, idxs,
                                    update_metadata=update_metadata)
        # None passed in to catalogObject as third argument indicates
        # that we shouldn't try to commit subtransactions within any
        # indexing code.  We throw away the result of the call to
        # catalogObject (which is a word count), because it's
        # worthless to us here.

        if self.maintain_zodb_cache():
            transaction.savepoint(optimistic=True)
            if pghandler:
                pghandler.info('committing subtransaction')

    @security.protected(manage_zcatalog_entries)
    def uncatalog_object(self, uid):
        self._catalog.uncatalogObject(uid)

    @security.protected(search_zcatalog)
    def uniqueValuesFor(self, name):
        # Return the unique values for a given FieldIndex
        return self._catalog.uniqueValuesFor(name)

    @security.protected(search_zcatalog)
    def getpath(self, rid):
        # Return the path to a cataloged object given a 'data_record_id_'
        return self._catalog.paths[rid]

    @security.protected(search_zcatalog)
    def getrid(self, path, default=None):
        # Return 'data_record_id_' the to a cataloged object given a 'path'
        return self._catalog.uids.get(path, default)

    @security.protected(search_zcatalog)
    def getobject(self, rid, REQUEST=None):
        # Return a cataloged object given a 'data_record_id_'
        return aq_parent(self).unrestrictedTraverse(self.getpath(rid))

    @security.protected(search_zcatalog)
    def getMetadataForUID(self, uid):
        # return the correct metadata given the uid, usually the path
        rid = self._catalog.uids[uid]
        return self._catalog.getMetadataForRID(rid)

    @security.protected(search_zcatalog)
    def getIndexDataForUID(self, uid):
        # return the current index contents given the uid, usually the path
        rid = self._catalog.uids[uid]
        return self._catalog.getIndexDataForRID(rid)

    @security.protected(search_zcatalog)
    def getMetadataForRID(self, rid):
        # return the correct metadata for the cataloged record id
        return self._catalog.getMetadataForRID(int(rid))

    @security.protected(search_zcatalog)
    def getIndexDataForRID(self, rid):
        # return the current index contents for the specific rid
        return self._catalog.getIndexDataForRID(rid)

    @security.protected(search_zcatalog)
    def getAllBrains(self):
        # return a generator of brains for all cataloged objects
        for rid in self._catalog.data:
            yield self._catalog[rid]

    @security.protected(search_zcatalog)
    def searchAll(self):
        # the result of a search for all documents
        return LazyMap(self._catalog.__getitem__,
                       self._catalog.data.keys(),
                       len(self))

    @security.protected(search_zcatalog)
    def schema(self):
        return self._catalog.schema.keys()

    @security.protected(search_zcatalog)
    def indexes(self):
        return self._catalog.indexes.keys()

    @security.protected(search_zcatalog)
    def index_objects(self):
        # This method returns unwrapped indexes!
        # You should probably use getIndexObjects instead
        return self._catalog.indexes.values()

    @security.protected(manage_zcatalog_indexes)
    def getIndexObjects(self):
        # Return a list of wrapped(!) indexes
        getIndex = self._catalog.getIndex
        return [getIndex(name) for name in self.indexes()]

    def _searchable_arguments(self):
        r = {}
        n = {'optional': 1}
        for name in self._catalog.indexes.keys():
            r[name] = n
        return r

    def _searchable_result_columns(self):
        r = []
        for name in self._catalog.schema.keys():
            i = {}
            i['name'] = name
            i['type'] = 's'
            i['parser'] = str
            i['width'] = 8
            r.append(i)
        r.append({'name': 'data_record_id_',
                  'type': 's',
                  'parser': str,
                  'width': 8})
        return r

    @security.protected(search_zcatalog)
    def searchResults(self, query=None, **kw):
        """Search the catalog.

        Search terms can be passed as a query or as keyword arguments.
        """
        return self._catalog.searchResults(query, **kw)

    security.declareProtected(search_zcatalog, '__call__')
    __call__ = searchResults

    @security.protected(search_zcatalog)
    def search(self, query,
               sort_index=None, reverse=0, limit=None, merge=1):
        """Programmatic search interface, use for searching the catalog from
        scripts.

        query:      Dictionary containing catalog query
        sort_index: Name of sort index
        reverse:    Reverse sort order?
        limit:      Limit sorted result count (optimization hint)
        merge:      Return merged results (like searchResults) or raw
                    results for later merging.
        """
        if sort_index is not None:
            sort_index = self._catalog.indexes[sort_index]
        return self._catalog.search(
            query, sort_index, reverse, limit, merge)

    @security.protected(search_zcatalog)
    def valid_roles(self):
        # Return list of valid roles
        obj = self
        roles = set()
        x = 0
        while x < 100:
            if hasattr(obj, '__ac_roles__'):
                for role in obj.__ac_roles__:
                    roles.add(role)
            obj = aq_parent(obj)
            if obj is None:
                break
            x = x + 1
        roles = sorted(roles)
        return roles

    @security.protected(manage_zcatalog_entries)
    def ZopeFindAndApply(self, obj, obj_ids=None, obj_metatypes=None,
                         obj_searchterm=None, obj_expr=None,
                         obj_mtime=None, obj_mspec=None,
                         obj_permission=None, obj_roles=None,
                         search_sub=0,
                         REQUEST=None, result=None, pre='',
                         apply_func=None, apply_path=''):
        """Zope Find interface and apply

        This is a *great* hack.  Zope find just doesn't do what we
        need here; the ability to apply a method to all the objects
        *as they're found* and the need to pass the object's path into
        that method.
        """

        if result is None:
            result = []

            if obj_metatypes and 'all' in obj_metatypes:
                obj_metatypes = None

            if obj_mtime and isinstance(obj_mtime, str):
                obj_mtime = DateTime(obj_mtime).timeTime()

            if obj_permission:
                obj_permission = getPermissionIdentifier(obj_permission)

            if obj_roles and isinstance(obj_roles, str):
                obj_roles = [obj_roles]

            if obj_expr:
                # Setup expr machinations
                md = td()
                obj_expr = (Eval(obj_expr), md, md._push, md._pop)

        base = aq_base(obj)

        if not hasattr(base, 'objectItems'):
            return result
        try:
            items = obj.objectItems()
        except Exception:
            return result

        try:
            add_result = result.append
        except Exception:
            raise AttributeError(repr(result))

        for id, ob in items:
            if pre:
                p = f"{pre}/{id}"
            else:
                p = id

            dflag = 0
            if hasattr(ob, '_p_changed') and (ob._p_changed is None):
                dflag = 1

            bs = aq_base(ob)

            if ((not obj_ids or absattr(bs.id) in obj_ids)
                    and (not obj_metatypes or (hasattr(bs, 'meta_type')
                         and bs.meta_type in obj_metatypes))
                    and (not obj_searchterm
                         or (hasattr(ob, 'PrincipiaSearchSource')
                             and ob.PrincipiaSearchSource().find(obj_searchterm) >= 0))  # noqa: E501
                    and (not obj_expr
                         or expr_match(ob, obj_expr))
                    and (not obj_mtime
                         or mtime_match(ob, obj_mtime, obj_mspec))
                    and ((not obj_permission or not obj_roles)
                         or role_match(ob, obj_permission, obj_roles))):
                if apply_func:
                    apply_func(ob, (apply_path + '/' + p))
                else:
                    add_result((p, ob))
                    dflag = 0

            if search_sub and hasattr(bs, 'objectItems'):
                self.ZopeFindAndApply(ob, obj_ids, obj_metatypes,
                                      obj_searchterm, obj_expr,
                                      obj_mtime, obj_mspec,
                                      obj_permission, obj_roles,
                                      search_sub,
                                      REQUEST, result, p,
                                      apply_func, apply_path)
            if dflag:
                ob._p_deactivate()

        return result

    @security.protected(search_zcatalog)
    def resolve_url(self, path, REQUEST):
        # Attempt to resolve a url into an object in the Zope
        # namespace. The url may be absolute or a catalog path
        # style url. If no object is found, None is returned.
        # No exceptions are raised.
        if REQUEST:
            script = REQUEST.script
            if path.find(script) != 0:
                path = f'{script}/{path}'
            try:
                return REQUEST.resolve_url(path)
            except Exception:
                pass

    @security.protected(search_zcatalog)
    def resolve_path(self, path):
        # Attempt to resolve a url into an object in the Zope
        # namespace. The url may be absolute or a catalog path
        # style url. If no object is found, None is returned.
        # No exceptions are raised.
        try:
            return self.unrestrictedTraverse(path)
        except Exception:
            pass

    @security.protected(manage_zcatalog_entries)
    def manage_normalize_paths(self, REQUEST):
        """Ensure that all catalog paths are full physical paths

        This should only be used with ZCatalogs in which all paths can
        be resolved with unrestrictedTraverse."""

        paths = self._catalog.paths
        uids = self._catalog.uids
        unchanged = 0
        fixed = []
        removed = []

        for path, rid in uids.items():
            ob = None
            if path[:1] == '/':
                ob = self.resolve_url(path[1:], REQUEST)
            if ob is None:
                ob = self.resolve_url(path, REQUEST)
                if ob is None:
                    removed.append(path)
                    continue
            ppath = '/'.join(ob.getPhysicalPath())
            if path != ppath:
                fixed.append((path, ppath))
            else:
                unchanged = unchanged + 1

        for path, ppath in fixed:
            rid = uids[path]
            del uids[path]
            paths[rid] = ppath
            uids[ppath] = rid
        for path in removed:
            self.uncatalog_object(path)

    @security.protected(manage_zcatalog_entries)
    def manage_setProgress(self, pgthreshold=0, RESPONSE=None, URL1=None):
        """Set parameter to perform logging of reindexing operations very
           'pgthreshold' objects
        """
        self.pgthreshold = pgthreshold
        if RESPONSE:
            RESPONSE.redirect(URL1 + '/manage_catalogAdvanced?'
                              'manage_tabs_message=Catalog%20Changed')

    def _getProgressThreshold(self):
        if not hasattr(self, 'pgthreshold'):
            self.pgthreshold = 0
        return self.pgthreshold

    # Indexing methods

    @security.protected(manage_zcatalog_indexes)
    def addIndex(self, name, type, extra=None):
        if IPluggableIndex.providedBy(type):
            self._catalog.addIndex(name, type)
            return

        # Convert the type by finding an appropriate product which supports
        # this interface by that name.  Bleah
        products = ObjectManager.all_meta_types(self,
                                                interfaces=(IPluggableIndex, ))

        p = None

        for prod in products:
            if prod['name'] == type:
                p = prod
                break

        if p is None:
            raise ValueError("Index of type %s not found" % type)

        base = p['instance']

        if base is None:
            raise ValueError("Index type %s does not support addIndex" % type)

        # This code is *really* lame but every index type has its own
        # function signature *sigh* and there is no common way to pass
        # additional parameters to the constructor. The suggested way
        # for new index types is to use an "extra" record.

        if 'extra' in base.__init__.__code__.co_varnames:
            index = base(name, extra=extra, caller=self)
        elif 'caller' in base.__init__.__code__.co_varnames:
            index = base(name, caller=self)
        else:
            index = base(name)

        self._catalog.addIndex(name, index)

    @security.protected(manage_zcatalog_indexes)
    def availableIndexes(self):
        """Return a sorted list of indexes.

        Only indexes get returned for which the user has adequate
        permission to add them.
        """
        return sorted(
            self.Indexes.filtered_meta_types(),
            key=lambda meta_types: meta_types['name'])

    @security.protected(manage_zcatalog_indexes)
    def delIndex(self, name):
        self._catalog.delIndex(name)

    @security.protected(manage_zcatalog_indexes)
    def clearIndex(self, name):
        self._catalog.getIndex(name).clear()

    @security.protected(manage_zcatalog_indexes)
    def addColumn(self, name, default_value=None):
        return self._catalog.addColumn(name, default_value,
                                       threshold=self.threshold)

    @security.protected(manage_zcatalog_indexes)
    def delColumn(self, name):
        return self._catalog.delColumn(name, threshold=self.threshold)

    # Catalog plan methods

    @security.protected(manage_zcatalog_entries)
    def getCatalogPlan(self):
        """Get a string representation of a query plan"""
        pmap = PriorityMap.get_value()
        output = []
        output.append('# query plan dumped at %r\n' % time.asctime())
        output.append('queryplan = {')
        for cid, plan in sorted(pmap.items()):
            output.append('  %s: {' % repr(cid))
            for querykey, details in plan.items():
                if isinstance(details, (frozenset, set)):
                    output.append(f'    {querykey!r}: {details!r},')
                else:
                    output.append('    %s: {' % repr(querykey))
                    for indexname, bench in sorted(details.items()):
                        tuplebench = (round(bench[0], 4), ) + bench[1:]
                        output.append('      {!r}:\n      {!r},'.format(
                            indexname, tuplebench))
                    output.append('    },')
            output.append('  },')
        output.append('}')
        return '\n'.join(output)

    @security.protected(manage_zcatalog_entries)
    def getCatalogReport(self):
        """Query time reporting."""
        rval = self._catalog.getCatalogPlan().report()
        rval.sort(key=operator.itemgetter('duration'), reverse=True)
        return rval

    @security.protected(manage_zcatalog_entries)
    def manage_resetCatalogReport(self, REQUEST=None):
        """Resets the catalog report."""
        self._catalog.getCatalogPlan().reset()

        if REQUEST is not None:
            REQUEST.response.redirect(REQUEST.URL1 + (
                '/manage_catalogReport?manage_tabs_message=Report%20cleared'))

    @security.protected(manage_zcatalog_entries)
    def manage_editCatalogReport(self, long_query_time=0.1, REQUEST=None):
        """Edit the long query time."""
        if not isinstance(long_query_time, float):
            long_query_time = float(long_query_time)
        self.long_query_time = long_query_time

        if REQUEST is not None:
            REQUEST.response.redirect(REQUEST.URL1 + (
                '/manage_catalogReport?manage_tabs_message='
                'Long%20query%20time%20changed'))


InitializeClass(ZCatalog)


def absattr(attr):
    if callable(attr):
        return attr()
    return attr


class td(RestrictedDTML, TemplateDict):  # NOQA
    pass


def expr_match(ob, ed):
    e, md, push, pop = ed
    push(InstanceDict(ob, md))
    r = 0
    try:
        r = e.eval(md)
    finally:
        pop()
        return r


def mtime_match(ob, t, q):
    mtime = getattr(ob, '_p_mtime', _marker)
    if mtime is _marker():
        return False
    return q == '<' and (mtime < t) or (mtime > t)


def role_match(ob, permission, roles):
    pr = []
    while True:
        p = getattr(ob, permission, _marker)
        if p is not _marker:
            if isinstance(p, list):
                pr.append(p)
                ob = aq_parent(ob)
                if ob is not None:
                    continue
                break
            if isinstance(p, tuple):
                pr.append(p)
                break
            if p is None:
                pr.append(('Manager', 'Anonymous'))
                break

        ob = aq_parent(ob)
        if ob is not None:
            continue
        break

    for role in roles:
        if role not in pr:
            return False
    return True
