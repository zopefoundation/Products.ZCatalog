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
"""Virtual container for ZCatalog indexes.
"""

from AccessControl.class_init import InitializeClass
from AccessControl.Permissions import manage_zcatalog_indexes
from AccessControl.Permissions import view_management_screens
from AccessControl.SecurityInfo import ClassSecurityInfo
from Acquisition import Implicit
from Acquisition import aq_base
from Acquisition import aq_inner
from Acquisition import aq_parent
from App.special_dtml import DTMLFile
from OFS.Folder import Folder
from OFS.ObjectManager import IFAwareObjectManager
from OFS.SimpleItem import SimpleItem
from Persistence import Persistent

from Products.PluginIndexes.interfaces import IPluggableIndex


_marker = []


class ZCatalogIndexes(IFAwareObjectManager, Folder, Persistent, Implicit):
    """A mapping object, responding to getattr requests by looking up
    the requested indexes in an object manager."""

    # The interfaces we want to show up in our object manager
    _product_interfaces = (IPluggableIndex, )

    meta_type = 'ZCatalogIndex'
    zmi_icon = 'far fa-list-alt'
    manage_options = ()

    security = ClassSecurityInfo()

    security.declareObjectProtected(manage_zcatalog_indexes)
    security.setPermissionDefault(manage_zcatalog_indexes, ('Manager', ))
    security.declareProtected(manage_zcatalog_indexes, 'addIndexForm')
    addIndexForm = DTMLFile('dtml/addIndexForm', globals())

    # You no longer manage the Indexes here, they are managed from ZCatalog
    def manage_main(self, REQUEST, RESPONSE):
        """Redirect to the parent where the management screen now lives"""
        RESPONSE.redirect('../manage_catalogIndexes')

    manage_workspace = manage_main

    # base accessors loop back through our dictionary interface
    def _setOb(self, id, object):
        indexes = aq_parent(self)._catalog.indexes
        indexes[id] = object
        aq_base(aq_parent(self))._indexes = indexes

    def _delOb(self, id):
        indexes = aq_parent(self)._catalog.indexes
        del indexes[id]
        aq_base(aq_parent(self))._indexes = indexes

    def _getOb(self, id, default=_marker):
        indexes = aq_parent(aq_inner(self))._catalog.indexes
        if default is _marker:
            return indexes.get(id)
        return indexes.get(id, default)

    @security.protected(view_management_screens)
    def manage_get_sortedObjects(self, sortkey, revkey):
        """We need to wrap the index objects because some of them
        can have security which does not work if they are unwrapped.
        This happened to ZCTextIndex objects in Plone."""
        items = super().manage_get_sortedObjects(
            sortkey, revkey)
        for item in items:
            item['obj'] = item['obj'].__of__(self)
        return items

    @security.protected(manage_zcatalog_indexes)
    def objectIds(self, spec=None):
        indexes = aq_parent(aq_inner(self))._catalog.indexes
        if spec is not None:
            if isinstance(spec, str):
                spec = [spec]
            result = []

            for ob in indexes.keys():
                o = indexes.get(ob)
                meta = getattr(o, 'meta_type', None)
                if meta is not None and meta in spec:
                    result.append(ob)
            return result

        return indexes.keys()

    def __contains__(self, name):
        return name in aq_parent(self)._catalog.indexes

    def _setObject(self, id, object, roles=None, user=None, set_owner=1):
        # Eat _setObject calls
        pass

    def __bobo_traverse__(self, REQUEST, name):
        indexes = aq_parent(self)._catalog.indexes
        o = indexes.get(name, None)
        if o is not None:
            if getattr(o, 'manage_workspace', None) is None:
                o = OldCatalogWrapperObject(o)
            return o.__of__(self)

        return getattr(self, name)


InitializeClass(ZCatalogIndexes)


class OldCatalogWrapperObject(SimpleItem, Implicit):

    manage_options = (
        {'label': 'Settings', 'action': 'manage_main'},
    )

    manage_main = DTMLFile('dtml/manageOldindex', globals())
    manage_main._setName('manage_main')
    manage_workspace = manage_main

    def __init__(self, o):
        self.index = o
