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

from zope.deferredimport import deprecated  # pragma: nocover

# BBB ZCatalog 5.0
deprecated(
    'Please import from Products.ZCatalog.query.',
    IndexRequestParseError='Products.ZCatalog.query:IndexQueryParseError',
    parseIndexRequest='Products.ZCatalog.query:IndexQuery',
)  # pragma: nocover

# BBB ZCatalog 5.0
deprecated(
    'Please import from Products.PluginIndexes.cache.',
    RequestCache='Products.PluginIndexes.cache:RequestCache',
)  # pragma: nocover
