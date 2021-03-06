##############################################################################
#
# Copyright (c) 2002 Zope Foundation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE
#
##############################################################################
"""ZCatalog Text Index

Plugin text index for ZCatalog.
"""

from AccessControl import ModuleSecurityInfo

from Products.ZCTextIndex import HTMLSplitter  # NOQA
from Products.ZCTextIndex import ZCTextIndex
from Products.ZCTextIndex.PipelineFactory import element_factory


def initialize(context):

    context.registerClass(
        ZCTextIndex.ZCTextIndex,
        permission='Add Pluggable Index',
        constructors=(ZCTextIndex.manage_addZCTextIndexForm,
                      ZCTextIndex.manage_addZCTextIndex,
                      getIndexTypes),
        icon='www/index.gif',
        visibility=None
    )

    context.registerClass(
        ZCTextIndex.PLexicon,
        permission='Add Vocabularies',
        constructors=(ZCTextIndex.manage_addLexiconForm,
                      ZCTextIndex.manage_addLexicon,
                      getElementGroups, getElementNames),
        icon='www/lexicon.gif'
    )

# Functions below are for use in the ZMI constructor forms


def getElementGroups(self):
    return element_factory.getFactoryGroups()


def getElementNames(self, group):
    return element_factory.getFactoryNames(group)


def getIndexTypes(self):
    return ZCTextIndex.index_types.keys()


# Allow relevant exceptions to be caught in untrusted code
ModuleSecurityInfo('Products').declarePublic('ZCTextIndex')
ModuleSecurityInfo('Products.ZCTextIndex').declarePublic('ParseTree')
ModuleSecurityInfo(
    'Products.ZCTextIndex.ParseTree').declarePublic('QueryError')
ModuleSecurityInfo(
    'Products.ZCTextIndex.ParseTree').declarePublic('ParseError')
