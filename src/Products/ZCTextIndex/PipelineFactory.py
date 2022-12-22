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
from zope.interface import implementer

from Products.ZCTextIndex.interfaces import IPipelineElementFactory


@implementer(IPipelineElementFactory)
class PipelineElementFactory:

    def __init__(self):
        self._groups = {}

    def registerFactory(self, group, name, factory):
        if group in self._groups and name in self._groups[group]:
            raise ValueError('ZCTextIndex lexicon element "%s" '
                             'already registered in group "%s"'
                             % (name, group))

        elements = self._groups.get(group)
        if elements is None:
            elements = self._groups[group] = {}
        elements[name] = factory

    def getFactoryGroups(self):
        groups = sorted(self._groups.keys())
        return groups

    def getFactoryNames(self, group):
        names = sorted(self._groups[group].keys())
        return names

    def instantiate(self, group, name):
        factory = self._groups[group][name]
        if factory is not None:
            return factory()


element_factory = PipelineElementFactory()
