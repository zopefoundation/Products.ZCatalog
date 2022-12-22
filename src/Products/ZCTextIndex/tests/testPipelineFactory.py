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

from unittest import TestCase

from zope.interface import implementer

from Products.ZCTextIndex.interfaces import IPipelineElement
from Products.ZCTextIndex.PipelineFactory import PipelineElementFactory


@implementer(IPipelineElement)
class NullPipelineElement:
    pass


class PipelineFactoryTest(TestCase):

    def setUp(self):
        self.huey = NullPipelineElement()
        self.dooey = NullPipelineElement()
        self.louie = NullPipelineElement()
        self.daffy = NullPipelineElement()

    def testPipeline(self):
        pf = PipelineElementFactory()
        pf.registerFactory('donald', 'huey', self.huey)
        pf.registerFactory('donald', 'dooey', self.dooey)
        pf.registerFactory('donald', 'louie', self.louie)
        pf.registerFactory('looney', 'daffy', self.daffy)
        self.assertRaises(ValueError, pf.registerFactory, 'donald', 'huey',
                          self.huey)
        self.assertEqual(pf.getFactoryGroups(), ['donald', 'looney'])
        self.assertEqual(pf.getFactoryNames('donald'),
                         ['dooey', 'huey', 'louie'])
