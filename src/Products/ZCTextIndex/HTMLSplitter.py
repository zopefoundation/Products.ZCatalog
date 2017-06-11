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
# FOR A PARTICULAR PURPOSE.
#
##############################################################################
import re
import six

from zope.interface import implementer

from Products.ZCTextIndex.interfaces import ISplitter
from Products.ZCTextIndex.PipelineFactory import element_factory

if six.PY2:
    word_pattern = r"(?L)\w+"
    glob_pattern = r"(?L)\w+[\w*?]*"
else:
    # in Python 3, the locale flag can only be applied to bytes patterns
    word_pattern = r"\w+"
    glob_pattern = r"\w+[\w*?]*"


@implementer(ISplitter)
class HTMLWordSplitter(object):

    def process(self, text, wordpat=word_pattern):
        splat = []
        for t in text:
            splat += self._split(t, wordpat)
        return splat

    def processGlob(self, text):
        # see Lexicon.globToWordIds()
        return self.process(text, glob_pattern)

    def _split(self, text, wordpat):
        text = text.lower()
        remove = [r"<[^<>]*>",
                  r"&[A-Za-z]+;"]
        for pat in remove:
            text = re.sub(pat, " ", text)
        return re.findall(wordpat, text)


element_factory.registerFactory('Word Splitter',
                                'HTML aware splitter',
                                HTMLWordSplitter)
