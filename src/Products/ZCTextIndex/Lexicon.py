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
"""Lexicon.
"""

from random import randrange
import re
import six

from BTrees.IOBTree import IOBTree
from BTrees.OIBTree import OIBTree
from BTrees.Length import Length
from Persistence import Persistent
from zope.interface import implementer

from Products.ZCTextIndex.interfaces import ILexicon
from Products.ZCTextIndex.StopDict import get_stopdict
from Products.ZCTextIndex.ParseTree import QueryError
from Products.ZCTextIndex.PipelineFactory import element_factory


@implementer(ILexicon)
class Lexicon(Persistent):

    _v_nextid = None
    _wid_length_based = True  # Flag to distinguish new and old lexica

    def __init__(self, *pipeline):
        self.clear()
        self._pipeline = pipeline

    def clear(self):
        """Empty the lexicon.
        """
        self.length = Length()
        self._wid_length_based = False
        self._wids = OIBTree()  # word -> wid
        self._words = IOBTree()  # wid -> word
        # wid 0 is reserved for words that aren't in the lexicon (OOV -- out
        # of vocabulary).  This can happen, e.g., if a query contains a word
        # we never saw before, and that isn't a known stopword (or otherwise
        # filtered out).  Returning a special wid value for OOV words is a
        # way to let clients know when an OOV word appears.

    def length(self):
        """Return the number of unique terms in the lexicon.
        """
        # Overridden in instances with a BTrees.Length.Length
        raise NotImplementedError

    def words(self):
        return self._wids.keys()

    def wids(self):
        return self._words.keys()

    def items(self):
        return self._wids.items()

    def sourceToWordIds(self, text):
        last = _text2list(text)
        for element in self._pipeline:
            last = element.process(last)
        return list(map(self._getWordIdCreate, last))

    def termToWordIds(self, text):
        last = _text2list(text)
        for element in self._pipeline:
            process = getattr(element, "process_post_glob", element.process)
            last = process(last)
        wids = []
        for word in last:
            wids.append(self._wids.get(word, 0))
        return wids

    def parseTerms(self, text):
        last = _text2list(text)
        for element in self._pipeline:
            process = getattr(element, "processGlob", element.process)
            last = process(last)
        return last

    def isGlob(self, word):
        return "*" in word or "?" in word

    def get_word(self, wid):
        return self._words[wid]

    def get_wid(self, word):
        return self._wids.get(word, 0)

    def globToWordIds(self, pattern):
        # Implement * and ? just as in the shell, except the pattern
        # must not start with either of these
        prefix = ""
        while pattern and pattern[0] not in "*?":
            prefix += pattern[0]
            pattern = pattern[1:]
        if not pattern:
            # There were no globbing characters in the pattern
            wid = self._wids.get(prefix, 0)
            if wid:
                return [wid]
            else:
                return []
        if not prefix:
            # The pattern starts with a globbing character.
            # This is too efficient, so we raise an exception.
            raise QueryError(
                "pattern %r shouldn't start with glob character" % pattern)
        pat = prefix
        for c in pattern:
            if c == "*":
                pat += ".*"
            elif c == "?":
                pat += "."
            else:
                pat += re.escape(c)
        pat += "$"
        prog = re.compile(pat)
        keys = self._wids.keys(prefix)  # Keys starting at prefix
        wids = []
        for key in keys:
            if not key.startswith(prefix):
                break
            if prog.match(key):
                wids.append(self._wids[key])
        return wids

    def _getWordIdCreate(self, word):
        wid = self._wids.get(word)
        if wid is None:
            # WidCode requires us to use at least 0x4000 as a base number.
            # The algorithm in versions before 2.13 used the length as a base
            # number. So we don't even try to generate numbers below the
            # length as they are likely all taken
            minimum = 0x4000
            if self._wid_length_based:
                minimum = max(self.length(), 0x4000)

            while True:
                if self._v_nextid is None:
                    self._v_nextid = randrange(minimum, 0x10000000)

                wid = self._v_nextid
                self._v_nextid += 1

                if wid not in self._words:
                    break

                self._v_nextid = None

            self.length.change(1)
            self._wids[word] = wid
            self._words[wid] = word
        return wid


def _text2list(text):
    # Helper: splitter input may be a string or a list of strings
    try:
        text + ""
    except Exception:
        return text
    else:
        return [text]

# Sample pipeline elements


class Splitter(object):

    import re
    if six.PY2:
        rx = re.compile(br"(?L)\w+")
        rxGlob = re.compile(br"(?L)\w+[\w*?]*")
    else:
        rx = re.compile(r"\w+")
        rxGlob = re.compile(r"\w+[\w*?]*")  # See globToWordIds() above

    def process(self, lst):
        result = []
        for s in lst:
            result += self.rx.findall(s)
        return result

    def processGlob(self, lst):
        result = []
        for s in lst:
            result += self.rxGlob.findall(s)
        return result


element_factory.registerFactory('Word Splitter',
                                'Whitespace splitter',
                                Splitter)


class CaseNormalizer(object):

    def process(self, lst):
        return [w.lower() for w in lst]


element_factory.registerFactory('Case Normalizer',
                                'Case Normalizer',
                                CaseNormalizer)

element_factory.registerFactory('Stop Words',
                                ' Don\'t remove stop words',
                                None)


class StopWordRemover(object):

    dict = get_stopdict().copy()

    def process(self, lst):
        return [w for w in lst if w not in self.dict]


element_factory.registerFactory('Stop Words',
                                'Remove listed stop words only',
                                StopWordRemover)


class StopWordAndSingleCharRemover(StopWordRemover):

    dict = get_stopdict().copy()
    for c in range(255):
        dict[chr(c)] = None


element_factory.registerFactory('Stop Words',
                                'Remove listed and single char words',
                                StopWordAndSingleCharRemover)
