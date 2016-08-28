##############################################################################
#
# Copyright (c) 2016 Zope Foundation and Contributors.
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

B = 0.75
B_FROM1 = 0.25  # 1.0 - B

K1 = 1.2
K1_PLUS1 = 2.2  # K1 + 1.0


def score(result, d2fitems, d2len, idf, meandoclen):
    """
    Do the inner scoring loop for an Okapi index.

    result: IIBucket result, maps d to score
    d2fitems: _wordinfo[t].items(), maps d to f(d, t)
    d2len: _docweight, maps d to # words in d
    idf: inverse doc frequency of t
    meandoclen: average number of words in a doc

    BBB ZCTextIndex 4.0: Compatibility for former C implementation.
    """
    idf *= 1024.0  # float out part of the scaled_int computation

    for docid, f in d2fitems:
        f = int(f)
        lenweight = B_FROM1 + B * int(d2len[docid]) / meandoclen
        tf = f * K1_PLUS1 / (f + K1 * lenweight)
        result[docid] = int(tf * idf + 0.5)
