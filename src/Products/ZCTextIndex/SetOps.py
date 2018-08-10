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

"""SetOps -- Weighted intersections and unions applied to many inputs."""

from BTrees.IIBTree import IIBucket
from BTrees.IIBTree import weightedIntersection
from BTrees.IIBTree import weightedUnion

from Products.ZCTextIndex.NBest import NBest


def mass_weightedIntersection(l_):
    "A list of (mapping, weight) pairs -> their weightedIntersection IIBucket."
    l_ = [(x, wx) for (x, wx) in l_ if x is not None]
    if len(l_) < 2:
        return _trivial(l_)
    # Intersect with smallest first. We expect the input maps to be
    # IIBuckets, so it doesn't hurt to get their lengths repeatedly
    # (len(Bucket) is fast; len(BTree) is slow).

    def _key(value):
        return len(value)

    l_.sort(key=_key)
    (x, wx), (y, wy) = l_[:2]
    dummy, result = weightedIntersection(x, y, wx, wy)
    for x, wx in l_[2:]:
        dummy, result = weightedIntersection(result, x, 1, wx)
    return result


def mass_weightedUnion(l_):
    "A list of (mapping, weight) pairs -> their weightedUnion IIBucket."
    if len(l_) < 2:
        return _trivial(l_)
    # Balance unions as closely as possible, smallest to largest.
    merge = NBest(len(l_))
    for x, weight in l_:
        merge.add((x, weight), len(x))
    while len(merge) > 1:
        # Merge the two smallest so far, and add back to the queue.
        (x, wx), dummy = merge.pop_smallest()
        (y, wy), dummy = merge.pop_smallest()
        dummy, z = weightedUnion(x, y, wx, wy)
        merge.add((z, 1), len(z))
    (result, weight), dummy = merge.pop_smallest()
    return result


def _trivial(l_):
    # l is empty or has only one (mapping, weight) pair. If there is a
    # pair, we may still need to multiply the mapping by its weight.
    assert len(l_) <= 1
    if len(l_) == 0:
        return IIBucket()
    [(result, weight)] = l_
    if weight != 1:
        dummy, result = weightedUnion(IIBucket(), result, 0, weight)
    return result
