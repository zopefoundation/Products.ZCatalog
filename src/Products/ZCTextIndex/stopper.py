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


def process(stopdict, lst):
    """
    Remove stop words (the keys of dict) from the input list of strings
    to create a new list.

    BBB ZCTextIndex 4.0: Compatibility for former C implementation.
    """
    if not isinstance(stopdict, dict):
        raise TypeError('process requires a dict as argument 1')

    return [w for w in lst if w not in stopdict]
