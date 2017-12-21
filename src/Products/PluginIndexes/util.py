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

from datetime import datetime

from DateTime.DateTime import DateTime
import six

MAX32 = int(2 ** 31 - 1)


def safe_callable(ob):
    # Works with ExtensionClasses and Acquisition.
    if hasattr(ob, '__class__'):
        return hasattr(ob, '__call__') or isinstance(ob, six.class_types)
    else:
        return callable(ob)


def datetime_to_minutes(value, precision=1,
                        max_value=MAX32, min_value=-MAX32):
    if value is None:
        return value

    if isinstance(value, (str, datetime)):
        value = DateTime(value)

    if isinstance(value, DateTime):
        value = value.millis() / 1000 / 60  # flatten to minutes

    # flatten to precision
    if precision > 1:
        value = value - (value % precision)

    value = int(value)

    if value > max_value or value < min_value:
        # value must be integer fitting in the range (default 32bit)
        raise OverflowError(
            '%s is not within the range of dates allowed.' % value)

    return value
