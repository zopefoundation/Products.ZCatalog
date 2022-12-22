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

import time
from datetime import date
from datetime import datetime
from datetime import timedelta
from datetime import tzinfo
from logging import getLogger

from App.special_dtml import DTMLFile
from BTrees.IIBTree import IIBTree
from BTrees.IOBTree import IOBTree
from BTrees.Length import Length
from DateTime.DateTime import DateTime
from OFS.PropertyManager import PropertyManager
from zope.interface import implementer

from Products.PluginIndexes.interfaces import IDateIndex
from Products.PluginIndexes.unindex import UnIndex


LOG = getLogger('DateIndex')
_marker = []

###############################################################################
# copied from Python 2.3 datetime.tzinfo docs
# A class capturing the platform's idea of local time.

ZERO = timedelta(0)
STDOFFSET = timedelta(seconds=-time.timezone)
if time.daylight:
    DSTOFFSET = timedelta(seconds=-time.altzone)
else:
    DSTOFFSET = STDOFFSET

DSTDIFF = DSTOFFSET - STDOFFSET
MAX32 = int(2 ** 31 - 1)


class LocalTimezone(tzinfo):

    def utcoffset(self, dt):
        if self._isdst(dt):
            return DSTOFFSET
        else:
            return STDOFFSET

    def dst(self, dt):
        if self._isdst(dt):
            return DSTDIFF
        else:
            return ZERO

    def tzname(self, dt):
        return time.tzname[self._isdst(dt)]

    def _isdst(self, dt):
        tt = (dt.year, dt.month, dt.day,
              dt.hour, dt.minute, dt.second,
              dt.weekday(), 0, -1)
        stamp = time.mktime(tt)
        tt = time.localtime(stamp)
        return tt.tm_isdst > 0


Local = LocalTimezone()
###############################################################################


@implementer(IDateIndex)
class DateIndex(UnIndex, PropertyManager):
    """Index for dates.
    """

    meta_type = 'DateIndex'
    query_options = ('query', 'range', 'not')

    index_naive_time_as_local = True  # False means index as UTC
    precision = 1  # precision of indexed time in minutes
    _properties = ({'id': 'index_naive_time_as_local',
                    'type': 'boolean',
                    'mode': 'w'},
                   {'id': 'precision',
                    'type': 'int',
                    'mode': 'w'},)

    manage = manage_main = DTMLFile('dtml/manageDateIndex', globals())
    manage_browse = DTMLFile('../dtml/browseIndex', globals())

    manage_main._setName('manage_main')
    manage_options = ({'label': 'Settings', 'action': 'manage_main'},
                      {'label': 'Browse', 'action': 'manage_browse'},
                      ) + PropertyManager.manage_options

    def clear(self):
        """ Complete reset """
        self._index = IOBTree()
        self._unindex = IIBTree()
        self._length = Length()
        if self._counter is None:
            self._counter = Length()
        else:
            self._increment_counter()

    def _convert(self, value, default=None):
        """Convert Date/Time value to our internal representation"""
        if isinstance(value, DateTime):
            t_tup = value.toZone('UTC').parts()
        elif isinstance(value, (float, int)):
            t_tup = time.gmtime(value)
        elif isinstance(value, str) and value:
            t_obj = DateTime(value).toZone('UTC')
            t_tup = t_obj.parts()
        elif isinstance(value, datetime):
            if self.index_naive_time_as_local and value.tzinfo is None:
                value = value.replace(tzinfo=Local)
            # else if tzinfo is None, naive time interpreted as UTC
            t_tup = value.utctimetuple()
        elif isinstance(value, date):
            t_tup = value.timetuple()
        else:
            return default

        yr = t_tup[0]
        mo = t_tup[1]
        dy = t_tup[2]
        hr = t_tup[3]
        mn = t_tup[4]

        t_val = ((((yr * 12 + mo) * 31 + dy) * 24 + hr) * 60 + mn)

        # flatten to precision
        if self.precision > 1:
            t_val = t_val - (int(t_val) % self.precision)

        t_val = int(t_val)

        if t_val > MAX32:
            # t_val must be integer fitting in the 32bit range
            raise OverflowError(
                '{} is not within the range of'
                ' indexable dates (index: {})'.format(
                    value, self.id))
        return t_val


manage_addDateIndexForm = DTMLFile('dtml/addDateIndex', globals())


def manage_addDateIndex(self, id, REQUEST=None, RESPONSE=None, URL3=None):
    """Add a Date index"""
    return self.manage_addIndex(id, 'DateIndex', extra=None,
                                REQUEST=REQUEST, RESPONSE=RESPONSE, URL1=URL3)
