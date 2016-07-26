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

from types import InstanceType

from DateTime.DateTime import DateTime

_marker = []


class IndexRequestParseError(Exception):
    pass


class parseIndexRequest:
    """
    This class provides functionality to hide the internals of a request
    send from the Catalog/ZCatalog to an index._apply_index() method.

    The class understands the following type of parameters:

    - old-style parameters where the query for an index as value inside
      the request directory where the index name is the name of the key.

    - dictionary-style parameters specify a query for an index as
      an entry in the request dictionary where the key corresponds to the
      name of the index and the key is a dictionary with the parameters
      passed to the index.

      Allowed keys of the parameter dictionary:

      'query'  - contains the query (either string, list or tuple) (required)

      other parameters depend on the the index

   - record-style parameters specify a query for an index as instance
     of the Record class. This happens usually when parameters from a
     web form use the "record" type e.g.
           <input type="text" name="path.query:record:string">.
     All restrictions of the dictionary-style parameters apply to the
     record-style parameters
    """

    ParserException = IndexRequestParseError

    def __init__(self, request, iid, options=[]):
        """ parse a request  from the ZPublisher and return a uniform
        datastructure back to the _apply_index() method of the index

          request -- the request dictionary send from the ZPublisher
          iid     -- Id of index
          options -- a list of options the index is interested in
        """

        self.id = iid
        if iid not in request:
            self.keys = None
            return

        param = request[iid]
        keys = None

        if isinstance(param, InstanceType) and not isinstance(param, DateTime):
            """ query is of type record """

            record = param

            if not hasattr(record, 'query'):
                raise self.ParserException(
                    "record for '%s' *must* contain a "
                    "'query' attribute" % self.id)
            keys = record.query

            if isinstance(keys, str):
                keys = [keys.strip()]

            for op in options:
                if op == "query":
                    continue

                if hasattr(record, op):
                    setattr(self, op, getattr(record, op))

        elif isinstance(param, dict):
            """ query is a dictionary containing all parameters """

            query = param.get("query", ())
            if isinstance(query, (tuple, list)):
                keys = query
            else:
                keys = [query]

            for op in options:
                if op == "query":
                    continue

                if op in param:
                    setattr(self, op, param[op])

        else:
            """ query is tuple, list, string, number, or something else """

            if isinstance(param, (tuple, list)):
                keys = param
            else:
                keys = [param]

            for op in options:
                field = iid + "_" + op
                if field in request:
                    setattr(self, op, request[field])

        self.keys = keys
        not_value = getattr(self, 'not', None)
        if not_value is not None:
            if not isinstance(not_value, (tuple, list)):
                not_value = [not_value]
                setattr(self, 'not', not_value)

    def get(self, k, default_v=None):
        if hasattr(self, k):
            v = getattr(self, k)
            if v != '':
                return v
        return default_v


class RequestCache(dict):

    # stats info needed for testing
    _hits = 0
    _misses = 0
    _sets = 0

    def get(self, key, default=None):
        value = super(RequestCache, self).get(key, _marker)

        if value is _marker:
            self._misses += 1
            return default

        self._hits += 1
        return value

    def __getitem__(self, key):
        try:
            value = super(RequestCache, self).__getitem__(key)
        except KeyError as e:
            self._misses += 1
            raise e

        self._hits += 1
        return value

    def __setitem__(self, key, value):
        super(RequestCache, self).__setitem__(key, value)
        self._sets += 1

    def clear(self):
        super(RequestCache, self).clear()
        self._hits = 0
        self._misses = 0
        self._sets = 0

    def stats(self):
        stats = {'hits': self._hits,
                 'misses': self._misses,
                 'sets': self._sets}
        return stats

    def __str__(self):
        return "<RequestCache %s items (hits: %s, misses: %s, sets: %s)>" % \
            (len(self), self._hits, self._misses, self._sets)
