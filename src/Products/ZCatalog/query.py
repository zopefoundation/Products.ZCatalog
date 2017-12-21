##############################################################################
#
# Copyright (c) 2010 Zope Foundation and Contributors.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE
#
##############################################################################

_marker = object()


class IndexQueryParseError(Exception):
    pass


class IndexQuery(object):
    """
    This class provides functionality to hide the internals of a query
    send from the Catalog/ZCatalog to an index._apply_index() method.

    The class understands the following type of parameters:

    - old-style parameters where the query for an index as value inside
      the request dictionary where the index name is the name of the key.

    - dictionary-style parameters specify a query for an index as
      an entry in the request dictionary where the key corresponds to the
      name of the index and the key is a dictionary with the parameters
      passed to the index.

      Allowed keys of the parameter dictionary:

      'query'  - contains the query (either string, list or tuple) (required)

      other parameters depend on the the index.
    """
    operators = ('or', 'and')

    def __init__(self, request, iid, options=(), operators=('or', 'and'),
                 default_operator='or'):
        """Parse a query from the ZPublisher and return a uniform
        datastructure back to the _apply_index() method of the index.

          query -- the query dictionary send from the ZPublisher
          iid     -- Id of index
          options -- a list of options the index is interested in
          operators -- a tuple of allowed operators
          default_operator -- the default operator
        """

        self.id = iid
        self.operators = operators
        self.operator = default_operator
        if iid not in request:
            self.keys = None
            return

        param = request[iid]
        keys = None

        if isinstance(param, dict):
            # query is a dictionary containing all parameters
            query = param.get('query', ())
            if isinstance(query, (tuple, list)):
                keys = query
            else:
                keys = [query]

            for op in options:
                if op == 'query':
                    continue

                if op in param:
                    self.set(op, param[op])

        else:
            # query is tuple, list, string, number, or something else
            if isinstance(param, (tuple, list)):
                keys = param
            else:
                keys = [param]

            for op in options:
                field = iid + "_" + op
                if field in request:
                    self.set(op, request[field])

        self.keys = keys
        not_value = getattr(self, 'not', None)
        if not_value is not None:
            if not isinstance(not_value, (tuple, list)):
                not_value = [not_value]
                self.set('not', not_value)

    @property
    def operator(self):
        return self._operator

    @operator.setter
    def operator(self, value):
        value = value.lower()
        if value not in self.operators:
            raise RuntimeError('operator not valid: %r' % value)
        self._operator = value.lower()

    def get(self, key, default_v=None):
        value = getattr(self, key, _marker)
        if value is not _marker:
            return value
        return default_v

    def set(self, key, value):
        setattr(self, key, value)
