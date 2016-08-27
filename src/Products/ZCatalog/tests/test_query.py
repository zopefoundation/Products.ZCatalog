##############################################################################
#
# Copyright (c) 2007 Zope Foundation and Contributors.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE.
#
##############################################################################

import unittest


class TestIndexQuery(unittest.TestCase):

    def _getTargetClass(self):
        from Products.ZCatalog.query import IndexQuery
        return IndexQuery

    def _makeOne(self, *args, **kw):
        return self._getTargetClass()(*args, **kw)

    def test_get_dict(self):
        request = {'path': {'query': 'foo', 'level': 0, 'operator': 'and'}}
        parser = self._makeOne(request, 'path', ('query', 'level', 'operator'))
        self.assertEqual(parser.get('keys'), ['foo'])
        self.assertEqual(parser.get('level'), 0)
        self.assertEqual(parser.get('operator'), 'and')

    def test_get_string(self):
        request = {'path': 'foo', 'path_level': 0, 'path_operator': 'and'}
        parser = self._makeOne(request, 'path', ('query', 'level', 'operator'))
        self.assertEqual(parser.get('keys'), ['foo'])
        self.assertEqual(parser.get('level'), 0)
        self.assertEqual(parser.get('operator'), 'and')

    def test_get_not_dict(self):
        request = {'path': {'query': 'foo', 'not': 'bar'}}
        parser = self._makeOne(request, 'path', ('query', 'not'))
        self.assertEqual(parser.get('keys'), ['foo'])
        self.assertEqual(parser.get('not'), ['bar'])

    def test_get_not_dict_list(self):
        request = {'path': {'query': 'foo', 'not': ['bar', 'baz']}}
        parser = self._makeOne(request, 'path', ('query', 'not'))
        self.assertEqual(parser.get('keys'), ['foo'])
        self.assertEqual(parser.get('not'), ['bar', 'baz'])

    def test_get_not_string(self):
        request = {'path': 'foo', 'path_not': 'bar'}
        parser = self._makeOne(request, 'path', ('query', 'not'))
        self.assertEqual(parser.get('keys'), ['foo'])
        self.assertEqual(parser.get('not'), ['bar'])

    def test_get_not_string_list(self):
        request = {'path': 'foo', 'path_not': ['bar', 'baz']}
        parser = self._makeOne(request, 'path', ('query', 'not'))
        self.assertEqual(parser.get('keys'), ['foo'])
        self.assertEqual(parser.get('not'), ['bar', 'baz'])

    def test_get_not_int(self):
        request = {'path': 'foo', 'path_not': 0}
        parser = self._makeOne(request, 'path', ('query', 'not'))
        self.assertEqual(parser.get('keys'), ['foo'])
        self.assertEqual(parser.get('not'), [0])
