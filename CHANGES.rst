Changelog
=========

5.0.4 (2020-02-11)
------------------

- Fix some ``DocumentTemplate`` imports to prevent DeprecationWarnings


5.0.3 (2020-01-17)
------------------

- Fix exception when viewing `Indexes` tab when Chameleon template
  engine is deactivated.
  (`#84 <https://github.com/zopefoundation/Products.ZCatalog/issues/84>`_)


5.0.2 (2019-12-06)
------------------

- Fix exception happening when reindexing multiple indexes while having a
  progress handler.
  (`#89 <https://github.com/zopefoundation/Products.ZCatalog/pull/89>`_)


5.0.1 (2019-06-17)
------------------

- Fix sorting of index overview table in ZMI. Migrated the template from 
  to zpt.
  (`#62 <https://github.com/zopefoundation/Products.ZCatalog/issues/62>`_)


5.0 (2019-05-10)
----------------

Backwards incompatible changes
++++++++++++++++++++++++++++++

- Raise a ``ValueError`` if a query uses invalid index parameters. This
  prevents the query from being changed without feedback to the user and
  delivering implausible search results.
  (`#67 <https://github.com/zopefoundation/Products.ZCatalog/pull/67>`_)

Bug fixes
+++++++++

- Fix rewriting of query to avoid wrong optimization of CompositeIndex.
  (`#59 <https://github.com/zopefoundation/Products.ZCatalog/issues/59>`_)

- Consistent use of ``UnIndex._convert`` method to avoid unnecessary
  doubling of code.
  (`#69 <https://github.com/zopefoundation/Products.ZCatalog/issues/69>`_)

- Fix performance issue of ``valueindexes`` method for catalogs with
  many indexed objects
  (`#39 <https://github.com/zopefoundation/Products.ZCatalog/issues/39>`_)

- Make sure ``PathIndex._index_object`` removes old index entries
  when path of object changes
  (`#61 <https://github.com/zopefoundation/Products.ZCatalog/issues/61>`_)

- Code cleanup and completion of TopicIndex according to existing index
  methods and properties
  (`#65 <https://github.com/zopefoundation/Products.ZCatalog/issues/65>`_)


4.4 (2019-03-08)
----------------

- Make sure WidCode decode handles bytes that were improperly
  converted from Python 2 to Python 3.

- Specify supported Python versions using ``python_requires`` in setup.py
  (`Zope#481 <https://github.com/zopefoundation/Zope/issues/481>`_)

- Added support for Python 3.8

- Flake8 the code.


4.3 (2019-02-08)
----------------

- Adapt remaining ZMI tabs to Bootstrap
  (`#45 <https://github.com/zopefoundation/Products.ZCatalog/issues/45>`_)

- Replace deprecated ``cgi.escape`` with ``html.escape`` for Python 3.

- Fix "invalid escape sequence" warning in Python 3.


4.2 (2018-10-05)
----------------

- Replace ``urllib.quote`` with ``six.moves.urllib.parse.quote``.
  Fixes an issue on a ZMI redirect after rebuilding the catalog.

- Adapt the ZMI HTML to the new Bootstrap ZMI.
  (`#41 <https://github.com/zopefoundation/Products.ZCatalog/pull/41>`_)

- Fix sorting in _sort_iterate_resultset in Python 3.
  (`#42 <https://github.com/zopefoundation/Products.ZCatalog/pull/42>`_)

- Add support for Python 3.7.

- Drop support for Python 3.4.


4.1.1 (2018-07-05)
------------------

- Fix a TypeError on Python 3 when trying to lookup in an OOBTree
  a value for a key that has an invalid type.
  (`#36 <https://github.com/zopefoundation/Products.ZCatalog/pull/36>`_)


4.1 (2018-03-06)
----------------

- Add new precision property to date and date range indexes.
  This lets you index more coarse grained time values instead of the
  default one minute based time resolution.

- Add new `getAllBrains` method to the ZCatalog, returning a generator
  of brains for all cataloged objects. You can use this if you relied
  on `searchResults` returning all brains for empty queries before
  version 4.0a2.

- Fix logging issue in KeywordIndex.

4.0.1 (2017-10-10)
------------------

- Fix a bug in the BooleanIndex where documents without an entry in
  the index were not being filtered out in all queries.

- More PEP8 compliance.

4.0.0 (2017-05-23)
------------------

- Python 3 compatibility

- Target use with Zope 4:  no longer support 2.13.x.

- `five.globalrequest` got merged into Zope2 itself.

- Use aq_inner before aq_parent at some places to safely get the parent.

4.0a3 (2017-02-02)
------------------

- #19: Fix stale cache results after clearing an index.

- Use `@implementer` class decorator.

- Add `__contains__` method to ZCatalogIndexes, fixes zopefoundation/Zope#69.

- Raise BadRequest instead of returning MessageDialog.

4.0a2 (2016-08-28)
------------------

- Move PluginIndexes.common.UnIndex module to PluginIndexes.unindex.

- Remove unused `Products.PluginIndexes.common.ResultList` and
  `randid` modules.

- Merge in the ZCTextIndex code.

- Extend IQueryIndex interface to handle operator parsing.

- Add new IQueryIndex interface for indices. This introduces a new
  `query_index` method on each index with a simplified contract compared
  to `_apply_index`. The responsibility for parsing and skipping the query
  has moved into the catalog, and the return value no longer has to be
  a tuple of (result, used_attributes), as the later wasn't used by the
  catalog.

- Rename `parseIndexRequest` to `IndexQuery` and move it to `ZCatalog.query`.

- Remove unused ZMI icons.

- Remove deprecated Catalog(Path)Awareness modules.

- Remove CatalogSearchArgumentsMap and support for using requests
  objects as queries.

- Empty catalog queries now return no results.

- No longer special-case empty strings in catalog queries.

- Add new CompositeIndex index type.

4.0a1 (2016-07-22)
------------------

- Moved `Products.ZCatalog.Lazy` module to `ZTUtils.Lazy`.

- Add configure.zcml with deprecatedManageAddDelete directives.

3.2 (2016-07-18)
----------------

- #12: Add request cache for index results to all UnIndex subclasses.

- Add dependency on `five.globalrequest`.

3.1.2 (2016-07-17)
------------------

- #6, #7, #11: Run ZODB cache garbage collection during queries.

- #13: Deal with threshold value of None in add/delColum.

3.1.1 (2016-07-17)
------------------

- Make index-listing compatible with Zope 4.

- #5: Ignore None values in UnIndex instead of raising a TypeError.

- Add a new getCounter method to indices.

- Update to ZODB 4.0 as direct dependency.

3.1 (2014-11-02)
----------------

- Raise a TypeError when trying to index or lookup `None` in an UnIndex.
  This is a required change for BTrees 4.0+ compatibility, which prevents
  objects without a clear ordering definition from being inserted in a tree.

- No longer try to insert a None value into a field index in tests.

3.0.2 (2014-03-04)
------------------

- Restore ability for indexes to use extra query params.
  See PR #1.

- Change `CatalogPlan.valueindexes` to avoid using a `len()` call on the
  result of each index `uniqueValues` method. This was loading entire BTrees
  into memory and caused excessive database load on startup.

- Correct `withLengths` argument name on `PathIndex.uniqueValues` to use
  plural form, adhering to the interface specification.

- Clarify the `IUniqueValueIndex.uniqueValues` method description and
  explicitly mention generators/iterators as potential return values.
  The PathIndex was one example returning a generator for some time.

- Adjust `actual_result_count` for sorted queries where the sort index doesn't
  contain all the documents. Fixes LP #1237141.

- Restore safeguard for using the `iterate over sort index` case and avoid
  it while using limiting at the same time. Fixes LP #1236790.

3.0.1 (2013-10-15)
------------------

- Fix BooleanIndex when index inversion occurs as a result of reindexing
  and existing document with the opposite value. Fixes LP #1236354.

3.0 (2013-02-24)
------------------

- Strip white space from name when adding a column or index.

- Forward compatibility for Zope 4 removal of RequestContainer.

- Optimize brain instantiation, by creating underlying record items in a
  single step, instead of creation and three update calls.

3.0b1 (2012-07-19)
------------------

- LP #727981: Fix DateIndex ZMI browsing for dates in the first month of a
  year.

- Unify Unindex and DateIndex search logic (`_apply_index`) adding `not`
  support to DateIndexes.

3.0a2 (2012-04-26)
------------------

- Fixed another issue with preserving score values, when a custom index was
  queried first which was neither ILimitedResultIndex aware nor return scores,
  and a later index was of the default ZCTextIndex type.

3.0a1 (2012-04-22)
------------------

- Expand query report, to cover details on sort indexes, order and limits.

- As part of each progress handler report, also do an automatic transaction
  savepoint, to give the ZODB cache a chance to do garbage collection.

- Added a `threshold` argument to the catalog's `addColumn` and `delColumn`
  methods and used it for a progress handler. Also optimized some of their
  internals.

- Added support for `sort_on` queries with any number of sort indexes and
  differing `sort_order` values. For example:
  `{'foo': 'a', 'sort_on': ('foo', 'bar')}`
  `{'foo': 'a', 'sort_on': ('foo', 'bar'), 'sort_order': ('', 'reverse')}`
  `{'foo': 'a', 'sort_on': ('foo', 'bar', 'baz')}`

- Added support for `not` queries in field and keyword indexes. Both
  restrictions of normal queries and range queries are supported, as well as
  purely exclusive queries. For example:
  `{'foo': {'query': ['a', 'ab'], 'not': 'a'}}`
  `{'foo': {'query': 'a', 'range': 'min', 'not': ['a', 'e', 'f']}}`
  `{'foo': {'not': ['a', 'b']}}`.
  Note that negative filtering on an index still restricts items to those
  having a value in the index. So with 10 documents, 5 of them in the `foo`
  index with a value of `1`, a query for `not 1` will return no items instead
  of the 5 items without a value. You need to index a dummy/default value if
  you want to consider all items for a particular index.

- Updated deprecation warnings to point to Zope 4 instead of 2.14.

2.13.22 (2011-11-17)
--------------------

- Added a new `load_from_path` class method to the `PriorityMap`, which allows
  one to load a plan from a file, instead of a module via an environment var.

2.13.21 (2011-10-20)
--------------------

- Refactored value index logic. Determine value indexes per catalog instead of
  globally. Store value index set in the priority map, so it can be seen in the
  ZMI and stored in the module level storage.

- Added support for using ZCatalog as local utility.
  This feature requires the optional `five.globalrequest` dependency.

2.13.20 (2011-08-23)
--------------------

- Fixed incorrect calculation of batches in the second half of the result set
  in sortResults.

2.13.19 (2011-08-20)
--------------------

- Increase plan precision to 4 digits in its string representation.

2.13.18 (2011-07-29)
--------------------

- In the string representation of a catalog plan, round the times to at most
  two digits after the comma.

2.13.17 (2011-07-29)
--------------------

- Put back the `weightedIntersection` optimization but guard against results
  with values and do the appropriate fallback to the weighted version.

2.13.16 (2011-07-24)
--------------------

- Restored preserving score values from ZCTextIndex indices.
  https://bugs.launchpad.net/zope2/+bug/815469

2.13.15 (2011-06-30)
--------------------

- Fixed undefined variables in BooleanIndex inline migration code.

- Fixed BooleanIndex' items method so the ZMI browse view works.

2.13.14 (2011-05-19)
--------------------

- Fixed addition of two LazyCat's if any of them was already flattened.

- Extend BooleanIndex by making the indexed value variable instead of
  hardcoding it to `True`. The indexed value will determine the smaller set
  automatically and choose its best value. An inline switch is done once the
  indexed value set grows larger than 60% of the total length. 60% was chosen
  to avoid constant switching for indexes that have an almost equal
  distribution of `True/False`.

- Substitute catalog entry in UUIDIndex error message.

2.13.13 (2011-05-04)
--------------------

- Optimize `Catalog.updateMetadata` avoiding a `self.uids` lookup and removing
  inline migration code for converting `self.data` from non-IOBTree types.

- In the path index, don't update data if the value hasn't changed.

2.13.12 (2011-05-02)
--------------------

- Optimize DateRangeIndex for better conflict resolution handling. It always
  starts out with storing an IITreeSet of the value instead of special casing
  storing an int for a single value. The `single value as int` optimization
  should be provided via a separate API to be called periodically outside the
  context of a normal request.

- Replaced `weightedIntersection` and `weightedUnion` calls with their
  non-weighted version, as we didn't pass in weights.

2.13.11 (2011-05-02)
--------------------

- Fix possible TypeError in `sortResults` method if only b_start but not b_size
  has been provided.

- Prevent the new UUIDIndex from acquiring attributes via Acquisition.

2.13.10 (2011-04-21)
--------------------

- Handle `TypeErrors` in the KeywordIndex if an indexed attribute is a method
  with required arguments.

- Added reporting of the intersection time of each index' result with the
  result set of the other indexes and consider this time to be part of each
  index time for prioritizing the index.

- Removed tracking of result length from the query plan. The calculation of the
  length of an intermediate index result can itself be expensive.

2.13.9 (2011-04-10)
-------------------

- Added a floor and ceiling value to the date range index. Values outside the
  specified range will be interpreted the same way as passing `None`, i.e.
  `since the beginning of time` and `until the end of it`. This allows the
  index to apply its optimizations, while objects with values outside this
  range can still be stored in a normal date index, which omits explicitly
  passed in `None` values.

2.13.8 (2011-04-01)
-------------------

- Fixed bug in date range index, which would omit objects exactly matching the
  query term if a resultset was provided.

- Fixed the BooleanIndex to not index objects without the cataloged attribute.

2.13.7 (2011-02-15)
-------------------

- Fixed the `DateIndex._unindex` to be of type `IIBTree` instead of `OIBTree`.
  It stores document ids as keys, which can only be ints.

2.13.6 (2011-02-10)
-------------------

- Remove docstrings from various methods, as they shouldn't be web-publishable.

2.13.5 (2011-02-05)
-------------------

- Fixed test failures introduced in 2.13.4.

2.13.4 (2011-02-05)
-------------------

- Added a new UUIDIndex, based on the common UnIndex. It behaves like a
  FieldIndex, but can only store one document id per value, so there's a 1:1
  mapping from value to document id. An error is logged if a different document
  id is indexed for an already taken value. The internal data structures are
  optimized for this and avoid storing one IITreeSet per value.

- Optimize sorting in presence of batching arguments. If a batch from the end
  of the result set is requested, we internally reverse the sorting order and
  at the end reverse the lazy sequence again. In a sequence with 100 entries,
  if we request the batch with items 80 to 90, we now reverse sort 20 items
  (100 to 80), slice of the first ten items and then reverse them. Before we
  would had to sort the first 90 items and then slice of the last 10.

- If batching arguments are provided, limit the returned lazy sequence to the
  items in the required batch instead of returning leading items falling
  outside of the requested batch.

- Fixed inline `IISet` to `IITreeSet` conversion code inside DateRangeIndex'
  `_insertForwardIndexEntry` method.

2.13.3 (2011-01-01)
-------------------

- Avoid locale-dependent test condition in `test_length_with_filter`.

2.13.2 (2010-12-31)
-------------------

- Preserve `actual_result_count` on flattening nested LazyCat's.

- Preserve the `actual_result_count` on all lazy return values. This allows
  to get proper batching information from catalog results which have been
  restricted by `sort_limit`.

- Made sure `actual_result_count` is available on all lazy classes and falls
  back to `__len__` if not explicitly provided.

- Optimized length calculation of Lazy classes.

2.13.1 (2010-12-25)
-------------------

- Added automatic sorting limit calculation based on batch arguments. If the
  query contains a `b_start` and `b_size` argument and no explicit `sort_limit`
  is provided, the sort limit will be calculated as `b_start + b_size`.

- Avoid pre-allocation of marker items in `LazyMap`.

2.13.0 (2010-12-25)
-------------------

- Fix `LazyMap` to avoid unnecessary function calls.

- Released as separate distribution.
