CompositeIndex README

  Overview 

     CompositeIndex is a plugin index for the ZCatalog. Indexes containing
     more than one attribute to index an object are called “composite
     index”. Such indexes should be created if you expect to run
     queries that will have multiple attributes in the search phrase
     and all attributes combined will give significantly less hits
     than the any of the attributes alone. The key of a composite
     index is called “composite key” and is composed of two or more
     attributes of an object.

     Catalog queries containing attributes managed by CompositeIndex
     are transparently catched and transformed seamlessly into a
     CompositeIndex query. In particular, large sites with a
     combination of additional indexes (FieldIndex, KeywordIndex, BooleanIndex) 
     and lots of content (>100k) will profit. The expected performance
     enhancement for combined index queries is about a factor of >2-3.

     For example many catalog queries in plone are based on the combination of
     indexed attributes as follows: 'Language', 'review_state',
     'portal_type' and 'allowedRolesAndUsers'. Normally, the ZCatalog
     sequentially executes each corresponding atomic index and
     calculates intersection between each result. This strategy, in
     particular for large sites, decreases the performance of the
     catalog and simultaneously increases the volatility of ZODB’s
     object cache, because each index individually has a high number
     of hits whereas the the intersection between each index result
     has a low number of hits.

     CompositeIndex overcomes this difficulty because it already
     contains a pre-calculateted intersection by means of its
     composite keys. The loading of large sets and the following
     expensive computation of the intersection is therefore obsolete.

     IMPORTANT: CompositeIndex can only be used as an add-on not as 
     a replacement for field, keyword and boolean indexes.
