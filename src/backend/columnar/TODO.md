To see the list of features and bug-fixes planned for next releases, see our
[development roadmap][roadmap].

Requested Features
------------------

* Improve write performance
* Improve read performance
* Add checksum logic
* Add new compression methods
* Enable INSERT/DELETE/UPDATE
* Enable users other than superuser to safely create columnar tables (permissions)
* Transactional semantics
* Add config setting to make pg\_fsync() optional


Known Issues
------------

* Copy command ignores NOT NULL constraints.
* Planning functions don't take into account average column width.
* Planning functions don't correctly take into account block skipping benefits.
* On 32-bit platforms, when file size is outside the 32-bit signed range, EXPLAIN
  command prints incorrect file size.
* If two different columnar tables are configured to point to the same file,
  writes to the underlying file aren't protected from each other.
* When a data load is in progress, concurrent reads on the table overestimate the
  page count.
* We have a minor memory leak in CStoreEndWrite. We need to also free the
  comparisonFunctionArray.
* block\_filtering test fails on Ubuntu because the "da\_DK" locale is not enabled
  by default.
* We don't yet incorporate the compression method's impact on disk I/O into cost
  estimates.
* CitusDB integration errors:
* Concurrent staging cstore\_fdw tables doesn't work.
* Setting a default value for column with ALTER TABLE has limited support for
  existing rows.

[roadmap]: https://github.com/citusdata/cstore_fdw/wiki/Roadmap

