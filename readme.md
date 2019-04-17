BleveFDB
======

[FoundationDB](https://www.foundationdb.org/) key-value store implementation for [Bleve](http://blevesearch.com/) text indexing library. Currently in development so use with care (see warning at the bottom).

Prerequisites
------

* Install [FoundationDB](https://www.foundationdb.org/download/)


Package dependencies
------
* FoundationDB GO bindings [git](https://github.com/apple/foundationdb/tree/master/bindings/go)
* Bleve [git](https://github.com/blevesearch/bleve)

Dependencies are handled with modules.

Tests
------
Test are currently configured to use FoundationDB 6.0.X.
During tests there are 2 folders created and removed at the end of the tests.
To prevent deletion of those folders a flag **-cleanup=false** can be used (see example below).
1. index which uses FoundationDB at a root level creates a folder roottest.bleve in the tests folder
2. index which uses FoundationDB with a subspace creates a folder subtest.bleve in the tests folder (fails because it is still in development)

```
go test
or
go test -cleanup=false
```

Warning
------
FoundationDB usage with subspaces is currently in development and the search is not working.