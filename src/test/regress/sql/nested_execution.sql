SET search_path TO nested_execution;
SET citus.enable_local_execution TO on;
\set VERBOSITY terse

-- nested execution from queries on distributed tables is generally disallowed
SELECT dist_query_single_shard(key) FROM distributed WHERE key = 1;
SELECT dist_query_multi_shard() FROM distributed WHERE key = 1;
SELECT ref_query() FROM distributed WHERE key = 1;

SELECT dist_query_single_shard(key) FROM distributed LIMIT 1;
SELECT dist_query_multi_shard() FROM distributed LIMIT 1;
SELECT ref_query() FROM distributed LIMIT 1;

-- nested execution is allowed outside of an aggregate
-- note that this behaviour is different if distributed has only 1 shard
-- however, this test always uses 4 shards
SELECT dist_query_single_shard(count(*)::int) FROM distributed;
SELECT dist_query_multi_shard()+count(*) FROM distributed;
SELECT ref_query()+count(*) FROM distributed;

-- nested execution is allowed in a query that only has intermediate results
SELECT dist_query_single_shard(key) FROM (SELECT key FROM distributed LIMIT 1) s;
SELECT dist_query_multi_shard() FROM (SELECT key FROM distributed LIMIT 1) s;
SELECT ref_query() FROM (SELECT key FROM distributed LIMIT 1) s;

-- nested execution from queries on reference tables is generally allowed
SELECT dist_query_single_shard(id::int) FROM reference WHERE id = 1;
SELECT dist_query_multi_shard() FROM reference WHERE id = 1;
SELECT ref_query() FROM reference WHERE id = 1;

-- repeat checks in insert..select (somewhat different code path)
INSERT INTO distributed SELECT dist_query_single_shard(key) FROM distributed WHERE key = 1;
INSERT INTO distributed SELECT dist_query_multi_shard() FROM distributed WHERE key = 1;
INSERT INTO distributed SELECT ref_query() FROM distributed WHERE key = 1;

INSERT INTO distributed SELECT dist_query_single_shard(key) FROM distributed LIMIT 1;
INSERT INTO distributed SELECT dist_query_multi_shard() FROM distributed LIMIT 1;
INSERT INTO distributed SELECT ref_query() FROM distributed LIMIT 1;

BEGIN;
INSERT INTO distributed SELECT dist_query_single_shard(count(*)::int) FROM distributed;
INSERT INTO distributed SELECT dist_query_multi_shard()+count(*) FROM distributed;
INSERT INTO distributed SELECT ref_query()+count(*) FROM distributed;
ROLLBACK;

BEGIN;
INSERT INTO distributed SELECT dist_query_single_shard(key) FROM (SELECT key FROM distributed LIMIT 1) s;
INSERT INTO distributed SELECT dist_query_multi_shard() FROM (SELECT key FROM distributed LIMIT 1) s;
INSERT INTO distributed SELECT ref_query() FROM (SELECT key FROM distributed LIMIT 1) s;
ROLLBACK;

BEGIN;
INSERT INTO distributed SELECT dist_query_single_shard(id::int) FROM reference WHERE id = 1;
INSERT INTO distributed SELECT dist_query_multi_shard() FROM reference WHERE id = 1;
INSERT INTO distributed SELECT ref_query() FROM reference WHERE id = 1;
ROLLBACK;

-- nested execution without local execution is disallowed (not distinguishable from queries on shard)
SET citus.enable_local_execution TO off;

SELECT dist_query_single_shard(id::int) FROM reference WHERE id = 1;
SELECT dist_query_multi_shard() FROM reference WHERE id = 1;
SELECT ref_query() FROM reference WHERE id = 1;
