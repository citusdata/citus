-- Exercise both cache representations in one backend, including a rolled-back
-- catalog change. The placement rows used below belong to this fixture alone.
CREATE SCHEMA compact_cache_test;
SET search_path TO compact_cache_test;
SET citus.shard_replication_factor TO 1;
CREATE FUNCTION check_cached_shard(bigint, bigint, "char", bigint, boolean)
RETURNS boolean AS 'citus', 'test_check_cached_shard' LANGUAGE C STRICT;
CREATE FUNCTION load_bounds(bigint, anyelement)
RETURNS anyarray AS 'citus', 'load_shard_interval_array' LANGUAGE C STRICT;
CREATE TABLE events(k int);
SELECT create_distributed_table('events','k',colocate_with=>'none',shard_count=>4);
CREATE VIEW snapshot AS
SELECT s.shardid, p.placementid, s.shardstorage, p.shardlength, p.groupid
FROM pg_dist_shard s JOIN pg_dist_placement p USING (shardid)
WHERE s.logicalrelid='events'::regclass;
SELECT bool_and(check_cached_shard(shardid, placementid, shardstorage, shardlength, true))
FROM snapshot;
COPY events FROM STDIN;
1
2
3
4
\.
SET citus.enable_fast_path_router_planner TO false;
SELECT array_agg(k ORDER BY k) FROM events WHERE k=1 OR k=2 OR k=1;
SELECT array_agg(k ORDER BY k) FROM events WHERE k=1 OR k>2;
SELECT array_agg(k ORDER BY k) FROM events;
BEGIN;
UPDATE pg_dist_placement SET shardlength=31 WHERE placementid=(SELECT min(placementid) FROM snapshot);
SELECT bool_and(check_cached_shard(shardid, placementid, shardstorage, shardlength, false))
FROM snapshot;
ROLLBACK;
SELECT bool_and(check_cached_shard(shardid, placementid, shardstorage, shardlength, true))
FROM snapshot;
-- Missing and additional placements force the whole table to the general layout.
BEGIN;
DELETE FROM pg_dist_placement WHERE placementid=(SELECT min(placementid) FROM snapshot);
SELECT count(*)=3 AND bool_and(check_cached_shard(shardid, placementid, shardstorage, shardlength, false))
FROM snapshot;
ROLLBACK;
BEGIN;
INSERT INTO pg_dist_placement(shardid,shardstate,shardlength,groupid)
SELECT s.shardid,1,0,n.groupid FROM snapshot s JOIN pg_dist_node n ON n.groupid<>s.groupid
WHERE n.groupid>0 ORDER BY s.shardid,n.groupid LIMIT 1;
SELECT count(*)=5 AND bool_and(check_cached_shard(shardid, placementid, shardstorage, shardlength, false))
FROM snapshot;
ROLLBACK;
-- Make the first interval wider by one token and the second narrower.
-- Change storage on only one shard and verify both endpoints and shard routing.
BEGIN;
UPDATE pg_dist_shard SET
 shardmaxvalue=CASE WHEN shardmaxvalue='-1073741825' THEN '-1073741824' ELSE shardmaxvalue END,
 shardminvalue=CASE WHEN shardminvalue='-1073741824' THEN '-1073741823' ELSE shardminvalue END,
 shardstorage=CASE WHEN shardminvalue='-2147483648' THEN 'f'::"char" ELSE shardstorage END
WHERE logicalrelid='events'::regclass;
SELECT bool_and(check_cached_shard(shardid, placementid, shardstorage, shardlength, true))
FROM snapshot;
SELECT bool_and(load_bounds(shardid,0)=ARRAY[shardminvalue::int,shardmaxvalue::int])
FROM pg_dist_shard WHERE logicalrelid='events'::regclass;
SELECT bool_and(get_shard_id_for_distribution_column('events',k)=s.shardid)
FROM generate_series(-100,100) k JOIN pg_dist_shard s ON
 hashint4(k) BETWEEN s.shardminvalue::int AND s.shardmaxvalue::int
WHERE s.logicalrelid='events'::regclass;
ROLLBACK;
SET client_min_messages TO warning;
DROP SCHEMA compact_cache_test CASCADE;
