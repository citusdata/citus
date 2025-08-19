CREATE SCHEMA citus_aggregated_stats;
SET search_path TO citus_aggregated_stats, public;
SET citus.shard_count = 2;
SET citus.next_shard_id TO 1870000;

CREATE USER user1;
GRANT ALL ON SCHEMA citus_aggregated_stats TO user1;

SET SESSION AUTHORIZATION user1;

CREATE TABLE current_check (currentid int, payload text, rlsuser text);
GRANT ALL ON current_check TO PUBLIC;

INSERT INTO current_check VALUES
    (1, 'abc', 'user1'),
    (3, 'cde', 'user1'),
    (4, 'def', 'user1'),
    (4, 'def', 'user1'),
    (3, 'cde', 'user2'),
    (5, NULL, NULL),
    (4, 'def', 'user1');

ALTER TABLE current_check ENABLE ROW LEVEL SECURITY;

SET row_security TO ON;

ANALYZE current_check;

SELECT schemaname, tablename, attname, null_frac, most_common_vals, most_common_freqs FROM pg_stats
  WHERE tablename IN ('current_check', 'dist_current_check', 'ref_current_check', 'citus_local_current_check')
  ORDER BY 1;

SELECT * FROM citus_stats
  WHERE tablename IN ('current_check', 'dist_current_check', 'ref_current_check', 'citus_local_current_check')
  ORDER BY 1;

-- test various Citus table types in citus_stats
CREATE TABLE dist_current_check (currentid int, payload text, rlsuser text);
CREATE TABLE ref_current_check (currentid int, payload text, rlsuser text);
CREATE TABLE citus_local_current_check (currentid int, payload text, rlsuser text);

SELECT create_distributed_table('dist_current_check', 'currentid');
SELECT create_reference_table('ref_current_check');
SELECT citus_add_local_table_to_metadata('citus_local_current_check');

INSERT INTO dist_current_check VALUES
    (1, 'abc', 'user1'),
    (3, 'cde', 'user1'),
    (4, 'def', 'user1'),
    (4, 'def', 'user1'),
    (3, 'cde', 'user2'),
    (5, NULL, NULL),
    (4, 'def', 'user1');

INSERT INTO ref_current_check VALUES
    (1, 'abc', 'user1'),
    (3, 'cde', 'user1'),
    (4, 'def', 'user1'),
    (4, 'def', 'user1'),
    (3, 'cde', 'user2'),
    (5, NULL, NULL),
    (4, 'def', 'user1');

INSERT INTO citus_local_current_check VALUES
    (1, 'abc', 'user1'),
    (3, 'cde', 'user1'),
    (4, 'def', 'user1'),
    (4, 'def', 'user1'),
    (3, 'cde', 'user2'),
    (5, NULL, NULL),
    (4, 'def', 'user1');

ANALYZE dist_current_check;
ANALYZE ref_current_check;
ANALYZE citus_local_current_check;

SELECT schemaname, tablename, attname, null_frac, most_common_vals, most_common_freqs FROM pg_stats
  WHERE tablename IN ('current_check', 'dist_current_check', 'ref_current_check', 'citus_local_current_check')
  ORDER BY 1;

SELECT * FROM citus_stats
  WHERE tablename IN ('current_check', 'dist_current_check', 'ref_current_check', 'citus_local_current_check')
  ORDER BY 1;

RESET SESSION AUTHORIZATION;
DROP SCHEMA citus_aggregated_stats CASCADE;
DROP USER user1;
