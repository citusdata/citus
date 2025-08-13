CREATE SCHEMA citus_aggregated_stats;
SET search_path TO citus_aggregated_stats, public;
SET citus.shard_count = 2;

CREATE USER user1;
GRANT ALL ON SCHEMA citus_aggregated_stats, public TO user1;

SET SESSION AUTHORIZATION user1;

CREATE TABLE current_check (currentid int, payload text, rlsuser text);
GRANT ALL ON current_check TO PUBLIC;

INSERT INTO current_check VALUES
    (1, 'abc', 'user1'),
    (3, 'cde', 'user1'),
    (4, 'def', 'user1'),
    (4, 'def', 'user1'),
    (3, 'cde', 'user2'),
    (5, NULL, NULL);

ALTER TABLE current_check ENABLE ROW LEVEL SECURITY;

SET row_security TO ON;

ANALYZE current_check;

SELECT schemaname, tablename, attname, null_frac, most_common_vals, most_common_freqs FROM pg_stats
  WHERE tablename IN ('current_check', 'dist_current_check_0', 'dist_current_check_1')
  ORDER BY 1;

SELECT * FROM citus_stats
  WHERE tablename IN ('current_check', 'dist_current_check_0', 'dist_current_check_1')
  ORDER BY 1;

CREATE TABLE dist_current_check_0 (currentid int, payload text, rlsuser text);
CREATE TABLE dist_current_check_1 (currentid int, payload text, rlsuser text);
SELECT create_distributed_table('dist_current_check_0', 'currentid');
SELECT create_distributed_table('dist_current_check_1', 'currentid');

INSERT INTO dist_current_check_0 VALUES
    (1, 'abc', 'user1'),
    (3, 'cde', 'user1'),
    (4, 'def', 'user1'),
    (4, 'def', 'user1'),
    (3, 'cde', 'user2'),
    (5, NULL, NULL);

INSERT INTO dist_current_check_1 VALUES
    (1, 'abc', 'user1'),
    (3, 'cde', 'user1'),
    (4, 'def', 'user1'),
    (4, 'def', 'user1'),
    (3, 'cde', 'user2'),
    (5, NULL, NULL);

ANALYZE dist_current_check_0;
ANALYZE dist_current_check_1;

SELECT schemaname, tablename, attname, null_frac, most_common_vals, most_common_freqs FROM pg_stats
  WHERE tablename IN ('current_check', 'dist_current_check_0', 'dist_current_check_1')
  ORDER BY 1;

SELECT * FROM citus_stats
  WHERE tablename IN ('current_check', 'dist_current_check_0', 'dist_current_check_1')
  ORDER BY 1;

DROP SCHEMA citus_aggregated_stats CASCADE;

RESET SESSION AUTHORIZATION;
REVOKE ALL ON SCHEMA public FROM user1;
DROP USER user1;
