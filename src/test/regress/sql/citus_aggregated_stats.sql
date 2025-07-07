SET citus.shard_count = 2;

CREATE USER user1;
GRANT ALL ON SCHEMA public TO user1;

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

SELECT attname, null_frac, most_common_vals, most_common_freqs FROM pg_stats
  WHERE tablename = 'current_check'
  ORDER BY 1;

SELECT * FROM citus_column_stats('current_check');

CREATE TABLE dist_current_check (currentid int, payload text, rlsuser text);
SELECT create_distributed_table('dist_current_check', 'currentid');

INSERT INTO dist_current_check VALUES
    (1, 'abc', 'user1'),
    (3, 'cde', 'user1'),
    (4, 'def', 'user1'),
    (4, 'def', 'user1'),
    (3, 'cde', 'user2'),
    (5, NULL, NULL);

ANALYZE dist_current_check;

SELECT attname, null_frac, most_common_vals, most_common_freqs FROM pg_stats
  WHERE tablename = 'dist_current_check'
  ORDER BY 1;

SELECT * FROM citus_column_stats('dist_current_check');

DROP TABLE current_check;
DROP TABLE dist_current_check;

RESET SESSION AUTHORIZATION;
RESET row_security;

-- compare pg_stat_user_tables with citus_stat_user_tables
CREATE TABLE trunc_stats_test(id serial);
CREATE TABLE trunc_stats_dist_test(id serial);
SELECT create_distributed_table('trunc_stats_dist_test', 'id');

-- rollback a savepoint: this should count 4 inserts and have 2
-- live tuples after commit (and 2 dead ones due to aborted subxact)
BEGIN;

INSERT INTO trunc_stats_test DEFAULT VALUES;
INSERT INTO trunc_stats_test DEFAULT VALUES;

INSERT INTO trunc_stats_dist_test DEFAULT VALUES;
INSERT INTO trunc_stats_dist_test DEFAULT VALUES;

SAVEPOINT p1;

INSERT INTO trunc_stats_test DEFAULT VALUES;
INSERT INTO trunc_stats_test DEFAULT VALUES;
TRUNCATE trunc_stats_test;
INSERT INTO trunc_stats_test DEFAULT VALUES;

INSERT INTO trunc_stats_dist_test DEFAULT VALUES;
INSERT INTO trunc_stats_dist_test DEFAULT VALUES;
TRUNCATE trunc_stats_dist_test;
INSERT INTO trunc_stats_dist_test DEFAULT VALUES;

ROLLBACK TO SAVEPOINT p1;
COMMIT;

\c - - - :worker_1_port
SELECT pg_stat_force_next_flush();
\c - - - :worker_2_port
SELECT pg_stat_force_next_flush();
\c - - - :master_port
SELECT pg_stat_force_next_flush();

SELECT relname, n_tup_ins, n_live_tup, n_dead_tup
FROM pg_stat_user_tables
WHERE relname like 'trunc_stats%';

SELECT relname, n_tup_ins, n_live_tup, n_dead_tup
FROM citus_stat_user_tables();

REVOKE ALL ON SCHEMA public FROM user1;
DROP USER user1;
DROP TABLE trunc_stats_test;
DROP TABLE trunc_stats_dist_test;
