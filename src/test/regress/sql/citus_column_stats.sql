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
    (3, 'cde', 'user2');

ALTER TABLE current_check ENABLE ROW LEVEL SECURITY;

SET row_security TO ON;

ANALYZE current_check;

SELECT attname, most_common_vals, most_common_freqs FROM pg_stats
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
    (3, 'cde', 'user2');

ANALYZE dist_current_check;

SELECT attname, most_common_vals, most_common_freqs FROM pg_stats
  WHERE tablename = 'dist_current_check'
  ORDER BY 1;

SELECT * FROM citus_column_stats('dist_current_check');

DROP TABLE current_check;
DROP TABLE dist_current_check;

RESET SESSION AUTHORIZATION;
REVOKE ALL ON SCHEMA public FROM user1;
DROP USER user1;
