-- Tests for boolean aggregates in multi-shard queries
CREATE SCHEMA bool_agg;
SET search_path TO bool_agg;

CREATE TABLE bool_test (id int, val int, flag bool, kind int);
SELECT create_distributed_table('bool_agg.bool_test','id');
INSERT INTO bool_test VALUES (1, 1, true, 99), (2, 2, false, 99), (2, 3, true, 88);

-- mix of true and false
SELECT bool_and(flag), bool_or(flag), every(flag) FROM bool_test;
SELECT kind, bool_and(flag), bool_or(flag), every(flag) FROM bool_test GROUP BY kind ORDER BY 2;

-- expressions in aggregate
SELECT bool_or(val > 2 OR id < 2), bool_and(val < 3) FROM bool_test;
SELECT kind, bool_or(val > 2 OR id < 2), bool_and(val < 3) FROM bool_test GROUP BY kind ORDER BY 3;

-- 1 & 3, 1 | 3
SELECT bit_and(val), bit_or(val) FROM bool_test WHERE flag;
SELECT flag, bit_and(val), bit_or(val) FROM bool_test GROUP BY flag ORDER BY flag;

DROP SCHEMA bool_agg CASCADE;
