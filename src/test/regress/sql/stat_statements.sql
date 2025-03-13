--
-- stat_statements
--
-- tests citus_stat_statements functionality

SET compute_query_id = 'on';

-- check if pg_stat_statements is available
SELECT name FROM pg_available_extensions WHERE name = 'pg_stat_statements';

SELECT regexp_split_to_array(setting, ',') @> ARRAY['pg_stat_statements'] AS stats_loaded
FROM pg_settings WHERE name = 'shared_preload_libraries';

DROP EXTENSION IF EXISTS pg_stat_statements;
-- verify it is not loaded yet
SELECT extname FROM pg_extension WHERE extname = 'pg_stat_statements';

-- this should error out since extension is not created yet
SELECT * FROM citus_stat_statements;

-- create extension if available
SELECT CASE WHEN COUNT(*) > 0 THEN
    'CREATE EXTENSION  pg_stat_statements'
ELSE 'SELECT false as pg_stat_statements_available' END
AS create_cmd FROM pg_available_extensions()
WHERE name = 'pg_stat_statements'
\gset

:create_cmd;

CREATE FUNCTION normalize_query_string(query_string text)
	RETURNS TEXT
    LANGUAGE plpgsql
    AS $function$
BEGIN
	RETURN rtrim(regexp_replace(query_string, '\$\d+', '?', 'g'), ';');
END;
$function$;

-- verify citus stat statements reset
SELECT citus_stat_statements_reset();

CREATE TABLE test(a int);
SELECT create_distributed_table('test','a');
insert into test values(1);

select query, calls from citus_stat_statements();
SET compute_query_id = 'off';

-- for pg >= 14, since compute_query_id is off, this insert
-- shouldn't be tracked
-- for pg < 14, we disable it explicitly so that we don't need
-- to add an alternative output file.
insert into test values(1);
select query, calls from citus_stat_statements();


SET compute_query_id = 'on';


SELECT citus_stat_statements_reset();


-- it should now succeed, but with empty result
SELECT normalize_query_string(query) FROM citus_stat_statements;

-- run some queries
SELECT count(*) FROM lineitem_hash_part;
SELECT count(*) FROM lineitem_hash_part;
SELECT l_orderkey FROM lineitem_hash_part;
SELECT l_orderkey FROM lineitem_hash_part WHERE l_orderkey > 100;
SELECT count(*) FROM lineitem_hash_part WHERE l_orderkey = 4;
SELECT count(*) FROM lineitem_hash_part WHERE l_orderkey = 4;
SELECT count(*) FROM lineitem_hash_part WHERE l_orderkey = 1;
SELECT count(*) FROM lineitem_hash_part WHERE l_orderkey = 1200;

SELECT normalize_query_string(query), executor, partition_key, calls
FROM citus_stat_statements
ORDER BY 1, 2, 3, 4;

-- test GUC citus.stat_statements_track
SET citus.stat_statements_track TO 'none';
-- this shouldn't increment the call count since tracking is disabled
SELECT count(*) FROM lineitem_hash_part WHERE l_orderkey = 4;
-- this should give the same output as above, since the last query is not counted
SELECT normalize_query_string(query), executor, partition_key, calls
FROM citus_stat_statements
ORDER BY 1, 2, 3, 4;
-- reset the GUC to track stats
SET citus.stat_statements_track TO 'all';

-- reset pg_stat_statements and verify it also cleans citus_stat_statements output
-- verify that entries are actually removed from citus_stat_statements
SELECT pg_stat_statements_reset() IS NOT NULL AS t;
SELECT * FROM citus_stat_statements;

-- run some queries
SELECT count(*) FROM lineitem_hash_part;
SELECT count(*) FROM lineitem_hash_part;
SELECT l_orderkey FROM lineitem_hash_part;
SELECT l_orderkey FROM lineitem_hash_part WHERE l_orderkey > 100;
SELECT count(*) FROM lineitem_hash_part WHERE l_orderkey = 4;
SELECT count(*) FROM lineitem_hash_part WHERE l_orderkey = 4;
SELECT count(*) FROM lineitem_hash_part WHERE l_orderkey = 1;
SELECT count(*) FROM lineitem_hash_part WHERE l_orderkey = 1200;

-- show current list, and reset pg_stat_statements
SELECT normalize_query_string(query), executor, partition_key, calls
FROM citus_stat_statements
ORDER BY 1, 2, 3, 4;

SELECT pg_stat_statements_reset() IS NOT NULL AS t;

SELECT count(*) FROM lineitem_hash_part;
SELECT count(*) FROM lineitem_hash_part WHERE l_orderkey = 4;

-- verify although pg_stat_statements was reset, some call counts are not
-- if a query is re-issued between pg_stat_statements_reset() and citus_stat_statements()
-- its call count is preserved.
SELECT normalize_query_string(query), executor, partition_key, calls
FROM citus_stat_statements
ORDER BY 1, 2, 3, 4;


-- citus_stat_statements_reset() must be called to reset call counts
SELECT citus_stat_statements_reset();

SELECT count(*) FROM lineitem_hash_part;
SELECT count(*) FROM lineitem_hash_part WHERE l_orderkey = 4;

-- verify citus stats has only 2 rows
SELECT normalize_query_string(query), executor, partition_key, calls
FROM citus_stat_statements
ORDER BY 1, 2, 3, 4;

-- create test tables to run update/delete scenarios
CREATE TABLE stat_test_text(user_id text, value int);
CREATE TABLE stat_test_bigint(user_id bigint, value int);

SELECT citus_stat_statements_reset();

-- verify regular tables are not included in citus_stat_statements
SELECT * FROM stat_test_text;
SELECT * FROM stat_test_bigint WHERE user_id = 1::bigint;

SELECT normalize_query_string(query), executor, partition_key, calls
FROM citus_stat_statements
ORDER BY 1, 2, 3, 4;

SELECT create_distributed_table('stat_test_text', 'user_id');
SELECT create_distributed_table('stat_test_bigint', 'user_id');

SELECT normalize_query_string(query), executor, partition_key, calls
FROM citus_stat_statements
ORDER BY 1, 2, 3, 4;

SELECT * FROM stat_test_text;
SELECT * FROM stat_test_text WHERE user_id = 'me';

SELECT * FROM stat_test_bigint;
SELECT * FROM stat_test_bigint WHERE user_id = 2::bigint;

SELECT normalize_query_string(query), executor, partition_key, calls
FROM citus_stat_statements
ORDER BY 1, 2, 3, 4;

-- insert some rows and check stats
INSERT INTO stat_test_bigint VALUES (1, 1);
INSERT INTO stat_test_bigint VALUES (7, 1);
INSERT INTO stat_test_bigint VALUES (7, 1), (2,3);
INSERT INTO stat_test_bigint VALUES (8, 1), (8,3);

SELECT normalize_query_string(query), executor, partition_key, calls
FROM citus_stat_statements
ORDER BY 1, 2, 3, 4;

-- delete some rows and check stats
SELECT citus_stat_statements_reset();

DELETE FROM stat_test_bigint WHERE value > 1000;
DELETE FROM stat_test_bigint WHERE value > 1200;
DELETE FROM stat_test_bigint WHERE user_id > 1000;
DELETE FROM stat_test_bigint WHERE user_id = 1000;
DELETE FROM stat_test_bigint WHERE user_id = 1000;

SELECT normalize_query_string(query), executor, partition_key, calls
FROM citus_stat_statements
ORDER BY 1, 2, 3, 4;

-- update some rows and check stats
SELECT citus_stat_statements_reset();

UPDATE stat_test_bigint SET value = 300 WHERE value = 3000;
UPDATE stat_test_bigint SET value = 320 WHERE value = 3200;
UPDATE stat_test_bigint SET value = 300 WHERE user_id = 3;
UPDATE stat_test_bigint SET value = 300 WHERE user_id = 3;
UPDATE stat_test_bigint SET value = 3000 WHERE user_id > 500;

SELECT normalize_query_string(query), executor, partition_key, calls
FROM citus_stat_statements
ORDER BY 1, 2, 3, 4;

-- test joins
CREATE TABLE stat_test_bigint_other(LIKE stat_test_bigint);
SELECT create_distributed_table('stat_test_bigint_other', 'user_id');

SELECT citus_stat_statements_reset();

INSERT INTO stat_test_bigint_other SELECT * FROM stat_test_bigint;
INSERT INTO stat_test_bigint_other SELECT * FROM stat_test_bigint WHERE user_id = 3;
INSERT INTO stat_test_bigint_other SELECT * FROM stat_test_bigint WHERE user_id = 3;

SELECT count(*) FROM stat_test_bigint b JOIN stat_test_bigint_other o USING (user_id);
SELECT count(*) FROM stat_test_bigint b JOIN stat_test_bigint_other o USING (user_id);

SELECT count(*) FROM stat_test_bigint b JOIN stat_test_bigint_other o USING (user_id)
WHERE b.user_id = 3;

SELECT count(*) FROM stat_test_bigint b JOIN stat_test_bigint_other o USING (user_id)
WHERE b.user_id = 3;

SELECT count(*) FROM stat_test_bigint b JOIN stat_test_bigint_other o USING (user_id)
WHERE o.user_id = 3;

SELECT normalize_query_string(query), executor, partition_key, calls FROM citus_stat_statements
ORDER BY 1, 2, 3, 4;

-- test reference table
CREATE TABLE stat_test_reference(LIKE stat_test_bigint);
SELECT create_reference_table('stat_test_reference');

INSERT INTO stat_test_reference SELECT user_id, count(*) FROM stat_test_bigint GROUP BY user_id;

SELECT citus_stat_statements_reset();

SELECT count(*) FROM stat_test_reference;
SELECT count(*) FROM stat_test_reference WHERE user_id = 2;
SELECT count(*) FROM stat_test_reference WHERE user_id = 2;

SELECT count(*) FROM stat_test_bigint b JOIN stat_test_reference r USING (user_id);
SELECT count(*) FROM stat_test_bigint b JOIN stat_test_reference r USING (user_id);

SELECT count(*) FROM stat_test_bigint b JOIN stat_test_reference r USING (user_id)
WHERE b.user_id = 1;

SELECT count(*) FROM stat_test_bigint b JOIN stat_test_reference r USING (user_id)
WHERE b.user_id = 1 and r.value > 0;

SELECT count(*) FROM stat_test_bigint b JOIN stat_test_reference r USING (user_id)
WHERE r.user_id = 1;

SELECT normalize_query_string(query), executor, partition_key, calls
FROM citus_stat_statements
ORDER BY 1, 2, 3, 4;

-- non-stats role should only see its own entries, even when calling citus_query_stats directly
CREATE USER nostats;
GRANT SELECT ON TABLE lineitem_hash_part TO nostats;

SET ROLE nostats;
SELECT count(*) FROM lineitem_hash_part WHERE l_orderkey = 2;
SELECT partition_key FROM citus_query_stats();
RESET ROLE;

-- stats-role/superuser should be able to see entries belonging to other users
SELECT partition_key FROM citus_query_stats() WHERE partition_key = '2';

-- drop pg_stat_statements and verify citus_stat_statement does not work anymore
DROP extension pg_stat_statements;
SELECT normalize_query_string(query), executor, partition_key, calls
FROM citus_stat_statements
ORDER BY 1, 2, 3, 4;

-- drop created tables
DROP TABLE stat_test_text, stat_test_bigint, stat_test_bigint_other, stat_test_reference;

DROP FUNCTION normalize_query_string(text);


SET compute_query_id = 'off';
