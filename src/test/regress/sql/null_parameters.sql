-- this file contain tests with pruning of NULL
-- values with prepared statements
CREATE SCHEMA null_parameters;
SET search_path TO null_parameters;

SET citus.next_shard_id TO 1680000;
SET citus.shard_count to 32;
CREATE TABLE text_dist_column (key text, value text);
SELECT create_distributed_table('text_dist_column', 'key');

-- it seems that sometimes the pruning is deferred and sometimes not.
-- what we care about is that these queries don't crash and the log for this
-- one shouldn't matter. This is to prevent these test being from flaky in our CI.
SET client_min_messages to NOTICE;

PREPARE null_select_on_text AS SELECT count(*) FROM text_dist_column WHERE key = NULL;
EXECUTE null_select_on_text;
EXECUTE null_select_on_text;
EXECUTE null_select_on_text;
EXECUTE null_select_on_text;
EXECUTE null_select_on_text;
EXECUTE null_select_on_text;

PREPARE null_select_on_text_param(text) AS SELECT count(*) FROM text_dist_column WHERE key = $1;
EXECUTE null_select_on_text_param(NULL);
EXECUTE null_select_on_text_param(NULL);
EXECUTE null_select_on_text_param(5::text);
EXECUTE null_select_on_text_param(NULL);
EXECUTE null_select_on_text_param(NULL);
EXECUTE null_select_on_text_param(NULL);
EXECUTE null_select_on_text_param(NULL);
EXECUTE null_select_on_text_param(NULL);
EXECUTE null_select_on_text_param(NULL::varchar);
EXECUTE null_select_on_text_param('test'::varchar);
EXECUTE null_select_on_text_param(5::text);

PREPARE null_select_on_text_param_and_false(text) AS SELECT count(*) FROM text_dist_column WHERE key = $1 AND false;
EXECUTE null_select_on_text_param_and_false(NULL);
EXECUTE null_select_on_text_param_and_false(NULL);
EXECUTE null_select_on_text_param_and_false(NULL);
EXECUTE null_select_on_text_param_and_false(NULL);
EXECUTE null_select_on_text_param_and_false(NULL);
EXECUTE null_select_on_text_param_and_false(NULL);
EXECUTE null_select_on_text_param_and_false(NULL);

PREPARE null_select_on_text_param_and_in(text) AS SELECT count(*) FROM text_dist_column WHERE key IN ($1);
EXECUTE null_select_on_text_param_and_in(NULL);
EXECUTE null_select_on_text_param_and_in(NULL);
EXECUTE null_select_on_text_param_and_in(NULL);
EXECUTE null_select_on_text_param_and_in(NULL);
EXECUTE null_select_on_text_param_and_in(NULL);
EXECUTE null_select_on_text_param_and_in(NULL);

PREPARE null_select_on_text_param_and_in_2(text) AS SELECT count(*) FROM text_dist_column WHERE key IN ($1, 'test');
EXECUTE null_select_on_text_param_and_in_2(NULL);
EXECUTE null_select_on_text_param_and_in_2(NULL);
EXECUTE null_select_on_text_param_and_in_2(NULL);
EXECUTE null_select_on_text_param_and_in_2(NULL);
EXECUTE null_select_on_text_param_and_in_2(NULL);
EXECUTE null_select_on_text_param_and_in_2(NULL);

PREPARE null_select_on_text_param_and_in_3(text, text) AS SELECT count(*) FROM text_dist_column WHERE key IN ($1, $2);
EXECUTE null_select_on_text_param_and_in_3(NULL, NULL);
EXECUTE null_select_on_text_param_and_in_3(NULL, NULL);
EXECUTE null_select_on_text_param_and_in_3(NULL, NULL);
EXECUTE null_select_on_text_param_and_in_3(NULL, NULL);
EXECUTE null_select_on_text_param_and_in_3(NULL, NULL);
EXECUTE null_select_on_text_param_and_in_3(NULL, NULL);


PREPARE null_select_on_text_param_and_in_4(text) AS SELECT count(*) FROM text_dist_column WHERE key IN ($1) AND false;
EXECUTE null_select_on_text_param_and_in_4(NULL);
EXECUTE null_select_on_text_param_and_in_4(NULL);
EXECUTE null_select_on_text_param_and_in_4(NULL);
EXECUTE null_select_on_text_param_and_in_4(NULL);
EXECUTE null_select_on_text_param_and_in_4(NULL);
EXECUTE null_select_on_text_param_and_in_4(NULL);

-- not a fast-path, still good to run
PREPARE null_select_on_text_param_and_in_5(text, text) AS SELECT count(*) FROM text_dist_column WHERE key = $1 OR key = $2;
EXECUTE null_select_on_text_param_and_in_5(NULL, NULL);
EXECUTE null_select_on_text_param_and_in_5(NULL, NULL);
EXECUTE null_select_on_text_param_and_in_5(1, NULL);
EXECUTE null_select_on_text_param_and_in_5(NULL, NULL);
EXECUTE null_select_on_text_param_and_in_5(NULL, NULL);
EXECUTE null_select_on_text_param_and_in_5(NULL, NULL);
EXECUTE null_select_on_text_param_and_in_5(NULL, NULL);
EXECUTE null_select_on_text_param_and_in_5(NULL, NULL);
EXECUTE null_select_on_text_param_and_in_5(1, NULL);

-- not a fast-path, still good to have
PREPARE null_select_on_text_param_and_in_6(text) AS SELECT count(*) FROM text_dist_column WHERE key NOT IN (SELECT value FROM text_dist_column WHERE key = $1);
EXECUTE null_select_on_text_param_and_in_6(NULL);
EXECUTE null_select_on_text_param_and_in_6(NULL);
EXECUTE null_select_on_text_param_and_in_6(NULL);
EXECUTE null_select_on_text_param_and_in_6(NULL);
EXECUTE null_select_on_text_param_and_in_6(NULL);
EXECUTE null_select_on_text_param_and_in_6(NULL);
EXECUTE null_select_on_text_param_and_in_6(NULL);

-- different expression types which may not be fast-path queries
PREPARE null_select_on_text_param_7(text) AS SELECT count(*) FROM text_dist_column WHERE (key = $1) is true;
EXECUTE null_select_on_text_param_7(NULL);
EXECUTE null_select_on_text_param_7(NULL);
EXECUTE null_select_on_text_param_7(NULL);
EXECUTE null_select_on_text_param_7(NULL);
EXECUTE null_select_on_text_param_7(NULL);
EXECUTE null_select_on_text_param_7(NULL);
EXECUTE null_select_on_text_param_7(NULL);

-- different expression types which may not be fast-path queries
PREPARE null_select_on_text_param_8(text) AS SELECT count(*) FROM text_dist_column WHERE key = ANY(ARRAY[$1]);
EXECUTE null_select_on_text_param_8(NULL);
EXECUTE null_select_on_text_param_8(NULL);
EXECUTE null_select_on_text_param_8(NULL);
EXECUTE null_select_on_text_param_8(NULL);
EXECUTE null_select_on_text_param_8(NULL);
EXECUTE null_select_on_text_param_8(NULL);
EXECUTE null_select_on_text_param_8(NULL);


PREPARE null_select_on_text_param_9(text) AS SELECT count(*) FROM text_dist_column WHERE key = ANY(ARRAY[$1, 'test']);
EXECUTE null_select_on_text_param_9(NULL);
EXECUTE null_select_on_text_param_9(NULL);
EXECUTE null_select_on_text_param_9(NULL);
EXECUTE null_select_on_text_param_9(NULL);
EXECUTE null_select_on_text_param_9(NULL);
EXECUTE null_select_on_text_param_9(NULL);
EXECUTE null_select_on_text_param_9(NULL);

-- different expression types which may not be fast-path queries
PREPARE null_select_on_text_param_10(text) AS SELECT count(*) FROM text_dist_column WHERE key = ALL(ARRAY[$1]);
EXECUTE null_select_on_text_param_10(NULL);
EXECUTE null_select_on_text_param_10(NULL);
EXECUTE null_select_on_text_param_10(NULL);
EXECUTE null_select_on_text_param_10(NULL);
EXECUTE null_select_on_text_param_10(NULL);
EXECUTE null_select_on_text_param_10(NULL);
EXECUTE null_select_on_text_param_10(NULL);

-- different expression types which may not be fast-path queries
PREPARE null_select_on_text_param_11(text) AS SELECT count(*) FROM text_dist_column WHERE (CASE WHEN key > $1 THEN key::int / 100 > 1 ELSE false END);
EXECUTE null_select_on_text_param_11(NULL);
EXECUTE null_select_on_text_param_11(NULL);
EXECUTE null_select_on_text_param_11(NULL);
EXECUTE null_select_on_text_param_11(NULL);
EXECUTE null_select_on_text_param_11(NULL);
EXECUTE null_select_on_text_param_11(NULL);
EXECUTE null_select_on_text_param_11(NULL);
EXECUTE null_select_on_text_param_11(NULL);

-- different expression types which may not be fast-path queries
PREPARE null_select_on_text_param_12(text) AS SELECT count(*) FROM text_dist_column WHERE COALESCE(($1::int/50000)::bool, false);

EXECUTE null_select_on_text_param_12(NULL);
EXECUTE null_select_on_text_param_12(NULL);
EXECUTE null_select_on_text_param_12(NULL);
EXECUTE null_select_on_text_param_12(NULL);
EXECUTE null_select_on_text_param_12(NULL);
EXECUTE null_select_on_text_param_12(NULL);
EXECUTE null_select_on_text_param_12(NULL);

-- different expression types which may not be fast-path queries
PREPARE null_select_on_text_param_13(text) AS SELECT count(*) FROM text_dist_column WHERE NULLIF(($1::int/50000)::bool, false);

EXECUTE null_select_on_text_param_13(NULL);
EXECUTE null_select_on_text_param_13(NULL);
EXECUTE null_select_on_text_param_13(NULL);
EXECUTE null_select_on_text_param_13(NULL);
EXECUTE null_select_on_text_param_13(NULL);
EXECUTE null_select_on_text_param_13(NULL);
EXECUTE null_select_on_text_param_13(NULL);

PREPARE null_select_on_text_param_14(text) AS SELECT count(*) FROM text_dist_column WHERE key = $1 AND 0!=0;
EXECUTE null_select_on_text_param_14(NULL);
EXECUTE null_select_on_text_param_14(NULL);
EXECUTE null_select_on_text_param_14(NULL);
EXECUTE null_select_on_text_param_14(NULL);
EXECUTE null_select_on_text_param_14(NULL);
EXECUTE null_select_on_text_param_14(NULL);
EXECUTE null_select_on_text_param_14(NULL);
EXECUTE null_select_on_text_param_14(NULL);

PREPARE null_select_on_text_param_15(text) AS SELECT count(*) FROM text_dist_column WHERE row(key, 100) > row($1, 0);
EXECUTE null_select_on_text_param_15(NULL);
EXECUTE null_select_on_text_param_15(NULL);
EXECUTE null_select_on_text_param_15(NULL);
EXECUTE null_select_on_text_param_15(NULL);
EXECUTE null_select_on_text_param_15(NULL);
EXECUTE null_select_on_text_param_15(NULL);
EXECUTE null_select_on_text_param_15(NULL);
EXECUTE null_select_on_text_param_15(NULL);


PREPARE null_select_on_text_param_16(text) AS SELECT count(*) FROM text_dist_column WHERE key IS DISTINCT FROM $1;
EXECUTE null_select_on_text_param_16(NULL);
EXECUTE null_select_on_text_param_16(NULL);
EXECUTE null_select_on_text_param_16(NULL);
EXECUTE null_select_on_text_param_16(NULL);
EXECUTE null_select_on_text_param_16(NULL);
EXECUTE null_select_on_text_param_16(NULL);
EXECUTE null_select_on_text_param_16(NULL);
EXECUTE null_select_on_text_param_16(NULL);



PREPARE null_select_on_text_param_17(text) AS SELECT count(*) FROM text_dist_column WHERE key = $1 AND 0!=1;
EXECUTE null_select_on_text_param_17(NULL);
EXECUTE null_select_on_text_param_17(NULL);
EXECUTE null_select_on_text_param_17(NULL);
EXECUTE null_select_on_text_param_17(NULL);
EXECUTE null_select_on_text_param_17(NULL);
EXECUTE null_select_on_text_param_17(NULL);
EXECUTE null_select_on_text_param_17(NULL);
EXECUTE null_select_on_text_param_17(NULL);

PREPARE null_select_on_text_param_18(text) AS SELECT count(*) FROM text_dist_column WHERE key = upper($1);
EXECUTE null_select_on_text_param_18(NULL);
EXECUTE null_select_on_text_param_18(NULL);
EXECUTE null_select_on_text_param_18(NULL);
EXECUTE null_select_on_text_param_18(NULL);
EXECUTE null_select_on_text_param_18(NULL);
EXECUTE null_select_on_text_param_18(NULL);
EXECUTE null_select_on_text_param_18(NULL);

PREPARE null_select_on_text_param_19(text) AS SELECT count(*) FROM text_dist_column WHERE key = $1::varchar(4);
EXECUTE null_select_on_text_param_19(NULL);
EXECUTE null_select_on_text_param_19(NULL);
EXECUTE null_select_on_text_param_19(NULL);
EXECUTE null_select_on_text_param_19(NULL);
EXECUTE null_select_on_text_param_19(NULL);
EXECUTE null_select_on_text_param_19(NULL);
EXECUTE null_select_on_text_param_19(NULL);


PREPARE null_update_on_text AS UPDATE text_dist_column SET value = '' WHERE key = NULL;
EXECUTE null_update_on_text(NULL);
EXECUTE null_update_on_text(NULL);
EXECUTE null_update_on_text(NULL);
EXECUTE null_update_on_text(NULL);
EXECUTE null_update_on_text(NULL);
EXECUTE null_update_on_text(NULL);


PREPARE null_update_on_text_param(text) AS UPDATE text_dist_column SET value = '' WHERE key = $1;
EXECUTE null_update_on_text_param(NULL);
EXECUTE null_update_on_text_param(NULL);
EXECUTE null_update_on_text_param(NULL);
EXECUTE null_update_on_text_param(NULL);
EXECUTE null_update_on_text_param(NULL);
EXECUTE null_update_on_text_param(NULL);
EXECUTE null_update_on_text_param(NULL);


PREPARE null_update_on_text_param_and_false(text) AS UPDATE text_dist_column SET value = '' WHERE key = $1 AND false;
EXECUTE null_update_on_text_param_and_false(NULL);
EXECUTE null_update_on_text_param_and_false(NULL);
EXECUTE null_update_on_text_param_and_false(NULL);
EXECUTE null_update_on_text_param_and_false(NULL);
EXECUTE null_update_on_text_param_and_false(NULL);
EXECUTE null_update_on_text_param_and_false(NULL);
EXECUTE null_update_on_text_param_and_false(NULL);



PREPARE null_update_on_text_param_and_in(text) AS UPDATE text_dist_column SET value = '' WHERE key IN ($1);
EXECUTE null_update_on_text_param_and_in(NULL);
EXECUTE null_update_on_text_param_and_in(NULL);
EXECUTE null_update_on_text_param_and_in(NULL);
EXECUTE null_update_on_text_param_and_in(NULL);
EXECUTE null_update_on_text_param_and_in(NULL);
EXECUTE null_update_on_text_param_and_in(NULL);
EXECUTE null_update_on_text_param_and_in(NULL);



PREPARE null_update_on_text_param_and_in_2(text) AS UPDATE text_dist_column SET value = '' WHERE key IN ($1, 'test');
EXECUTE null_update_on_text_param_and_in_2(NULL);
EXECUTE null_update_on_text_param_and_in_2(NULL);
EXECUTE null_update_on_text_param_and_in_2(NULL);
EXECUTE null_update_on_text_param_and_in_2(NULL);
EXECUTE null_update_on_text_param_and_in_2(NULL);
EXECUTE null_update_on_text_param_and_in_2(NULL);
EXECUTE null_update_on_text_param_and_in_2(NULL);


PREPARE null_update_on_text_param_and_in_3(text, text) AS UPDATE text_dist_column SET value = '' WHERE key IN ($1, $2);
EXECUTE null_update_on_text_param_and_in_3(NULL, NULL);
EXECUTE null_update_on_text_param_and_in_3(NULL, NULL);
EXECUTE null_update_on_text_param_and_in_3(NULL, NULL);
EXECUTE null_update_on_text_param_and_in_3(NULL, NULL);
EXECUTE null_update_on_text_param_and_in_3(NULL, NULL);
EXECUTE null_update_on_text_param_and_in_3(NULL, NULL);
EXECUTE null_update_on_text_param_and_in_3(NULL, 'test');

PREPARE null_update_on_text_param_and_in_4(text) AS UPDATE text_dist_column SET value = '' WHERE key IN ($1) and false;
EXECUTE null_update_on_text_param_and_in_4(NULL);
EXECUTE null_update_on_text_param_and_in_4(NULL);
EXECUTE null_update_on_text_param_and_in_4(NULL);
EXECUTE null_update_on_text_param_and_in_4(NULL);
EXECUTE null_update_on_text_param_and_in_4(NULL);
EXECUTE null_update_on_text_param_and_in_4(NULL);
EXECUTE null_update_on_text_param_and_in_4(NULL);

-- sanity check with JSBONB column
CREATE TABLE jsonb_dist_column (key jsonb, value text);
SELECT create_distributed_table('jsonb_dist_column', 'key');
PREPARE null_select_on_json_param(jsonb) AS SELECT count(*) FROM jsonb_dist_column WHERE key = $1;
EXECUTE null_select_on_json_param(NULL);
EXECUTE null_select_on_json_param(NULL);
EXECUTE null_select_on_json_param(NULL);
EXECUTE null_select_on_json_param(NULL);
EXECUTE null_select_on_json_param(NULL);
EXECUTE null_select_on_json_param(NULL);
EXECUTE null_select_on_json_param(NULL);

SET client_min_messages TO ERROR;
DROP SCHEMA null_parameters CASCADE;
