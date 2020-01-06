
ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 1330000;


-- ===================================================================
-- test end-to-end modification functionality for mx tables
-- ===================================================================

-- basic single-row INSERT
INSERT INTO limit_orders_mx VALUES (32743, 'AAPL', 9580, '2004-10-19 10:23:54', 'buy',
								 20.69);
SELECT COUNT(*) FROM limit_orders_mx WHERE id = 32743;

-- now singe-row INSERT from a worker
\c - - - :worker_1_port
INSERT INTO limit_orders_mx VALUES (32744, 'AAPL', 9580, '2004-10-19 10:23:54', 'buy',
								 20.69);
SELECT COUNT(*) FROM limit_orders_mx WHERE id = 32744;

-- now singe-row INSERT to the other worker
\c - - - :worker_2_port
\set VERBOSITY terse

INSERT INTO limit_orders_mx VALUES (32745, 'AAPL', 9580, '2004-10-19 10:23:54', 'buy',
								 20.69);
SELECT COUNT(*) FROM limit_orders_mx WHERE id = 32745;

-- and see all the inserted rows
SELECT * FROM limit_orders_mx ORDER BY 1;

-- basic single-row INSERT with RETURNING
INSERT INTO limit_orders_mx VALUES (32746, 'AAPL', 9580, '2004-10-19 10:23:54', 'buy', 20.69) RETURNING *;

-- INSERT with DEFAULT in the target list
INSERT INTO limit_orders_mx VALUES (12756, 'MSFT', 10959, '2013-05-08 07:29:23', 'sell',
								 DEFAULT);
SELECT * FROM limit_orders_mx WHERE id = 12756;

-- INSERT with expressions in target list
INSERT INTO limit_orders_mx VALUES (430, upper('ibm'), 214, timestamp '2003-01-28 10:31:17' +
								 interval '5 hours', 'buy', sqrt(2));
SELECT * FROM limit_orders_mx WHERE id = 430;

-- INSERT without partition key
INSERT INTO limit_orders_mx DEFAULT VALUES;

-- squelch WARNINGs that contain worker_port
SET client_min_messages TO ERROR;

-- INSERT violating NOT NULL constraint
INSERT INTO limit_orders_mx VALUES (NULL, 'T', 975234, DEFAULT);

-- INSERT violating column constraint
INSERT INTO limit_orders_mx VALUES (18811, 'BUD', 14962, '2014-04-05 08:32:16', 'sell',
								 -5.00);
-- INSERT violating primary key constraint
INSERT INTO limit_orders_mx VALUES (32743, 'LUV', 5994, '2001-04-16 03:37:28', 'buy', 0.58);

-- INSERT violating primary key constraint, with RETURNING specified.
INSERT INTO limit_orders_mx VALUES (32743, 'LUV', 5994, '2001-04-16 03:37:28', 'buy', 0.58) RETURNING *;

-- INSERT, with RETURNING specified, failing with a non-constraint error
INSERT INTO limit_orders_mx VALUES (34153, 'LEE', 5994, '2001-04-16 03:37:28', 'buy', 0.58) RETURNING id / 0;

SET client_min_messages TO DEFAULT;

-- commands with non-constant partition values are unsupported
INSERT INTO limit_orders_mx VALUES (random() * 100, 'ORCL', 152, '2011-08-25 11:50:45',
								 'sell', 0.58);

-- values for other columns are totally fine
INSERT INTO limit_orders_mx VALUES (2036, 'GOOG', 5634, now(), 'buy', random());

-- commands with mutable functions in their quals
DELETE FROM limit_orders_mx WHERE id = 246 AND bidder_id = (random() * 1000);

-- commands with mutable but non-volatile functions(ie: stable func.) in their quals
-- (the cast to timestamp is because the timestamp_eq_timestamptz operator is stable)
DELETE FROM limit_orders_mx WHERE id = 246 AND placed_at = current_timestamp::timestamp;

-- commands with multiple rows are supported
INSERT INTO limit_orders_mx VALUES (2037, 'GOOG', 5634, now(), 'buy', random()),
                                   (2038, 'GOOG', 5634, now(), 'buy', random()),
                                   (2039, 'GOOG', 5634, now(), 'buy', random());

-- connect back to the other node
\c - - - :worker_1_port

-- commands containing a CTE are supported
WITH deleted_orders AS (DELETE FROM limit_orders_mx WHERE id < 0 RETURNING *)
INSERT INTO limit_orders_mx SELECT * FROM deleted_orders;

-- test simple DELETE
INSERT INTO limit_orders_mx VALUES (246, 'TSLA', 162, '2007-07-02 16:32:15', 'sell', 20.69);
SELECT COUNT(*) FROM limit_orders_mx WHERE id = 246;

DELETE FROM limit_orders_mx WHERE id = 246;
SELECT COUNT(*) FROM limit_orders_mx WHERE id = 246;

-- test simple DELETE with RETURNING
DELETE FROM limit_orders_mx WHERE id = 430 RETURNING *;
SELECT COUNT(*) FROM limit_orders_mx WHERE id = 430;

-- DELETE with expression in WHERE clause
INSERT INTO limit_orders_mx VALUES (246, 'TSLA', 162, '2007-07-02 16:32:15', 'sell', 20.69);
SELECT COUNT(*) FROM limit_orders_mx WHERE id = 246;

DELETE FROM limit_orders_mx WHERE id = (2 * 123);
SELECT COUNT(*) FROM limit_orders_mx WHERE id = 246;

-- multi shard delete is supported
DELETE FROM limit_orders_mx WHERE bidder_id = 162;

-- commands with a USING clause are unsupported
CREATE TABLE bidders ( name text, id bigint );
DELETE FROM limit_orders_mx USING bidders WHERE limit_orders_mx.id = 246 AND
											 limit_orders_mx.bidder_id = bidders.id AND
											 bidders.name = 'Bernie Madoff';

-- commands containing a CTE are supported
WITH new_orders AS (INSERT INTO limit_orders_mx VALUES (411, 'FLO', 12, '2017-07-02 16:32:15', 'buy', 66))
DELETE FROM limit_orders_mx WHERE id < 0;

-- cursors are not supported
DELETE FROM limit_orders_mx WHERE CURRENT OF cursor_name;

INSERT INTO limit_orders_mx VALUES (246, 'TSLA', 162, '2007-07-02 16:32:15', 'sell', 20.69);

-- simple UPDATE
UPDATE limit_orders_mx SET symbol = 'GM' WHERE id = 246;
SELECT symbol FROM limit_orders_mx WHERE id = 246;

-- simple UPDATE with RETURNING
UPDATE limit_orders_mx SET symbol = 'GM' WHERE id = 246 RETURNING *;

-- expression UPDATE
UPDATE limit_orders_mx SET bidder_id = 6 * 3 WHERE id = 246;
SELECT bidder_id FROM limit_orders_mx WHERE id = 246;

-- expression UPDATE with RETURNING
UPDATE limit_orders_mx SET bidder_id = 6 * 5 WHERE id = 246 RETURNING *;

-- multi-column UPDATE
UPDATE limit_orders_mx SET (kind, limit_price) = ('buy', DEFAULT) WHERE id = 246;
SELECT kind, limit_price FROM limit_orders_mx WHERE id = 246;

-- multi-column UPDATE with RETURNING
UPDATE limit_orders_mx SET (kind, limit_price) = ('buy', 999) WHERE id = 246 RETURNING *;

-- Test that on unique contraint violations, we fail fast
INSERT INTO limit_orders_mx VALUES (275, 'ADR', 140, '2007-07-02 16:32:15', 'sell', 43.67);
INSERT INTO limit_orders_mx VALUES (275, 'ADR', 140, '2007-07-02 16:32:15', 'sell', 43.67);

-- multi shard update is supported
UPDATE limit_orders_mx SET limit_price = 0.00;

-- attempting to change the partition key is unsupported
UPDATE limit_orders_mx SET id = 0 WHERE id = 246;

-- UPDATEs with a FROM clause are unsupported
UPDATE limit_orders_mx SET limit_price = 0.00 FROM bidders
					WHERE limit_orders_mx.id = 246 AND
						  limit_orders_mx.bidder_id = bidders.id AND
						  bidders.name = 'Bernie Madoff';

-- commands containing a CTE are supported
WITH deleted_orders AS (INSERT INTO limit_orders_mx VALUES (399, 'PDR', 14, '2017-07-02 16:32:15', 'sell', 43))
UPDATE limit_orders_mx SET symbol = 'GM';

SELECT symbol, bidder_id FROM limit_orders_mx WHERE id = 246;

-- updates referencing just a var are supported
UPDATE limit_orders_mx SET bidder_id = id WHERE id = 246;

-- updates referencing a column are supported
UPDATE limit_orders_mx SET bidder_id = bidder_id + 1 WHERE id = 246;

-- IMMUTABLE functions are allowed
UPDATE limit_orders_mx SET symbol = LOWER(symbol) WHERE id = 246;

SELECT symbol, bidder_id FROM limit_orders_mx WHERE id = 246;

-- IMMUTABLE functions are allowed -- even in returning
UPDATE limit_orders_mx SET symbol = UPPER(symbol) WHERE id = 246 RETURNING id, LOWER(symbol), symbol;

-- connect coordinator to run the DDL
\c - - - :master_port
ALTER TABLE limit_orders_mx ADD COLUMN array_of_values integer[];

-- connect back to the other node
\c - - - :worker_2_port

-- updates referencing STABLE functions are allowed
UPDATE limit_orders_mx SET placed_at = LEAST(placed_at, now()::timestamp) WHERE id = 246;
-- so are binary operators
UPDATE limit_orders_mx SET array_of_values = 1 || array_of_values WHERE id = 246;

-- connect back to the other node
\c - - - :worker_2_port

-- immutable function calls with vars are also allowed
UPDATE limit_orders_mx
SET array_of_values = immutable_append_mx(array_of_values, 2) WHERE id = 246;

CREATE FUNCTION stable_append_mx(old_values int[], new_value int)
RETURNS int[] AS $$ BEGIN RETURN old_values || new_value; END; $$
LANGUAGE plpgsql STABLE;

-- but STABLE function calls with vars are not allowed
UPDATE limit_orders_mx
SET array_of_values = stable_append_mx(array_of_values, 3) WHERE id = 246;

SELECT array_of_values FROM limit_orders_mx WHERE id = 246;

-- STRICT functions work as expected
CREATE FUNCTION temp_strict_func(integer,integer) RETURNS integer AS
'SELECT COALESCE($1, 2) + COALESCE($1, 3);' LANGUAGE SQL STABLE STRICT;
UPDATE limit_orders_mx SET bidder_id = temp_strict_func(1, null) WHERE id = 246;

SELECT array_of_values FROM limit_orders_mx WHERE id = 246;

-- connect coordinator to run the DDL
\c - - - :master_port
ALTER TABLE limit_orders_mx DROP array_of_values;

-- connect back to the other node
\c - - - :worker_2_port

-- even in RETURNING
UPDATE limit_orders_mx SET placed_at = placed_at WHERE id = 246 RETURNING NOW();

-- cursors are not supported
UPDATE limit_orders_mx SET symbol = 'GM' WHERE CURRENT OF cursor_name;

-- check that multi-row UPDATE/DELETEs with RETURNING work
INSERT INTO multiple_hash_mx VALUES ('0', '1');
INSERT INTO multiple_hash_mx VALUES ('0', '2');
INSERT INTO multiple_hash_mx VALUES ('0', '3');
INSERT INTO multiple_hash_mx VALUES ('0', '4');
INSERT INTO multiple_hash_mx VALUES ('0', '5');
INSERT INTO multiple_hash_mx VALUES ('0', '6');

UPDATE multiple_hash_mx SET data = data ||'-1' WHERE category = '0' RETURNING *;
DELETE FROM multiple_hash_mx WHERE category = '0' RETURNING *;

-- ensure returned row counters are correct
\set QUIET off
INSERT INTO multiple_hash_mx VALUES ('1', '1');
INSERT INTO multiple_hash_mx VALUES ('1', '2');
INSERT INTO multiple_hash_mx VALUES ('1', '3');
INSERT INTO multiple_hash_mx VALUES ('2', '1');
INSERT INTO multiple_hash_mx VALUES ('2', '2');
INSERT INTO multiple_hash_mx VALUES ('2', '3');
INSERT INTO multiple_hash_mx VALUES ('2', '3') RETURNING *;

-- check that update return the right number of rows
-- one row
UPDATE multiple_hash_mx SET data = data ||'-1' WHERE category = '1' AND data = '1';
-- three rows
UPDATE multiple_hash_mx SET data = data ||'-2' WHERE category = '1';
-- three rows, with RETURNING
UPDATE multiple_hash_mx SET data = data ||'-2' WHERE category = '1' RETURNING category;
-- check
SELECT * FROM multiple_hash_mx WHERE category = '1' ORDER BY category, data;

-- check that deletes return the right number of rows
-- one row
DELETE FROM multiple_hash_mx WHERE category = '2' AND data = '1';
-- two rows
DELETE FROM multiple_hash_mx WHERE category = '2';
-- three rows, with RETURNING
DELETE FROM multiple_hash_mx WHERE category = '1' RETURNING category;
-- check
SELECT * FROM multiple_hash_mx WHERE category = '1' ORDER BY category, data;
SELECT * FROM multiple_hash_mx WHERE category = '2' ORDER BY category, data;

--- INSERT ... SELECT ... FROM commands are supported from workers
INSERT INTO multiple_hash_mx
SELECT s, s*2 FROM generate_series(1,10) s;

-- including distributed INSERT ... SELECT
BEGIN;
SET LOCAL client_min_messages TO DEBUG1;
INSERT INTO multiple_hash_mx SELECT * FROM multiple_hash_mx;
END;

-- verify interaction of default values, SERIAL, and RETURNING
\set QUIET on

-- make sure this test always returns the same output no matter which tests have run
SELECT minimum_value::bigint AS min_value,
       maximum_value::bigint AS max_value
  FROM information_schema.sequences
  WHERE sequence_name = 'app_analytics_events_mx_id_seq' \gset
SELECT last_value FROM app_analytics_events_mx_id_seq \gset
ALTER SEQUENCE app_analytics_events_mx_id_seq NO MINVALUE NO MAXVALUE;
SELECT setval('app_analytics_events_mx_id_seq'::regclass, 3940649673949184);

INSERT INTO app_analytics_events_mx VALUES (DEFAULT, 101, 'Fauxkemon Geaux') RETURNING id;
INSERT INTO app_analytics_events_mx (app_id, name) VALUES (102, 'Wayz') RETURNING id;
INSERT INTO app_analytics_events_mx (app_id, name) VALUES (103, 'Mynt') RETURNING *;

-- clean up
SELECT setval('app_analytics_events_mx_id_seq'::regclass, :last_value);
ALTER SEQUENCE app_analytics_events_mx_id_seq
  MINVALUE :min_value MAXVALUE :max_value;
