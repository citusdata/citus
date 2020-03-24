SET citus.shard_count TO 32;
SET citus.next_shard_id TO 750000;
SET citus.next_placement_id TO 750000;

CREATE SCHEMA multi_modifications;

-- some failure messages that comes from the worker nodes
-- might change due to parallel executions, so suppress those
-- using \set VERBOSITY terse

-- ===================================================================
-- test end-to-end modification functionality
-- ===================================================================

CREATE TYPE order_side AS ENUM ('buy', 'sell');

CREATE TABLE limit_orders (
	id bigint PRIMARY KEY,
	symbol text NOT NULL,
	bidder_id bigint NOT NULL,
	placed_at timestamp NOT NULL,
	kind order_side NOT NULL,
	limit_price decimal NOT NULL DEFAULT 0.00 CHECK (limit_price >= 0.00)
);


CREATE TABLE multiple_hash (
	category text NOT NULL,
	data text NOT NULL
);

CREATE TABLE insufficient_shards ( LIKE limit_orders );
CREATE TABLE range_partitioned ( LIKE limit_orders );
CREATE TABLE append_partitioned ( LIKE limit_orders );

SET citus.shard_count TO 2;

SELECT create_distributed_table('limit_orders', 'id', 'hash');
SELECT create_distributed_table('multiple_hash', 'id', 'hash');
SELECT create_distributed_table('range_partitioned', 'id', 'range');
SELECT create_distributed_table('append_partitioned', 'id', 'append');

SET citus.shard_count TO 1;
SET citus.shard_replication_factor TO 1;
-- make a single shard that covers no partition values
SELECT create_distributed_table('insufficient_shards', 'id', 'hash');
UPDATE pg_dist_shard SET shardminvalue = 0, shardmaxvalue = 0
WHERE logicalrelid = 'insufficient_shards'::regclass;

-- create range-partitioned shards
SELECT master_create_empty_shard('range_partitioned') AS new_shard_id
\gset
UPDATE pg_dist_shard SET shardminvalue = 0, shardmaxvalue = 49999
WHERE shardid = :new_shard_id;

SELECT master_create_empty_shard('range_partitioned') AS new_shard_id
\gset
UPDATE pg_dist_shard SET shardminvalue = 50000, shardmaxvalue = 99999
WHERE shardid = :new_shard_id;

-- create append-partitioned shards
SELECT master_create_empty_shard('append_partitioned') AS new_shard_id
\gset
UPDATE pg_dist_shard SET shardminvalue = 0, shardmaxvalue = 500000
WHERE shardid = :new_shard_id;

SELECT master_create_empty_shard('append_partitioned') AS new_shard_id
\gset
UPDATE pg_dist_shard SET shardminvalue = 500000, shardmaxvalue = 1000000
WHERE shardid = :new_shard_id;

-- basic single-row INSERT
INSERT INTO limit_orders VALUES (32743, 'AAPL', 9580, '2004-10-19 10:23:54', 'buy',
								 20.69);
SELECT COUNT(*) FROM limit_orders WHERE id = 32743;

-- basic single-row INSERT with RETURNING
INSERT INTO limit_orders VALUES (32744, 'AAPL', 9580, '2004-10-19 10:23:54', 'buy', 20.69) RETURNING *;

-- try a single-row INSERT with no shard to receive it
INSERT INTO insufficient_shards VALUES (32743, 'AAPL', 9580, '2004-10-19 10:23:54', 'buy',
										20.69);

-- try an insert to a range-partitioned table
INSERT INTO range_partitioned VALUES (32743, 'AAPL', 9580, '2004-10-19 10:23:54', 'buy',
									  20.69);

-- also insert to an append-partitioned table
INSERT INTO append_partitioned VALUES (414123, 'AAPL', 9580, '2004-10-19 10:23:54', 'buy',
									   20.69);
-- ensure the values are where we put them and query to ensure they are properly pruned
SET client_min_messages TO 'DEBUG2';
SELECT * FROM range_partitioned WHERE id = 32743;
SELECT * FROM append_partitioned WHERE id = 414123;
SET client_min_messages TO DEFAULT;

-- try inserting without a range-partitioned shard to receive the value
INSERT INTO range_partitioned VALUES (999999, 'AAPL', 9580, '2004-10-19 10:23:54', 'buy',
									  20.69);

-- and insert into an append-partitioned table with a value that spans shards:
INSERT INTO append_partitioned VALUES (500000, 'AAPL', 9580, '2004-10-19 10:23:54', 'buy',
									   20.69);

-- INSERT with DEFAULT in the target list
INSERT INTO limit_orders VALUES (12756, 'MSFT', 10959, '2013-05-08 07:29:23', 'sell',
								 DEFAULT);
SELECT COUNT(*) FROM limit_orders WHERE id = 12756;

-- INSERT with expressions in target list
INSERT INTO limit_orders VALUES (430, upper('ibm'), 214, timestamp '2003-01-28 10:31:17' +
								 interval '5 hours', 'buy', sqrt(2));
SELECT COUNT(*) FROM limit_orders WHERE id = 430;

-- INSERT without partition key
INSERT INTO limit_orders DEFAULT VALUES;

-- squelch WARNINGs that contain worker_port
SET client_min_messages TO ERROR;

-- INSERT violating NOT NULL constraint
INSERT INTO limit_orders VALUES (NULL, 'T', 975234, DEFAULT);

-- INSERT violating column constraint
\set VERBOSITY terse
INSERT INTO limit_orders VALUES (18811, 'BUD', 14962, '2014-04-05 08:32:16', 'sell',
								 -5.00);
-- INSERT violating primary key constraint
INSERT INTO limit_orders VALUES (32743, 'LUV', 5994, '2001-04-16 03:37:28', 'buy', 0.58);

-- INSERT violating primary key constraint, with RETURNING specified.
INSERT INTO limit_orders VALUES (32743, 'LUV', 5994, '2001-04-16 03:37:28', 'buy', 0.58) RETURNING *;

-- INSERT, with RETURNING specified, failing with a non-constraint error
INSERT INTO limit_orders VALUES (34153, 'LEE', 5994, '2001-04-16 03:37:28', 'buy', 0.58) RETURNING id / 0;

\set VERBOSITY DEFAULT
SET client_min_messages TO DEFAULT;

-- commands with non-constant partition values are supported
INSERT INTO limit_orders VALUES (random() * 100, 'ORCL', 152, '2011-08-25 11:50:45',
								 'sell', 0.58);

-- values for other columns are totally fine
INSERT INTO limit_orders VALUES (2036, 'GOOG', 5634, now(), 'buy', random());

-- commands with mutable functions in their quals
DELETE FROM limit_orders WHERE id = 246 AND bidder_id = (random() * 1000);

-- commands with mutable but non-volatile functions(ie: stable func.) in their quals
-- (the cast to timestamp is because the timestamp_eq_timestamptz operator is stable)
DELETE FROM limit_orders WHERE id = 246 AND placed_at = current_timestamp::timestamp;

-- multi-row inserts are supported
INSERT INTO limit_orders VALUES (12037, 'GOOG', 5634, '2001-04-16 03:37:28', 'buy', 0.50),
                                (12038, 'GOOG', 5634, '2001-04-17 03:37:28', 'buy', 2.50),
                                (12039, 'GOOG', 5634, '2001-04-18 03:37:28', 'buy', 1.50);

SELECT COUNT(*) FROM limit_orders WHERE id BETWEEN 12037 AND 12039;

-- even those with functions and returning
INSERT INTO limit_orders VALUES (22037, 'GOOG', 5634, now(), 'buy', 0.50),
                                (22038, 'GOOG', 5634, now(), 'buy', 2.50),
                                (22039, 'GOOG', 5634, now(), 'buy', 1.50)
RETURNING id;

SELECT COUNT(*) FROM limit_orders WHERE id BETWEEN 22037 AND 22039;

-- even those with functions in their partition columns
INSERT INTO limit_orders VALUES (random() * 10 + 70000, 'GOOG', 5634, now(), 'buy', 0.50),
                                (random() * 10 + 80000, 'GOOG', 5634, now(), 'buy', 2.50),
                                (random() * 10 + 80090, 'GOOG', 5634, now(), 'buy', 1.50);

SELECT COUNT(*) FROM limit_orders WHERE id BETWEEN 70000 AND 90000;

-- commands containing a CTE are supported
WITH deleted_orders AS (DELETE FROM limit_orders WHERE id < 0 RETURNING *)
INSERT INTO limit_orders SELECT * FROM deleted_orders;

-- test simple DELETE
INSERT INTO limit_orders VALUES (246, 'TSLA', 162, '2007-07-02 16:32:15', 'sell', 20.69);
SELECT COUNT(*) FROM limit_orders WHERE id = 246;

DELETE FROM limit_orders WHERE id = 246;
SELECT COUNT(*) FROM limit_orders WHERE id = 246;

-- test simple DELETE with RETURNING
DELETE FROM limit_orders WHERE id = 430 RETURNING *;
SELECT COUNT(*) FROM limit_orders WHERE id = 430;

-- DELETE with expression in WHERE clause
INSERT INTO limit_orders VALUES (246, 'TSLA', 162, '2007-07-02 16:32:15', 'sell', 20.69);
SELECT COUNT(*) FROM limit_orders WHERE id = 246;

DELETE FROM limit_orders WHERE id = (2 * 123);
SELECT COUNT(*) FROM limit_orders WHERE id = 246;

-- commands with a USING clause are supported
CREATE TABLE bidders ( name text, id bigint );
DELETE FROM limit_orders USING bidders WHERE limit_orders.id = 246 AND
											 limit_orders.bidder_id = bidders.id AND
											 bidders.name = 'Bernie Madoff';

-- commands containing a CTE are supported
WITH new_orders AS (INSERT INTO limit_orders VALUES (411, 'FLO', 12, '2017-07-02 16:32:15', 'buy', 66))
DELETE FROM limit_orders WHERE id < 0;

-- we have to be careful that modifying CTEs are part of the transaction and can thus roll back
\set VERBOSITY terse
WITH new_orders AS (INSERT INTO limit_orders VALUES (412, 'FLO', 12, '2017-07-02 16:32:15', 'buy', 66))
DELETE FROM limit_orders RETURNING id / 0;
\set VERBOSITY default
SELECT * FROM limit_orders WHERE id = 412;

INSERT INTO limit_orders VALUES (246, 'TSLA', 162, '2007-07-02 16:32:15', 'sell', 20.69);

-- simple UPDATE
UPDATE limit_orders SET symbol = 'GM' WHERE id = 246;
SELECT symbol FROM limit_orders WHERE id = 246;

-- simple UPDATE with RETURNING
UPDATE limit_orders SET symbol = 'GM' WHERE id = 246 RETURNING *;

-- expression UPDATE
UPDATE limit_orders SET bidder_id = 6 * 3 WHERE id = 246;
SELECT bidder_id FROM limit_orders WHERE id = 246;

-- expression UPDATE with RETURNING
UPDATE limit_orders SET bidder_id = 6 * 5 WHERE id = 246 RETURNING *;

-- multi-column UPDATE
UPDATE limit_orders SET (kind, limit_price) = ('buy', DEFAULT) WHERE id = 246;
SELECT kind, limit_price FROM limit_orders WHERE id = 246;

-- multi-column UPDATE with RETURNING
UPDATE limit_orders SET (kind, limit_price) = ('buy', 999) WHERE id = 246 RETURNING *;

-- Test that on unique contraint violations, we fail fast
\set VERBOSITY terse
INSERT INTO limit_orders VALUES (275, 'ADR', 140, '2007-07-02 16:32:15', 'sell', 43.67);
INSERT INTO limit_orders VALUES (275, 'ADR', 140, '2007-07-02 16:32:15', 'sell', 43.67);

-- Test that shards which miss a modification are marked unhealthy

-- First: Connect to the second worker node
\c - - :public_worker_2_host :worker_2_port

-- Second: Move aside limit_orders shard on the second worker node
ALTER TABLE limit_orders_750000 RENAME TO renamed_orders;

-- Third: Connect back to master node
\c - - :master_host :master_port

-- Fourth: Perform an INSERT on the remaining node
-- the whole transaction should fail
\set VERBOSITY terse
INSERT INTO limit_orders VALUES (276, 'ADR', 140, '2007-07-02 16:32:15', 'sell', 43.67);

-- set the shard name back
\c - - :public_worker_2_host :worker_2_port

-- Second: Move aside limit_orders shard on the second worker node
ALTER TABLE renamed_orders RENAME TO limit_orders_750000;

-- Verify the insert failed and both placements are healthy
-- or the insert succeeded and placement marked unhealthy
\c - - :public_worker_1_host :worker_1_port
SELECT count(*) FROM limit_orders_750000 WHERE id = 276;

\c - - :public_worker_2_host :worker_2_port
SELECT count(*) FROM limit_orders_750000 WHERE id = 276;

\c - - :master_host :master_port

SELECT count(*) FROM limit_orders WHERE id = 276;

SELECT count(*)
FROM   pg_dist_shard_placement AS sp,
	   pg_dist_shard           AS s
WHERE  sp.shardid = s.shardid
AND    sp.shardstate = 3
AND    s.logicalrelid = 'limit_orders'::regclass;

-- Test that if all shards miss a modification, no state change occurs

-- First: Connect to the first worker node
\c - - :public_worker_1_host :worker_1_port

-- Second: Move aside limit_orders shard on the second worker node
ALTER TABLE limit_orders_750000 RENAME TO renamed_orders;

-- Third: Connect back to master node
\c - - :master_host :master_port

-- Fourth: Perform an INSERT on the remaining node
\set VERBOSITY terse
INSERT INTO limit_orders VALUES (276, 'ADR', 140, '2007-07-02 16:32:15', 'sell', 43.67);
\set VERBOSITY DEFAULT

-- Last: Verify worker is still healthy
SELECT count(*)
FROM   pg_dist_shard_placement AS sp,
	   pg_dist_shard           AS s
WHERE  sp.shardid = s.shardid
AND    sp.nodename = 'localhost'
AND    sp.nodeport = :worker_1_port
AND    sp.shardstate = 1
AND    s.logicalrelid = 'limit_orders'::regclass;

-- Undo our change...

-- First: Connect to the first worker node
\c - - :public_worker_1_host :worker_1_port

-- Second: Move aside limit_orders shard on the second worker node
ALTER TABLE renamed_orders RENAME TO limit_orders_750000;

-- Third: Connect back to master node
\c - - :master_host :master_port

-- attempting to change the partition key is unsupported
UPDATE limit_orders SET id = 0 WHERE id = 246;
UPDATE limit_orders SET id = 0 WHERE id = 0 OR id = 246;

-- setting the partition column value to itself is allowed
UPDATE limit_orders SET id = 246 WHERE id = 246;
UPDATE limit_orders SET id = 246 WHERE id = 246 AND symbol = 'GM';
UPDATE limit_orders SET id = limit_orders.id WHERE id = 246;

-- UPDATEs with a FROM clause are unsupported
UPDATE limit_orders SET limit_price = 0.00 FROM bidders
					WHERE limit_orders.id = 246 AND
						  limit_orders.bidder_id = bidders.id AND
						  bidders.name = 'Bernie Madoff';

-- should succeed with a CTE
WITH deleted_orders AS (INSERT INTO limit_orders VALUES (399, 'PDR', 14, '2017-07-02 16:32:15', 'sell', 43))
UPDATE limit_orders SET symbol = 'GM';

SELECT symbol, bidder_id FROM limit_orders WHERE id = 246;

-- updates referencing just a var are supported
UPDATE limit_orders SET bidder_id = id WHERE id = 246;

-- updates referencing a column are supported
UPDATE limit_orders SET bidder_id = bidder_id + 1 WHERE id = 246;

-- IMMUTABLE functions are allowed
UPDATE limit_orders SET symbol = LOWER(symbol) WHERE id = 246;

SELECT symbol, bidder_id FROM limit_orders WHERE id = 246;

-- IMMUTABLE functions are allowed -- even in returning
UPDATE limit_orders SET symbol = UPPER(symbol) WHERE id = 246 RETURNING id, LOWER(symbol), symbol;

ALTER TABLE limit_orders ADD COLUMN array_of_values integer[];

-- updates referencing STABLE functions are allowed
UPDATE limit_orders SET placed_at = LEAST(placed_at, now()::timestamp) WHERE id = 246;
-- so are binary operators
UPDATE limit_orders SET array_of_values = 1 || array_of_values WHERE id = 246;

CREATE FUNCTION immutable_append(old_values int[], new_value int)
RETURNS int[] AS $$ SELECT old_values || new_value $$ LANGUAGE SQL IMMUTABLE;

\c - - :public_worker_1_host :worker_1_port
CREATE FUNCTION immutable_append(old_values int[], new_value int)
RETURNS int[] AS $$ SELECT old_values || new_value $$ LANGUAGE SQL IMMUTABLE;

\c - - :public_worker_2_host :worker_2_port
CREATE FUNCTION immutable_append(old_values int[], new_value int)
RETURNS int[] AS $$ SELECT old_values || new_value $$ LANGUAGE SQL IMMUTABLE;

\c - - :master_host :master_port

-- immutable function calls with vars are also allowed
UPDATE limit_orders
SET array_of_values = immutable_append(array_of_values, 2) WHERE id = 246;

CREATE FUNCTION stable_append(old_values int[], new_value int)
RETURNS int[] AS $$ BEGIN RETURN old_values || new_value; END; $$
LANGUAGE plpgsql STABLE;

-- but STABLE function calls with vars are not allowed
UPDATE limit_orders
SET array_of_values = stable_append(array_of_values, 3) WHERE id = 246;

SELECT array_of_values FROM limit_orders WHERE id = 246;

-- STRICT functions work as expected
CREATE FUNCTION temp_strict_func(integer,integer) RETURNS integer AS
'SELECT COALESCE($1, 2) + COALESCE($1, 3);' LANGUAGE SQL STABLE STRICT;

\set VERBOSITY terse
UPDATE limit_orders SET bidder_id = temp_strict_func(1, null) WHERE id = 246;
\set VERBOSITY default

SELECT array_of_values FROM limit_orders WHERE id = 246;

ALTER TABLE limit_orders DROP array_of_values;

-- even in RETURNING
UPDATE limit_orders SET placed_at = placed_at WHERE id = 246 RETURNING NOW();

-- check that multi-row UPDATE/DELETEs with RETURNING work
INSERT INTO multiple_hash VALUES ('0', '1');
INSERT INTO multiple_hash VALUES ('0', '2');
INSERT INTO multiple_hash VALUES ('0', '3');
INSERT INTO multiple_hash VALUES ('0', '4');
INSERT INTO multiple_hash VALUES ('0', '5');
INSERT INTO multiple_hash VALUES ('0', '6');

UPDATE multiple_hash SET data = data ||'-1' WHERE category = '0' RETURNING *;
DELETE FROM multiple_hash WHERE category = '0' RETURNING *;

-- ensure returned row counters are correct
\set QUIET off
INSERT INTO multiple_hash VALUES ('1', '1');
INSERT INTO multiple_hash VALUES ('1', '2');
INSERT INTO multiple_hash VALUES ('1', '3');
INSERT INTO multiple_hash VALUES ('2', '1');
INSERT INTO multiple_hash VALUES ('2', '2');
INSERT INTO multiple_hash VALUES ('2', '3');
INSERT INTO multiple_hash VALUES ('2', '3') RETURNING *;

-- check that update return the right number of rows
-- one row
UPDATE multiple_hash SET data = data ||'-1' WHERE category = '1' AND data = '1';
-- three rows
UPDATE multiple_hash SET data = data ||'-2' WHERE category = '1';
-- three rows, with RETURNING
UPDATE multiple_hash SET data = data ||'-2' WHERE category = '1' RETURNING category;
-- check
SELECT * FROM multiple_hash WHERE category = '1' ORDER BY category, data;

-- check that deletes return the right number of rows
-- one row
DELETE FROM multiple_hash WHERE category = '2' AND data = '1';
-- two rows
DELETE FROM multiple_hash WHERE category = '2';
-- three rows, with RETURNING
DELETE FROM multiple_hash WHERE category = '1' RETURNING category;
-- check
SELECT * FROM multiple_hash WHERE category = '1' ORDER BY category, data;
SELECT * FROM multiple_hash WHERE category = '2' ORDER BY category, data;

-- verify interaction of default values, SERIAL, and RETURNING
\set QUIET on
CREATE TABLE app_analytics_events (id serial, app_id integer, name text);
SET citus.shard_count TO 4;
SELECT create_distributed_table('app_analytics_events', 'app_id', 'hash');

INSERT INTO app_analytics_events VALUES (DEFAULT, 101, 'Fauxkemon Geaux') RETURNING id;
INSERT INTO app_analytics_events (app_id, name) VALUES (102, 'Wayz') RETURNING id;
INSERT INTO app_analytics_events (app_id, name) VALUES (103, 'Mynt') RETURNING *;

DROP TABLE app_analytics_events;

-- again with serial in the partition column
CREATE TABLE app_analytics_events (id serial, app_id integer, name text);
SELECT create_distributed_table('app_analytics_events', 'id');

INSERT INTO app_analytics_events VALUES (DEFAULT, 101, 'Fauxkemon Geaux') RETURNING id;
INSERT INTO app_analytics_events (app_id, name) VALUES (102, 'Wayz') RETURNING id;
INSERT INTO app_analytics_events (app_id, name) VALUES (103, 'Mynt') RETURNING *;

-- Test multi-row insert with serial in the partition column
INSERT INTO app_analytics_events (app_id, name)
VALUES (104, 'Wayz'), (105, 'Mynt') RETURNING *;

INSERT INTO app_analytics_events (id, name)
VALUES (DEFAULT, 'Foo'), (300, 'Wah') RETURNING *;

PREPARE prep(varchar) AS
INSERT INTO app_analytics_events (id, name)
VALUES (DEFAULT, $1 || '.1'), (400 , $1 || '.2') RETURNING *;

EXECUTE prep('version-1');
EXECUTE prep('version-2');
EXECUTE prep('version-3');
EXECUTE prep('version-4');
EXECUTE prep('version-5');
EXECUTE prep('version-6');

SELECT * FROM app_analytics_events ORDER BY id, name;
TRUNCATE app_analytics_events;

-- Test multi-row insert with a dropped column
ALTER TABLE app_analytics_events DROP COLUMN app_id;
INSERT INTO app_analytics_events (name)
VALUES ('Wayz'), ('Mynt') RETURNING *;

SELECT * FROM app_analytics_events ORDER BY id;
DROP TABLE app_analytics_events;

-- Test multi-row insert with a dropped column before the partition column
CREATE TABLE app_analytics_events (id int default 3, app_id integer, name text);
SELECT create_distributed_table('app_analytics_events', 'name', colocate_with => 'none');

ALTER TABLE app_analytics_events DROP COLUMN app_id;

INSERT INTO app_analytics_events (name)
VALUES ('Wayz'), ('Mynt') RETURNING *;

SELECT * FROM app_analytics_events WHERE name = 'Wayz';
DROP TABLE app_analytics_events;

-- Test multi-row insert with serial in a reference table
CREATE TABLE app_analytics_events (id serial, app_id integer, name text);
SELECT create_reference_table('app_analytics_events');

INSERT INTO app_analytics_events (app_id, name)
VALUES (104, 'Wayz'), (105, 'Mynt') RETURNING *;

SELECT * FROM app_analytics_events ORDER BY id;
DROP TABLE app_analytics_events;

-- Test multi-row insert with serial in a non-partition column
CREATE TABLE app_analytics_events (id int, app_id serial, name text);
SELECT create_distributed_table('app_analytics_events', 'id');

INSERT INTO app_analytics_events (id, name)
VALUES (99, 'Wayz'), (98, 'Mynt') RETURNING name, app_id;

SELECT * FROM app_analytics_events ORDER BY id;
DROP TABLE app_analytics_events;

-- test UPDATE with subqueries
CREATE TABLE raw_table (id bigint, value bigint);
CREATE TABLE summary_table (
	id bigint,
	min_value numeric,
	average_value numeric,
	count int,
	uniques int);

SELECT create_distributed_table('raw_table', 'id');
SELECT create_distributed_table('summary_table', 'id');

INSERT INTO raw_table VALUES (1, 100);
INSERT INTO raw_table VALUES (1, 200);
INSERT INTO raw_table VALUES (1, 200);
INSERT INTO raw_table VALUES (1, 300);
INSERT INTO raw_table VALUES (2, 400);
INSERT INTO raw_table VALUES (2, 500);

INSERT INTO summary_table VALUES (1);
INSERT INTO summary_table VALUES (2);

-- test noop deletes and updates
DELETE FROM summary_table WHERE false;
DELETE FROM summary_table WHERE null;
DELETE FROM summary_table WHERE null > jsonb_build_array();

UPDATE summary_table SET uniques = 0 WHERE false;
UPDATE summary_table SET uniques = 0 WHERE null;
UPDATE summary_table SET uniques = 0 WHERE null > jsonb_build_array();

SELECT * FROM summary_table ORDER BY id;

UPDATE summary_table SET average_value = average_query.average FROM (
	SELECT avg(value) AS average FROM raw_table WHERE id = 1
	) average_query
WHERE id = 1;

SELECT * FROM summary_table ORDER BY id;

-- try different syntax
UPDATE summary_table SET (min_value, average_value) =
	(SELECT min(value), avg(value) FROM raw_table WHERE id = 2)
WHERE id = 2;

SELECT * FROM summary_table ORDER BY id;

UPDATE summary_table SET min_value = 100
	WHERE id IN (SELECT id FROM raw_table WHERE id = 1 and value > 100) AND id = 1;

SELECT * FROM summary_table ORDER BY id;

-- indeed, we don't need filter on UPDATE explicitly if SELECT already prunes to one shard
UPDATE summary_table SET uniques = 2
	WHERE id IN (SELECT id FROM raw_table WHERE id = 1 and value IN (100, 200));

SELECT * FROM summary_table ORDER BY id;

-- use inner results for non-partition column
UPDATE summary_table SET uniques = NULL
	WHERE min_value IN (SELECT value FROM raw_table WHERE id = 1) AND id = 1;

SELECT * FROM summary_table ORDER BY id;

-- these should not update anything
UPDATE summary_table SET average_value = average_query.average FROM (
	SELECT avg(value) AS average FROM raw_table WHERE id = 1 AND id = 4
	) average_query
WHERE id = 1 AND id = 4;

UPDATE summary_table SET average_value = average_query.average FROM (
	SELECT avg(value) AS average FROM raw_table WHERE id = 1
	) average_query
WHERE id = 1 AND id = 4;

SELECT * FROM summary_table ORDER BY id;

-- update with NULL value
UPDATE summary_table SET average_value = average_query.average FROM (
	SELECT avg(value) AS average FROM raw_table WHERE id = 1 AND id = 4
	) average_query
WHERE id = 1;

SELECT * FROM summary_table ORDER BY id;

-- multi-shard updates with recursively planned subqueries
BEGIN;
UPDATE summary_table SET average_value = average_query.average FROM (
	SELECT avg(value) AS average FROM raw_table) average_query;
ROLLBACK;

BEGIN;
UPDATE summary_table SET average_value = average_value + 1 WHERE id =
  (SELECT id FROM raw_table WHERE value > 100 LIMIT 1);
ROLLBACK;

-- test complex queries
UPDATE summary_table
SET
       uniques = metrics.expensive_uniques,
       count = metrics.total_count
FROM
		(SELECT
			id,
			count(DISTINCT (CASE WHEN value > 100 then value end)) AS expensive_uniques,
			count(value) AS total_count
        FROM raw_table
        WHERE id = 1
        GROUP BY id) metrics
WHERE
   summary_table.id = metrics.id AND
   summary_table.id = 1;

SELECT * FROM summary_table ORDER BY id;

-- test joins
UPDATE summary_table SET count = count + 1 FROM raw_table
	WHERE raw_table.id = summary_table.id AND summary_table.id = 1;

SELECT * FROM summary_table ORDER BY id;

-- test with prepared statements
PREPARE prepared_update_with_subquery(int, int) AS
	UPDATE summary_table SET count = count + $1 FROM raw_table
		WHERE raw_table.id = summary_table.id AND summary_table.id = $2;

-- execute 6 times to trigger prepared statement usage
EXECUTE prepared_update_with_subquery(10, 1);
EXECUTE prepared_update_with_subquery(10, 1);
EXECUTE prepared_update_with_subquery(10, 1);
EXECUTE prepared_update_with_subquery(10, 1);
EXECUTE prepared_update_with_subquery(10, 1);
EXECUTE prepared_update_with_subquery(10, 1);

SELECT * FROM summary_table ORDER BY id;

-- test with reference tables

CREATE TABLE reference_raw_table (id bigint, value bigint);
CREATE TABLE reference_summary_table (
	id bigint,
	min_value numeric,
	average_value numeric,
	count int,
	uniques int);

SELECT create_reference_table('reference_raw_table');
SELECT create_reference_table('reference_summary_table');

INSERT INTO reference_raw_table VALUES (1, 100);
INSERT INTO reference_raw_table VALUES (1, 200);
INSERT INTO reference_raw_table VALUES (1, 200);
INSERT INTO reference_raw_table VALUES (1,300), (2, 400), (2,500) RETURNING *;

INSERT INTO reference_summary_table VALUES (1);
INSERT INTO reference_summary_table VALUES (2);

SELECT * FROM reference_summary_table ORDER BY id;

UPDATE reference_summary_table SET average_value = average_query.average FROM (
	SELECT avg(value) AS average FROM reference_raw_table WHERE id = 1
	) average_query
WHERE id = 1;

UPDATE reference_summary_table SET average_value = average_query.average_value FROM (
	SELECT average_value FROM summary_table WHERE id = 1 FOR UPDATE
	) average_query
WHERE id = 1;

UPDATE reference_summary_table SET (min_value, average_value) =
	(SELECT min(value), avg(value) FROM reference_raw_table WHERE id = 2)
WHERE id = 2;

SELECT * FROM reference_summary_table ORDER BY id;

-- no need partition colum equalities on reference tables
UPDATE reference_summary_table SET (count) =
	(SELECT id AS inner_id FROM reference_raw_table WHERE value = 500)
WHERE min_value = 400;

SELECT * FROM reference_summary_table ORDER BY id;

-- can read from a reference table and update a distributed table
UPDATE summary_table SET average_value = average_query.average FROM (
	SELECT avg(value) AS average FROM reference_raw_table WHERE id = 1
	) average_query
WHERE id = 1;

-- cannot read from a distributed table and update a reference table
UPDATE reference_summary_table SET average_value = average_query.average FROM (
	SELECT avg(value) AS average FROM raw_table WHERE id = 1
	) average_query
WHERE id = 1;

UPDATE reference_summary_table SET average_value = average_query.average FROM (
	SELECT avg(value) AS average FROM raw_table WHERE id = 1 AND  id = 2
	) average_query
WHERE id = 1;

-- test connection API via using COPY

-- COPY on SELECT part
BEGIN;

\COPY raw_table FROM STDIN WITH CSV
3, 100
3, 200
\.

INSERT INTO summary_table VALUES (3);

UPDATE summary_table SET average_value = average_query.average FROM (
	SELECT avg(value) AS average FROM raw_table WHERE id = 3
	) average_query
WHERE id = 3;

COMMIT;

SELECT * FROM summary_table ORDER BY id;

-- COPY on UPDATE part
BEGIN;

INSERT INTO raw_table VALUES (4, 100);
INSERT INTO raw_table VALUES (4, 200);

\COPY summary_table FROM STDIN WITH CSV
4,,,,
\.

UPDATE summary_table SET average_value = average_query.average FROM (
	SELECT avg(value) AS average FROM raw_table WHERE id = 4
	) average_query
WHERE id = 4;

COMMIT;

SELECT * FROM summary_table ORDER BY id;

-- COPY on both part
BEGIN;

\COPY raw_table FROM STDIN WITH CSV
5, 100
5, 200
\.

\COPY summary_table FROM STDIN WITH CSV
5,,,,
\.

UPDATE summary_table SET average_value = average_query.average FROM (
	SELECT avg(value) AS average FROM raw_table WHERE id = 5
	) average_query
WHERE id = 5;

COMMIT;

SELECT * FROM summary_table ORDER BY id;

-- COPY on reference tables
BEGIN;

\COPY reference_raw_table FROM STDIN WITH CSV
6, 100
6, 200
\.

\COPY summary_table FROM STDIN WITH CSV
6,,,,
\.

UPDATE summary_table SET average_value = average_query.average FROM (
	SELECT avg(value) AS average FROM reference_raw_table WHERE id = 6
	) average_query
WHERE id = 6;

COMMIT;

SELECT * FROM summary_table ORDER BY id;

-- test DELETE queries
SELECT * FROM raw_table ORDER BY id, value;

DELETE FROM summary_table
  WHERE min_value IN (SELECT value FROM raw_table WHERE id = 1) AND id = 1;

SELECT * FROM summary_table ORDER BY id;

-- test with different syntax
DELETE FROM summary_table USING raw_table
  WHERE summary_table.id = raw_table.id AND raw_table.id = 2;

SELECT * FROM summary_table ORDER BY id;

-- cannot read from a distributed table and delete from a reference table
DELETE FROM reference_summary_table USING raw_table
  WHERE reference_summary_table.id = raw_table.id AND raw_table.id = 3;

SELECT * FROM summary_table ORDER BY id;

-- test connection API via using COPY with DELETEs
BEGIN;

\COPY summary_table FROM STDIN WITH CSV
1,,,,
2,,,,
\.

DELETE FROM summary_table USING raw_table
  WHERE summary_table.id = raw_table.id AND raw_table.id = 1;

DELETE FROM summary_table USING reference_raw_table
  WHERE summary_table.id = reference_raw_table.id AND reference_raw_table.id = 2;

COMMIT;

SELECT * FROM summary_table ORDER BY id;

-- test DELETEs with prepared statements
PREPARE prepared_delete_with_join(int) AS
	DELETE FROM summary_table USING raw_table
  		WHERE summary_table.id = raw_table.id AND raw_table.id = $1;

INSERT INTO raw_table VALUES (6, 100);

-- execute 6 times to trigger prepared statement usage
EXECUTE prepared_delete_with_join(1);
EXECUTE prepared_delete_with_join(2);
EXECUTE prepared_delete_with_join(3);
EXECUTE prepared_delete_with_join(4);
EXECUTE prepared_delete_with_join(5);
EXECUTE prepared_delete_with_join(6);

SELECT * FROM summary_table ORDER BY id;

-- we don't support subqueries in VALUES clause
INSERT INTO summary_table (id) VALUES ((SELECT id FROM summary_table));
INSERT INTO summary_table (id) VALUES (5), ((SELECT id FROM summary_table));

-- similar queries with reference tables
INSERT INTO reference_summary_table (id) VALUES ((SELECT id FROM summary_table));
INSERT INTO summary_table (id) VALUES ((SELECT id FROM reference_summary_table));

-- subqueries that would be eliminated by = null clauses
DELETE FROM summary_table WHERE (
    SELECT 1 FROM pg_catalog.pg_statio_sys_sequences
) = null;
DELETE FROM summary_table WHERE (
    SELECT (select action_statement from information_schema.triggers)
    FROM pg_catalog.pg_statio_sys_sequences
) = null;

DELETE FROM summary_table WHERE id < (
    SELECT 0 FROM pg_dist_node
);

CREATE TABLE multi_modifications.local (a int default 1, b int);
INSERT INTO multi_modifications.local VALUES (default, (SELECT min(id) FROM summary_table));

DROP TABLE raw_table;
DROP TABLE summary_table;
DROP TABLE reference_raw_table;
DROP TABLE reference_summary_table;
DROP SCHEMA multi_modifications CASCADE;
