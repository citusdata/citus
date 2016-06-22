
ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 750000;
ALTER SEQUENCE pg_catalog.pg_dist_jobid_seq RESTART 750000;


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

SELECT master_create_distributed_table('limit_orders', 'id', 'hash');
SELECT master_create_distributed_table('multiple_hash', 'category', 'hash');
SELECT master_create_distributed_table('insufficient_shards', 'id', 'hash');
SELECT master_create_distributed_table('range_partitioned', 'id', 'range');
SELECT master_create_distributed_table('append_partitioned', 'id', 'append');

SELECT master_create_worker_shards('limit_orders', 2, 2);
SELECT master_create_worker_shards('multiple_hash', 2, 2);

-- make a single shard that covers no partition values
SELECT master_create_worker_shards('insufficient_shards', 1, 1);
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
SET citus.task_executor_type TO 'real-time';
SELECT * FROM range_partitioned WHERE id = 32743;
SELECT * FROM append_partitioned WHERE id = 414123;
SET client_min_messages TO DEFAULT;
SET citus.task_executor_type TO DEFAULT;

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
INSERT INTO limit_orders VALUES (18811, 'BUD', 14962, '2014-04-05 08:32:16', 'sell',
								 -5.00);
-- INSERT violating primary key constraint
INSERT INTO limit_orders VALUES (32743, 'LUV', 5994, '2001-04-16 03:37:28', 'buy', 0.58);

-- INSERT violating primary key constraint, with RETURNING specified.
INSERT INTO limit_orders VALUES (32743, 'LUV', 5994, '2001-04-16 03:37:28', 'buy', 0.58) RETURNING *;

-- INSERT, with RETURNING specified, failing with a non-constraint error
INSERT INTO limit_orders VALUES (34153, 'LEE', 5994, '2001-04-16 03:37:28', 'buy', 0.58) RETURNING id / 0;


SET client_min_messages TO DEFAULT;

-- commands with non-constant partition values are unsupported
INSERT INTO limit_orders VALUES (random() * 100, 'ORCL', 152, '2011-08-25 11:50:45',
								 'sell', 0.58);

-- commands with expressions that cannot be collapsed are unsupported
INSERT INTO limit_orders VALUES (2036, 'GOOG', 5634, now(), 'buy', random());

-- commands with mutable functions in their quals
DELETE FROM limit_orders WHERE id = 246 AND bidder_id = (random() * 1000);

-- commands with mutable but non-volatilte functions(ie: stable func.) in their quals
DELETE FROM limit_orders WHERE id = 246 AND placed_at = current_timestamp;

-- commands with multiple rows are unsupported
INSERT INTO limit_orders VALUES (DEFAULT), (DEFAULT);

-- INSERT ... SELECT ... FROM commands are unsupported
INSERT INTO limit_orders SELECT * FROM limit_orders;

-- commands containing a CTE are unsupported
WITH deleted_orders AS (DELETE FROM limit_orders RETURNING *)
INSERT INTO limit_orders DEFAULT VALUES;

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

-- commands with no constraints on the partition key are not supported
DELETE FROM limit_orders WHERE bidder_id = 162;

-- commands with a USING clause are unsupported
CREATE TABLE bidders ( name text, id bigint );
DELETE FROM limit_orders USING bidders WHERE limit_orders.id = 246 AND
											 limit_orders.bidder_id = bidders.id AND
											 bidders.name = 'Bernie Madoff';

-- commands containing a CTE are unsupported
WITH deleted_orders AS (INSERT INTO limit_orders DEFAULT VALUES RETURNING *)
DELETE FROM limit_orders;

-- cursors are not supported
DELETE FROM limit_orders WHERE CURRENT OF cursor_name;

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
INSERT INTO limit_orders VALUES (275, 'ADR', 140, '2007-07-02 16:32:15', 'sell', 43.67);
INSERT INTO limit_orders VALUES (275, 'ADR', 140, '2007-07-02 16:32:15', 'sell', 43.67);

-- Test that shards which miss a modification are marked unhealthy

-- First: Connect to the second worker node
\c - - - :worker_2_port

-- Second: Move aside limit_orders shard on the second worker node
ALTER TABLE limit_orders_750000 RENAME TO renamed_orders;

-- Third: Connect back to master node
\c - - - :master_port

-- Fourth: Perform an INSERT on the remaining node
INSERT INTO limit_orders VALUES (276, 'ADR', 140, '2007-07-02 16:32:15', 'sell', 43.67);

-- Last: Verify the insert worked but the deleted placement is now unhealthy
SELECT count(*) FROM limit_orders WHERE id = 276;

SELECT count(*)
FROM   pg_dist_shard_placement AS sp,
	   pg_dist_shard           AS s
WHERE  sp.shardid = s.shardid
AND    sp.nodename = 'localhost'
AND    sp.nodeport = :worker_2_port
AND    sp.shardstate = 3
AND    s.logicalrelid = 'limit_orders'::regclass;

-- Test that if all shards miss a modification, no state change occurs

-- First: Connect to the first worker node
\c - - - :worker_1_port

-- Second: Move aside limit_orders shard on the second worker node
ALTER TABLE limit_orders_750000 RENAME TO renamed_orders;

-- Third: Connect back to master node
\c - - - :master_port

-- Fourth: Perform an INSERT on the remaining node
INSERT INTO limit_orders VALUES (276, 'ADR', 140, '2007-07-02 16:32:15', 'sell', 43.67);

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
\c - - - :worker_1_port

-- Second: Move aside limit_orders shard on the second worker node
ALTER TABLE renamed_orders RENAME TO limit_orders_750000;

-- Third: Connect back to master node
\c - - - :master_port

-- commands with no constraints on the partition key are not supported
UPDATE limit_orders SET limit_price = 0.00;

-- attempting to change the partition key is unsupported
UPDATE limit_orders SET id = 0 WHERE id = 246;

-- UPDATEs with a FROM clause are unsupported
UPDATE limit_orders SET limit_price = 0.00 FROM bidders
					WHERE limit_orders.id = 246 AND
						  limit_orders.bidder_id = bidders.id AND
						  bidders.name = 'Bernie Madoff';

-- commands containing a CTE are unsupported
WITH deleted_orders AS (INSERT INTO limit_orders DEFAULT VALUES RETURNING *)
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

-- updates referencing non-IMMUTABLE functions are unsupported
UPDATE limit_orders SET placed_at = now() WHERE id = 246;

-- even in RETURNING
UPDATE limit_orders SET placed_at = placed_at WHERE id = 246 RETURNING NOW();

-- cursors are not supported
UPDATE limit_orders SET symbol = 'GM' WHERE CURRENT OF cursor_name;

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
