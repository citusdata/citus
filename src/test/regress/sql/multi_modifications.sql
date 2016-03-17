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

CREATE TABLE insufficient_shards ( LIKE limit_orders );
CREATE TABLE range_partitioned ( LIKE limit_orders );
CREATE TABLE append_partitioned ( LIKE limit_orders );

SELECT master_create_distributed_table('limit_orders', 'id', 'hash');
SELECT master_create_distributed_table('insufficient_shards', 'id', 'hash');
SELECT master_create_distributed_table('range_partitioned', 'id', 'range');
SELECT master_create_distributed_table('append_partitioned', 'id', 'append');

SELECT master_create_worker_shards('limit_orders', 2, 2);

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
SET citus.task_executor_type TO 'router';
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

-- commands with a RETURNING clause are unsupported
INSERT INTO limit_orders VALUES (7285, 'AMZN', 3278, '2016-01-05 02:07:36', 'sell', 0.00)
						 RETURNING *;

-- commands containing a CTE are unsupported
WITH deleted_orders AS (DELETE FROM limit_orders RETURNING *)
INSERT INTO limit_orders DEFAULT VALUES;

-- test simple DELETE
INSERT INTO limit_orders VALUES (246, 'TSLA', 162, '2007-07-02 16:32:15', 'sell', 20.69);
SELECT COUNT(*) FROM limit_orders WHERE id = 246;

DELETE FROM limit_orders WHERE id = 246;
SELECT COUNT(*) FROM limit_orders WHERE id = 246;

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

-- commands with a RETURNING clause are unsupported
DELETE FROM limit_orders WHERE id = 246 RETURNING *;

-- commands containing a CTE are unsupported
WITH deleted_orders AS (INSERT INTO limit_orders DEFAULT VALUES RETURNING *)
DELETE FROM limit_orders;

-- cursors are not supported
DELETE FROM limit_orders WHERE CURRENT OF cursor_name;

INSERT INTO limit_orders VALUES (246, 'TSLA', 162, '2007-07-02 16:32:15', 'sell', 20.69);

-- simple UPDATE
UPDATE limit_orders SET symbol = 'GM' WHERE id = 246;
SELECT symbol FROM limit_orders WHERE id = 246;

-- expression UPDATE
UPDATE limit_orders SET bidder_id = 6 * 3 WHERE id = 246;
SELECT bidder_id FROM limit_orders WHERE id = 246;

-- multi-column UPDATE
UPDATE limit_orders SET (kind, limit_price) = ('buy', DEFAULT) WHERE id = 246;
SELECT kind, limit_price FROM limit_orders WHERE id = 246;

-- Test that shards which miss a modification are marked unhealthy

-- First: Mark all placements for a node as inactive
UPDATE pg_dist_shard_placement
SET    shardstate = 3
WHERE  nodename = 'localhost' AND
	   nodeport = :worker_1_port;

-- Second: Perform an INSERT to the remaining node
INSERT INTO limit_orders VALUES (275, 'ADR', 140, '2007-07-02 16:32:15', 'sell', 43.67);

-- Third: Mark the original placements as healthy again
UPDATE pg_dist_shard_placement
SET    shardstate = 1
WHERE  nodename = 'localhost' AND
	   nodeport = :worker_1_port;

-- Fourth: Perform the same INSERT (primary key violation)
INSERT INTO limit_orders VALUES (275, 'ADR', 140, '2007-07-02 16:32:15', 'sell', 43.67);

-- Last: Verify the insert worked but the placement with the PK violation is now unhealthy
SELECT count(*) FROM limit_orders WHERE id = 275;
SELECT count(*)
FROM   pg_dist_shard_placement AS sp,
	   pg_dist_shard           AS s
WHERE  sp.shardid = s.shardid
AND    sp.nodename = 'localhost'
AND    sp.nodeport = :worker_2_port
AND    sp.shardstate = 3
AND    s.logicalrelid = 'limit_orders'::regclass;

-- commands with no constraints on the partition key are not supported
UPDATE limit_orders SET limit_price = 0.00;

-- attempting to change the partition key is unsupported
UPDATE limit_orders SET id = 0 WHERE id = 246;

-- UPDATEs with a FROM clause are unsupported
UPDATE limit_orders SET limit_price = 0.00 FROM bidders
					WHERE limit_orders.id = 246 AND
						  limit_orders.bidder_id = bidders.id AND
						  bidders.name = 'Bernie Madoff';

-- commands with a RETURNING clause are unsupported
UPDATE limit_orders SET symbol = 'GM' WHERE id = 246 RETURNING *;

-- commands containing a CTE are unsupported
WITH deleted_orders AS (INSERT INTO limit_orders DEFAULT VALUES RETURNING *)
UPDATE limit_orders SET symbol = 'GM';

-- cursors are not supported
UPDATE limit_orders SET symbol = 'GM' WHERE CURRENT OF cursor_name;
