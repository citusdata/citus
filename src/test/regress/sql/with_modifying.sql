-- Tests for modifying CTEs and CTEs in modifications
SET citus.next_shard_id TO 1502000;

CREATE SCHEMA with_modifying;
SET search_path TO with_modifying, public;

CREATE TABLE with_modifying.local_table (id int, val int);

CREATE TABLE with_modifying.modify_table (id int, val int);
SELECT create_distributed_table('modify_table', 'id');

CREATE TABLE with_modifying.users_table (LIKE public.users_table INCLUDING ALL);
SELECT create_distributed_table('with_modifying.users_table', 'user_id');
INSERT INTO with_modifying.users_table SELECT * FROM public.users_table;

CREATE TABLE with_modifying.summary_table (id int, counter int);
SELECT create_distributed_table('summary_table', 'id');

CREATE TABLE with_modifying.anchor_table (id int);
SELECT create_reference_table('anchor_table');

-- basic insert query in CTE
WITH basic_insert AS (
	INSERT INTO users_table VALUES (1), (2), (3) RETURNING *
)
SELECT
	*
FROM
	basic_insert
ORDER BY
	user_id;

-- single-shard UPDATE in CTE
WITH basic_update AS (
	UPDATE users_table SET value_3=41 WHERE user_id=1 RETURNING *
)
SELECT
	*
FROM
	basic_update
ORDER BY
	user_id,
	time
LIMIT 10;

-- multi-shard UPDATE in CTE
WITH basic_update AS (
	UPDATE users_table SET value_3=42 WHERE value_2=1 RETURNING *
)
SELECT
	*
FROM
	basic_update
ORDER BY
	user_id,
	time
LIMIT 10;

-- single-shard DELETE in CTE
WITH basic_delete AS (
	DELETE FROM users_table WHERE user_id=6 RETURNING *
)
SELECT
	*
FROM
	basic_delete
ORDER BY
	user_id,
	time
LIMIT 10;

-- multi-shard DELETE in CTE
WITH basic_delete AS (
	DELETE FROM users_table WHERE value_3=41 RETURNING *
)
SELECT
	*
FROM
	basic_delete
ORDER BY
	user_id,
	time
LIMIT 10;

-- INSERT...SELECT query in CTE
WITH copy_table AS (
	INSERT INTO users_table SELECT * FROM users_table WHERE user_id = 0 OR user_id = 3 RETURNING *
)
SELECT
	*
FROM
	copy_table
ORDER BY
	user_id,
	time
LIMIT 10;

-- CTEs prior to INSERT...SELECT via the coordinator should work
WITH cte AS (
	SELECT user_id FROM users_table WHERE value_2 IN (1, 2)
)
INSERT INTO modify_table (SELECT * FROM cte);


WITH cte_1 AS (
	SELECT user_id, value_2 FROM users_table WHERE value_2 IN (1, 2, 3, 4)
),
cte_2 AS (
	SELECT user_id, value_2 FROM users_table WHERE value_2 IN (3, 4, 5, 6)
)
INSERT INTO modify_table (SELECT cte_1.user_id FROM cte_1 join cte_2 on cte_1.value_2=cte_2.value_2);


-- we execute the query within a function to consolidate the error messages
-- between different executors
CREATE FUNCTION raise_failed_execution_cte(query text) RETURNS void AS $$
BEGIN
	EXECUTE query;
	EXCEPTION WHEN OTHERS THEN
	IF SQLERRM LIKE '%more than one row returned by a subquery used as an expression%' THEN
		RAISE 'Task failed to execute';
	ELSIF SQLERRM LIKE '%could not receive query results%' THEN
		RAISE 'Task failed to execute';
	END IF;
END;
$$LANGUAGE plpgsql;

SET client_min_messages TO ERROR;
\set VERBOSITY terse

-- even if this is an INSERT...SELECT, the CTE is under SELECT
-- function joins in CTE results can create lateral joins that are not supported
SELECT raise_failed_execution_cte($$
	WITH cte AS (
		SELECT user_id, value_2 FROM users_table WHERE value_2 IN (1, 2)
	)
	INSERT INTO modify_table (SELECT (SELECT value_2 FROM cte GROUP BY value_2));
$$);

SET client_min_messages TO DEFAULT;
\set VERBOSITY DEFAULT

-- CTEs prior to any other modification should error out
WITH cte AS (
	SELECT value_2 FROM users_table WHERE user_id IN (1, 2, 3)
)
DELETE FROM modify_table WHERE id IN (SELECT value_2 FROM cte);


WITH cte AS (
	SELECT value_2 FROM users_table WHERE user_id IN (1, 2, 3)
)
UPDATE modify_table SET val=-1 WHERE val IN (SELECT * FROM cte);


WITH user_data AS (
	SELECT user_id, value_2 FROM users_table
)
INSERT INTO modify_table SELECT * FROM user_data;

WITH raw_data AS (
	DELETE FROM modify_table RETURNING *
)
INSERT INTO summary_table SELECT id, COUNT(*) AS counter FROM raw_data GROUP BY id;

SELECT * FROM summary_table ORDER BY id;
SELECT COUNT(*) FROM modify_table;

INSERT INTO modify_table VALUES (1,1), (2, 2), (3,3);

WITH raw_data AS (
	DELETE FROM modify_table RETURNING *
)
INSERT INTO summary_table SELECT id, COUNT(*) AS counter FROM raw_data GROUP BY id;

SELECT * FROM summary_table ORDER BY id, counter;
SELECT COUNT(*) FROM modify_table;

WITH insert_reference AS (
	INSERT INTO anchor_table VALUES (1), (2) RETURNING *
)
SELECT id FROM insert_reference ORDER BY id;

WITH anchor_data AS (
	SELECT * FROM anchor_table
),
raw_data AS (
	DELETE FROM modify_table RETURNING *
),
summary_data AS (
	DELETE FROM summary_table RETURNING *
)
INSERT INTO
	summary_table
SELECT id, SUM(counter) FROM (
	(SELECT raw_data.id, COUNT(*) AS counter FROM raw_data, anchor_data
		WHERE raw_data.id = anchor_data.id GROUP BY raw_data.id)
	UNION ALL
	(SELECT * FROM summary_data)) AS all_rows
GROUP BY
	id;

SELECT COUNT(*) FROM modify_table;
SELECT * FROM summary_table ORDER BY id, counter;

WITH added_data AS (
	INSERT INTO modify_table VALUES (1,2), (1,6), (2,4), (3,6) RETURNING *
),
raw_data AS (
	DELETE FROM modify_table WHERE id = 1 AND val = (SELECT MAX(val) FROM added_data) RETURNING *
)
INSERT INTO summary_table SELECT id, COUNT(*) AS counter FROM raw_data GROUP BY id;

SELECT COUNT(*) FROM modify_table;
SELECT * FROM summary_table ORDER BY id, counter;

-- Merge rows in the summary_table
WITH summary_data AS (
	DELETE FROM summary_table RETURNING *
)
INSERT INTO summary_table SELECT id, SUM(counter) AS counter FROM summary_data GROUP BY id;

SELECT * FROM summary_table ORDER BY id, counter;
SELECT * FROM modify_table ORDER BY id, val;
SELECT * FROM anchor_table ORDER BY id;

INSERT INTO modify_table VALUES (11, 1), (12, 2), (13, 3);

WITH select_data AS (
	SELECT * FROM modify_table
),
raw_data AS (
	DELETE FROM modify_table WHERE id >= (SELECT min(id) FROM select_data WHERE id > 10) RETURNING *
)
INSERT INTO summary_table SELECT id, COUNT(*) AS counter FROM raw_data GROUP BY id;

INSERT INTO modify_table VALUES (21, 1), (22, 2), (23, 3);

-- read ids from the same table
WITH distinct_ids AS (
	SELECT DISTINCT id FROM modify_table
),
update_data AS (
	UPDATE modify_table SET val = 100 WHERE id > 10 AND
		id IN (SELECT * FROM distinct_ids) RETURNING *
)
SELECT count(*) FROM update_data;

-- read ids from a different table
WITH distinct_ids AS (
	SELECT DISTINCT id FROM summary_table
),
update_data AS (
	UPDATE modify_table SET val = 100 WHERE id > 10 AND
		id IN (SELECT * FROM distinct_ids) RETURNING *
)
SELECT count(*) FROM update_data;

-- test update with generate series
UPDATE modify_table SET val = 200 WHERE id > 10 AND
	id IN (SELECT 2*s FROM generate_series(1,20) s);

-- test update with generate series in CTE
WITH update_data AS (
	UPDATE modify_table SET val = 300 WHERE id > 10 AND
	id IN (SELECT 3*s FROM generate_series(1,20) s) RETURNING *
)
SELECT COUNT(*) FROM update_data;

WITH delete_rows AS (
	DELETE FROM modify_table WHERE id > 10 RETURNING *
)
SELECT * FROM delete_rows ORDER BY id, val;

WITH delete_rows AS (
	DELETE FROM summary_table WHERE id > 10 RETURNING *
)
SELECT * FROM delete_rows ORDER BY id, counter;

-- Check modifiying CTEs inside a transaction
BEGIN;

WITH raw_data AS (
	DELETE FROM modify_table RETURNING *
)
INSERT INTO summary_table SELECT id, COUNT(*) AS counter FROM raw_data GROUP BY id;

WITH insert_reference AS (
	INSERT INTO anchor_table VALUES (3), (4) RETURNING *
)
SELECT id FROM insert_reference ORDER BY id;

SELECT * FROM summary_table ORDER BY id, counter;
SELECT * FROM modify_table ORDER BY id, val;
SELECT * FROM anchor_table ORDER BY id;

ROLLBACK;

SELECT * FROM summary_table ORDER BY id, counter;
SELECT * FROM modify_table ORDER BY id, val;
SELECT * FROM anchor_table ORDER BY id;

-- Test delete with subqueries
WITH deleted_rows AS (
	DELETE FROM modify_table WHERE id IN (SELECT id FROM modify_table WHERE id = 1) RETURNING *
)
SELECT * FROM deleted_rows;

WITH deleted_rows AS (
	DELETE FROM modify_table WHERE id IN (SELECT id FROM modify_table WHERE val = 4) RETURNING *
)
SELECT * FROM deleted_rows;

WITH select_rows AS (
	SELECT id FROM modify_table WHERE val = 4
),
deleted_rows AS (
	DELETE FROM modify_table WHERE id IN (SELECT id FROM select_rows) RETURNING *
)
SELECT * FROM deleted_rows;

WITH deleted_rows AS (
	DELETE FROM modify_table WHERE val IN (SELECT val FROM modify_table WHERE id = 3) RETURNING *
)
SELECT * FROM deleted_rows;

WITH select_rows AS (
	SELECT val FROM modify_table WHERE id = 3
),
deleted_rows AS (
	DELETE FROM modify_table WHERE val IN (SELECT val FROM select_rows) RETURNING *
)
SELECT * FROM deleted_rows;

WITH deleted_rows AS (
	DELETE FROM modify_table WHERE ctid IN (SELECT ctid FROM modify_table WHERE id = 1) RETURNING *
)
SELECT * FROM deleted_rows;

WITH select_rows AS (
	SELECT ctid FROM modify_table WHERE id = 1
),
deleted_rows AS (
	DELETE FROM modify_table WHERE ctid IN (SELECT ctid FROM select_rows) RETURNING *
)
SELECT * FROM deleted_rows;

WITH added_data AS (
	INSERT INTO modify_table VALUES (1,2), (1,6) RETURNING *
),
select_data AS (
	SELECT * FROM added_data WHERE id = 1
),
raw_data AS (
	DELETE FROM modify_table WHERE id = 1 AND ctid IN (SELECT ctid FROM select_data) RETURNING val
)
SELECT * FROM raw_data ORDER BY val;

-- We materialize because of https://github.com/citusdata/citus/issues/3189
WITH added_data AS MATERIALIZED (
	INSERT INTO modify_table VALUES (1, trunc(10 * random())), (1, trunc(random())) RETURNING *
),
select_data AS MATERIALIZED (
	SELECT val, now() FROM added_data WHERE id = 1
),
raw_data AS MATERIALIZED (
	DELETE FROM modify_table WHERE id = 1 AND val IN (SELECT val FROM select_data) RETURNING *
)
SELECT COUNT(*) FROM raw_data;

WITH added_data AS (
	INSERT INTO modify_table VALUES (1, trunc(10 * random())), (1, trunc(random())) RETURNING *
),
select_data AS (
	SELECT val, '2011-01-01' FROM added_data WHERE id = 1
),
raw_data AS (
	DELETE FROM modify_table WHERE id = 1 AND val IN (SELECT val FROM select_data) RETURNING *
)
SELECT COUNT(*) FROM raw_data;

INSERT INTO modify_table VALUES (1,2), (1,6), (2, 3), (3, 5);
WITH select_data AS (
	SELECT * FROM modify_table
),
raw_data AS (
	DELETE FROM modify_table WHERE id IN (SELECT id FROM select_data WHERE val > 5) RETURNING id, val
)
SELECT * FROM raw_data ORDER BY val;

WITH select_data AS (
	SELECT * FROM modify_table
),
raw_data AS (
	UPDATE modify_table SET val = 0 WHERE id IN (SELECT id FROM select_data WHERE val < 5) RETURNING id, val
)
SELECT * FROM raw_data ORDER BY val;

SELECT * FROM modify_table ORDER BY id, val;

-- Test with joins
WITH select_data AS (
	SELECT * FROM modify_table
),
raw_data AS (
	UPDATE modify_table SET val = 0 WHERE
		id IN (SELECT id FROM select_data) AND
		val IN (SELECT counter FROM summary_table)
	RETURNING id, val
)
SELECT * FROM raw_data ORDER BY val;

-- Test that local tables are can be updated
-- selecting from distributed tables
UPDATE local_table lt SET val = mt.val
FROM modify_table mt WHERE mt.id = lt.id;

-- Including inside CTEs
WITH cte AS (
	UPDATE local_table lt SET val = mt.val
	FROM modify_table mt WHERE mt.id = lt.id
	RETURNING lt.id, lt.val
) SELECT * FROM cte JOIN modify_table mt ON mt.id = cte.id ORDER BY 1,2;

-- Make sure checks for volatile functions apply to CTEs too
WITH cte AS (UPDATE modify_table SET val = random() WHERE id = 3 RETURNING *)
SELECT * FROM cte JOIN modify_table mt ON mt.id = 3 AND mt.id = cte.id ORDER BY 1,2;

-- Two queries from HammerDB:
-- 1
CREATE TABLE with_modifying.stock (s_i_id numeric(6,0) NOT NULL, s_w_id numeric(4,0) NOT NULL, s_quantity numeric(6,0), s_dist_01 character(24)) WITH (fillfactor='50');
ALTER TABLE with_modifying.stock ADD CONSTRAINT stock_i1 PRIMARY KEY (s_i_id, s_w_id);
SELECT create_distributed_table('stock', 's_w_id');
INSERT INTO with_modifying.stock VALUES
	(64833, 10, 3, 'test1'),
	(64834, 10, 3, 'test2'),
	(63867, 10, 3, 'test3');
PREPARE su_after(INT[], SMALLINT[], SMALLINT[], NUMERIC(5,2)[], NUMERIC, NUMERIC, NUMERIC) AS
	WITH stock_update AS (
		UPDATE stock
		SET s_quantity = ( CASE WHEN s_quantity < (item_stock.quantity + 10) THEN s_quantity + 91 ELSE s_quantity END) - item_stock.quantity
		FROM UNNEST($1, $2, $3, $4) AS item_stock (item_id, supply_wid, quantity, price)
		WHERE stock.s_i_id = item_stock.item_id
			AND stock.s_w_id = item_stock.supply_wid
			AND stock.s_w_id = ANY ($2)
		RETURNING stock.s_dist_01 as s_dist, stock.s_quantity, ( item_stock.quantity + item_stock.price * ( 1 + $5 + $6 ) * ( 1 - $7) ) amount
	)
	SELECT array_agg ( s_dist ), array_agg ( s_quantity ), array_agg ( amount )
	FROM stock_update;
EXECUTE su_after('{64833,63857,13941,76514,35858,10004,88553,34483,91251,28144,51687,36407,54436,72873}', '{10,10,10,10,10,10,10,10,10,10,10,10,10,10}', '{8,2,2,6,7,4,6,1,1,5,6,7,6,2}', '{26.04,4.79,67.84,77.66,47.06,23.12,32.74,56.99,84.75,37.52,73.52,98.86,49.96,29.47}', 0.1800, 0.1100, 0.5000);
EXECUTE su_after('{64833,63857,13941,76514,35858,10004,88553,34483,91251,28144,51687,36407,54436,72873}', '{10,10,10,10,10,10,10,10,10,10,10,10,10,10}', '{8,2,2,6,7,4,6,1,1,5,6,7,6,2}', '{26.04,4.79,67.84,77.66,47.06,23.12,32.74,56.99,84.75,37.52,73.52,98.86,49.96,29.47}', 0.1800, 0.1100, 0.5000);
EXECUTE su_after('{64833,63857,13941,76514,35858,10004,88553,34483,91251,28144,51687,36407,54436,72873}', '{10,10,10,10,10,10,10,10,10,10,10,10,10,10}', '{8,2,2,6,7,4,6,1,1,5,6,7,6,2}', '{26.04,4.79,67.84,77.66,47.06,23.12,32.74,56.99,84.75,37.52,73.52,98.86,49.96,29.47}', 0.1800, 0.1100, 0.5000);
EXECUTE su_after('{64833,63857,13941,76514,35858,10004,88553,34483,91251,28144,51687,36407,54436,72873}', '{10,10,10,10,10,10,10,10,10,10,10,10,10,10}', '{8,2,2,6,7,4,6,1,1,5,6,7,6,2}', '{26.04,4.79,67.84,77.66,47.06,23.12,32.74,56.99,84.75,37.52,73.52,98.86,49.96,29.47}', 0.1800, 0.1100, 0.5000);
EXECUTE su_after('{64833,63857,13941,76514,35858,10004,88553,34483,91251,28144,51687,36407,54436,72873}', '{10,10,10,10,10,10,10,10,10,10,10,10,10,10}', '{8,2,2,6,7,4,6,1,1,5,6,7,6,2}', '{26.04,4.79,67.84,77.66,47.06,23.12,32.74,56.99,84.75,37.52,73.52,98.86,49.96,29.47}', 0.1800, 0.1100, 0.5000);
EXECUTE su_after('{64833,63857,13941,76514,35858,10004,88553,34483,91251,28144,51687,36407,54436,72873}', '{10,10,10,10,10,10,10,10,10,10,10,10,10,10}', '{8,2,2,6,7,4,6,1,1,5,6,7,6,2}', '{26.04,4.79,67.84,77.66,47.06,23.12,32.74,56.99,84.75,37.52,73.52,98.86,49.96,29.47}', 0.1800, 0.1100, 0.5000);

-- 2
CREATE TABLE with_modifying.orders (o_id numeric NOT NULL, o_w_id numeric NOT NULL, o_d_id numeric NOT NULL, o_c_id numeric) WITH (fillfactor='50');
CREATE UNIQUE INDEX orders_i2 ON with_modifying.orders USING btree (o_w_id, o_d_id, o_c_id, o_id) TABLESPACE pg_default;
ALTER TABLE with_modifying.orders ADD CONSTRAINT orders_i1 PRIMARY KEY (o_w_id, o_d_id, o_id);
CREATE TABLE with_modifying.order_line (ol_w_id numeric NOT NULL, ol_d_id numeric NOT NULL, ol_o_id numeric NOT NULL, ol_number numeric NOT NULL, ol_delivery_d timestamp without time zone, ol_amount numeric) WITH (fillfactor='50');
ALTER TABLE with_modifying.order_line ADD CONSTRAINT order_line_i1 PRIMARY KEY (ol_w_id, ol_d_id, ol_o_id, ol_number);
SELECT create_distributed_table('orders', 'o_w_id');
SELECT create_distributed_table('order_line', 'ol_w_id');
INSERT INTO orders VALUES (1, 1, 1, 1), (2, 2, 2, 2), (3, 3, 3, 3);
INSERT INTO order_line VALUES (1, 1, 1, 10), (2, 2, 2, 20), (3, 3, 3, 30);
PREPARE olu(int,int[],int[]) AS
	WITH order_line_update AS (
		UPDATE order_line
		SET ol_delivery_d = current_timestamp
		FROM UNNEST($2, $3) AS ids(o_id, d_id)
		WHERE ol_o_id = ids.o_id
			AND ol_d_id = ids.d_id
			AND ol_w_id = $1
		RETURNING ol_d_id, ol_o_id, ol_amount
	)
	SELECT array_agg(ol_d_id), array_agg(c_id), array_agg(sum_amount)
	FROM (
		SELECT ol_d_id,
			(SELECT DISTINCT o_c_id FROM orders WHERE o_id = ol_o_id AND o_d_id = ol_d_id AND o_w_id = $1) AS c_id,
			sum(ol_amount) AS sum_amount
		FROM order_line_update
		GROUP BY ol_d_id, ol_o_id
	) AS inner_sum;
EXECUTE olu(1,ARRAY[1,2],ARRAY[1,2]);
EXECUTE olu(1,ARRAY[1,2],ARRAY[1,2]);
EXECUTE olu(1,ARRAY[1,2],ARRAY[1,2]);
EXECUTE olu(1,ARRAY[1,2],ARRAY[1,2]);
EXECUTE olu(1,ARRAY[1,2],ARRAY[1,2]);
EXECUTE olu(1,ARRAY[1,2],ARRAY[1,2]);

-- test insert query with insert CTE
WITH insert_cte AS
	(INSERT INTO with_modifying.modify_table VALUES (23, 7))
INSERT INTO with_modifying.anchor_table VALUES (1998);
SELECT * FROM with_modifying.modify_table WHERE id = 23 AND val = 7;
SELECT * FROM with_modifying.anchor_table WHERE id = 1998;

-- test insert query with multiple CTEs
WITH select_cte AS (SELECT * FROM with_modifying.anchor_table),
	modifying_cte AS (INSERT INTO with_modifying.anchor_table SELECT * FROM select_cte)
INSERT INTO with_modifying.anchor_table VALUES (1995);
SELECT * FROM with_modifying.anchor_table ORDER BY 1;

-- test with returning
WITH returning_cte AS (INSERT INTO with_modifying.anchor_table values (1997) RETURNING *)
INSERT INTO with_modifying.anchor_table VALUES (1996);
SELECT * FROM with_modifying.anchor_table WHERE id IN (1996, 1997) ORDER BY 1;

-- test insert query with select CTE
WITH select_cte AS
	(SELECT * FROM with_modifying.modify_table)
INSERT INTO with_modifying.anchor_table VALUES (1990);
SELECT * FROM with_modifying.anchor_table WHERE id = 1990;

-- even if we do multi-row insert, it is not fast path router due to cte
WITH select_cte AS (SELECT 1 AS col)
INSERT INTO with_modifying.anchor_table VALUES (1991), (1992);
SELECT * FROM with_modifying.anchor_table WHERE id IN (1991, 1992) ORDER BY 1;

DELETE FROM with_modifying.anchor_table WHERE id IN (1990, 1991, 1992, 1995, 1996, 1997, 1998);

-- Test with replication factor 2
SET citus.shard_replication_factor to 2;

DROP TABLE modify_table;
CREATE TABLE with_modifying.modify_table (id int, val int);
SELECT create_distributed_table('modify_table', 'id');
INSERT INTO with_modifying.modify_table SELECT user_id, value_1 FROM public.users_table;

DROP TABLE summary_table;
CREATE TABLE with_modifying.summary_table (id int, counter int);
SELECT create_distributed_table('summary_table', 'id');

SELECT COUNT(*) FROM modify_table;
SELECT * FROM summary_table ORDER BY id, counter;

WITH raw_data AS (
	DELETE FROM modify_table RETURNING *
)
INSERT INTO summary_table SELECT id, COUNT(*) AS counter FROM raw_data GROUP BY id;

SELECT COUNT(*) FROM modify_table;
SELECT * FROM summary_table ORDER BY id, counter;

-- make sure that the intermediate result uses a connection
-- that does not interfere with placement connections
BEGIN;
	INSERT INTO modify_table (id) VALUES (10000);
	WITH test_cte AS (SELECT count(*) FROM modify_table) SELECT * FROM test_cte;
ROLLBACK;

-- similarly, make sure that the intermediate result uses a seperate connection
WITH first_query AS (INSERT INTO modify_table (id) VALUES (10001)),
 	second_query AS (SELECT * FROM modify_table) SELECT count(*) FROM second_query;

SET client_min_messages TO debug2;
-- pushed down without the insert
WITH mb AS (UPDATE modify_table SET val = 3 WHERE id = 3 RETURNING NULL) INSERT INTO modify_table WITH ma AS (SELECT * FROM modify_table LIMIT 10) SELECT count(*) FROM mb;

-- not pushed down due to volatile
WITH ma AS (SELECT count(*) FROM modify_table where id = 1), mu AS (WITH allref AS (SELECT random() a FROM modify_table limit 4) UPDATE modify_table SET val = 3 WHERE id = 1 AND val IN (SELECT a FROM allref) RETURNING id+1) SELECT count(*) FROM mu, ma;
WITH mu AS (WITH allref AS (SELECT random() a FROM anchor_table) UPDATE modify_table SET val = 3 WHERE id = 1 AND val IN (SELECT a FROM allref) RETURNING id+1) SELECT count(*) FROM mu;

-- pushed down
WITH mu AS (WITH allref AS (SELECT id a FROM anchor_table) UPDATE modify_table SET val = 3 WHERE id = 1 AND val IN (SELECT a FROM allref) RETURNING id+1) SELECT count(*) FROM mu;

-- pushed down and stable function evaluated
WITH mu AS (WITH allref AS (SELECT now() a FROM anchor_table) UPDATE modify_table SET val = 3 WHERE id = 1 AND now() IN (SELECT a FROM allref) RETURNING id+1) SELECT count(*) FROM mu;
RESET client_min_messages;

-- https://github.com/citusdata/citus/issues/3975
WITH mb AS (INSERT INTO modify_table VALUES (3, 3) RETURNING NULL, NULL) SELECT * FROM modify_table WHERE id = 3;
WITH mb AS (UPDATE modify_table SET val = 3 WHERE id = 3 RETURNING NULL) SELECT * FROM modify_table WHERE id = 3;
WITH mb AS (UPDATE modify_table SET val = 3 WHERE id = 3 RETURNING NULL) SELECT * FROM modify_table, mb WHERE id = 3;
WITH mb AS (UPDATE modify_table SET val = 3 WHERE id = 3 RETURNING NULL, NULL) SELECT * FROM modify_table WHERE id = 3;
WITH mb AS (UPDATE modify_table SET val = 3 WHERE id = 3 RETURNING NULL, NULL) SELECT * FROM modify_table, mb WHERE id = 3;
WITH mb AS (UPDATE modify_table SET val = 3 WHERE id = 3 RETURNING NULL alias) SELECT * FROM modify_table WHERE id = 3;
WITH mb AS (UPDATE modify_table SET val = 3 WHERE id = 3 RETURNING NULL alias) SELECT * FROM modify_table, mb WHERE id = 3;
WITH mb AS (UPDATE modify_table SET val = 3 WHERE id = 3 RETURNING val) SELECT * FROM modify_table WHERE id = 3;
WITH mb AS (UPDATE modify_table SET val = 3 WHERE id = 3 RETURNING val) SELECT * FROM modify_table, mb WHERE id = 3;
WITH mb AS (DELETE FROM modify_table WHERE id = 3 RETURNING NULL, NULL) SELECT * FROM modify_table WHERE id = 3;

\set VERBOSITY terse
DROP SCHEMA with_modifying CASCADE;
