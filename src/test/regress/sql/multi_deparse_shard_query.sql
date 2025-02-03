--
-- MULTI_DEPARSE_SHARD_QUERY
--

CREATE SCHEMA multi_deparse_shard_query;
SET search_path TO multi_deparse_shard_query;

SET citus.next_shard_id TO 13100000;
SET citus.shard_replication_factor TO 1;

CREATE FUNCTION deparse_shard_query_test(text)
	RETURNS VOID
	AS 'citus'
 	LANGUAGE C STRICT;

-- create the first table
CREATE TABLE raw_events_1
	(tenant_id bigint,
	 value_1 int,
	 value_2 int,
	 value_3 float,
	 value_4 bigint,
	 value_5 text,
	 value_6 int DEfAULT 10,
	 value_7 int,
	 event_at date DEfAULT now()
	 );

SELECT create_distributed_table('raw_events_1', 'tenant_id', 'hash');

-- create the first table
CREATE TABLE raw_events_2
	(tenant_id bigint,
	 value_1 int,
	 value_2 int,
	 value_3 float,
	 value_4 bigint,
	 value_5 text,
	 value_6 float DEfAULT (random()*100)::float,
	 value_7 int,
	 event_at date DEfAULT now()
	 );

SELECT create_distributed_table('raw_events_2', 'tenant_id', 'hash');

CREATE TABLE aggregated_events
	(tenant_id bigint,
	 sum_value_1 bigint,
	 average_value_2 float,
	 average_value_3 float,
	 sum_value_4 bigint,
	 sum_value_5 float,
	 average_value_6 int,
	 rollup_hour date);

SELECT create_distributed_table('aggregated_events', 'tenant_id', 'hash');


-- start with very simple examples on a single table
SELECT deparse_shard_query_test('
INSERT INTO raw_events_1
SELECT * FROM raw_events_1;
');

SELECT deparse_shard_query_test('
INSERT INTO raw_events_1(tenant_id, value_4)
SELECT
	tenant_id, value_4
FROM
	raw_events_1;
');

-- now that shuffle columns a bit on a single table
SELECT deparse_shard_query_test('
INSERT INTO raw_events_1(value_5, value_2, tenant_id, value_4)
SELECT
	value_2::text, value_5::int, tenant_id, value_4
FROM
	raw_events_1;
');

-- same test on two different tables
SELECT deparse_shard_query_test('
INSERT INTO raw_events_1(value_5, value_2, tenant_id, value_4)
SELECT
	value_2::text, value_5::int, tenant_id, value_4
FROM
	raw_events_2;
');

-- lets do some simple aggregations
SELECT deparse_shard_query_test(E'
INSERT INTO aggregated_events (tenant_id, rollup_hour, sum_value_1, average_value_3, average_value_6, sum_value_4)
SELECT
	tenant_id, date_trunc(\'hour\', event_at) , sum(value_1), avg(value_3), avg(value_6), sum(value_4)
FROM
	raw_events_1
GROUP BY
	tenant_id, date_trunc(\'hour\', event_at)
');


-- also some subqueries, JOINS with a complicated target lists
-- a simple JOIN
SELECT deparse_shard_query_test('
INSERT INTO raw_events_1 (value_3, tenant_id)
SELECT
	raw_events_2.value_3, raw_events_1.tenant_id
FROM
	raw_events_1, raw_events_2
WHERE
	raw_events_1.tenant_id = raw_events_2.tenant_id;
');

-- join with group by
SELECT deparse_shard_query_test('
INSERT INTO raw_events_1 (value_3, tenant_id)
SELECT
	max(raw_events_2.value_3), avg(raw_events_1.value_3)
FROM
	raw_events_1, raw_events_2
WHERE
	raw_events_1.tenant_id = raw_events_2.tenant_id GROUP BY raw_events_1.event_at
');

-- a more complicated JOIN
SELECT deparse_shard_query_test('
INSERT INTO aggregated_events (sum_value_4, tenant_id)
SELECT
	max(r1.value_4), r3.tenant_id
FROM
	raw_events_1 r1, raw_events_2 r2, raw_events_1 r3
WHERE
	r1.tenant_id = r2.tenant_id AND r2.tenant_id = r3.tenant_id
GROUP BY
	r1.value_1, r3.tenant_id, r2.event_at
ORDER BY
	r2.event_at DESC;
');


-- queries with CTEs are supported
SELECT deparse_shard_query_test('
WITH first_tenant AS (SELECT event_at, value_5, tenant_id FROM raw_events_1)
INSERT INTO aggregated_events (rollup_hour, sum_value_5, tenant_id)
SELECT
	event_at, sum(value_5::int), tenant_id
FROM
	raw_events_1
GROUP BY
	event_at, tenant_id;
');

SELECT deparse_shard_query_test('
WITH first_tenant AS (SELECT event_at, value_5, tenant_id FROM raw_events_1)
INSERT INTO aggregated_events (sum_value_5, tenant_id)
SELECT
	sum(value_5::int), tenant_id
FROM
	raw_events_1
GROUP BY
	event_at, tenant_id;
');

SELECT deparse_shard_query_test('
INSERT INTO aggregated_events (sum_value_1, sum_value_5, tenant_id)
WITH RECURSIVE hierarchy as (
	SELECT value_1, 1 AS LEVEL, tenant_id
		FROM raw_events_1
		WHERE tenant_id = 1
	UNION
	SELECT re.value_2, (h.level+1), re.tenant_id
		FROM hierarchy h JOIN raw_events_1 re
			ON (h.tenant_id = re.tenant_id AND
				h.value_1 = re.value_6))
SELECT * FROM hierarchy WHERE LEVEL <= 2;
');


SELECT deparse_shard_query_test('
INSERT INTO aggregated_events (sum_value_1)
SELECT
	DISTINCT value_1
FROM
	raw_events_1;
');


-- many filters suffled
SELECT deparse_shard_query_test(E'
INSERT INTO aggregated_events (sum_value_5, sum_value_1, tenant_id)
SELECT value_3, value_2, tenant_id
	FROM raw_events_1
	WHERE (value_5 like \'%s\' or value_5 like \'%a\') and (tenant_id = 1) and (value_6 < 3000 or value_3 > 8000);
');

SELECT deparse_shard_query_test(E'
INSERT INTO aggregated_events (sum_value_5, tenant_id)
SELECT rank() OVER (PARTITION BY tenant_id ORDER BY value_6), tenant_id
	FROM raw_events_1
	WHERE event_at = now();
');

SELECT deparse_shard_query_test(E'
INSERT INTO aggregated_events (sum_value_5, tenant_id, sum_value_4)
SELECT random(), int4eq(1, max(value_1))::int, value_6
	FROM raw_events_1
	WHERE event_at = now()
	GROUP BY event_at, value_7, value_6;
');

SELECT deparse_shard_query_test('
INSERT INTO aggregated_events (sum_value_1, tenant_id)
SELECT
	count(DISTINCT CASE
			WHEN
				value_1 > 100
			THEN
				tenant_id
			ELSE
				value_6
			END) as c,
		max(tenant_id)
	FROM
		raw_events_1;
');

SELECT deparse_shard_query_test('
INSERT INTO raw_events_1(value_7, value_1, tenant_id)
SELECT
	value_7, value_1, tenant_id
FROM
	(SELECT
		tenant_id, value_2 as value_7, value_1
	FROM
		raw_events_2
	) as foo
');

SELECT deparse_shard_query_test(E'
INSERT INTO aggregated_events(sum_value_1, tenant_id, sum_value_5)
SELECT
	sum(value_1), tenant_id, sum(value_5::bigint)
FROM
	(SELECT
		raw_events_1.event_at, raw_events_2.tenant_id, raw_events_2.value_5, raw_events_1.value_1
	FROM
		raw_events_2, raw_events_1
	WHERE
		raw_events_1.tenant_id = raw_events_2.tenant_id
	) as foo
GROUP BY
	tenant_id, date_trunc(\'hour\', event_at)
');


SELECT deparse_shard_query_test(E'
INSERT INTO raw_events_2(tenant_id, value_1, value_2, value_3, value_4)
SELECT
	tenant_id, value_1, value_2, value_3, value_4
FROM
	(SELECT
		value_2, value_4, tenant_id, value_1, value_3
	FROM
		raw_events_1
	) as foo
');


SELECT deparse_shard_query_test(E'
INSERT INTO raw_events_2(tenant_id, value_1, value_4, value_2, value_3)
SELECT
	*
FROM
	(SELECT
		value_2, value_4, tenant_id, value_1, value_3
	FROM
		raw_events_1
	) as foo
');


-- use a column multiple times
SELECT deparse_shard_query_test('
INSERT INTO raw_events_1(tenant_id, value_7, value_4)
SELECT
	tenant_id, value_7, value_7
FROM
	raw_events_1
ORDER BY
	value_2, value_1;
');

-- test dropped table as well
ALTER TABLE raw_events_1 DROP COLUMN value_5;

SELECT deparse_shard_query_test('
INSERT INTO raw_events_1(tenant_id, value_7, value_4)
SELECT
	tenant_id, value_7, value_4
FROM
	raw_events_1;
');

SET client_min_messages TO ERROR;
DROP SCHEMA multi_deparse_shard_query CASCADE;
