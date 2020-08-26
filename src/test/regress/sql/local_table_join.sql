CREATE SCHEMA local_table_join;
SET search_path TO local_table_join;


CREATE TABLE postgres_table (key int, value text, value_2 jsonb);
CREATE TABLE reference_table (key int, value text, value_2 jsonb);
SELECT create_reference_table('reference_table');
CREATE TABLE distributed_table (key int, value text, value_2 jsonb);
SELECT create_distributed_table('distributed_table', 'key');

SET client_min_messages TO DEBUG1;


-- the user doesn't allow local / distributed table joinn
SET citus.local_table_join_policy TO 'never';
SELECT count(*) FROM postgres_table JOIN distributed_table USING(key);
SELECT count(*) FROM postgres_table JOIN reference_table USING(key);

-- the user prefers local table recursively planned
SET citus.local_table_join_policy TO 'pull-local';
SELECT count(*) FROM postgres_table JOIN distributed_table USING(key);
SELECT count(*) FROM postgres_table JOIN reference_table USING(key);


-- the user prefers distributed table recursively planned
SET citus.local_table_join_policy TO 'pull-distributed';
SELECT count(*) FROM postgres_table JOIN distributed_table USING(key);
SELECT count(*) FROM postgres_table JOIN reference_table USING(key);


-- update/delete
-- auto tests

-- switch back to the default policy, which is auto
RESET citus.local_table_join_policy;

-- on the default mode, the local tables should be recursively planned
SELECT count(*) FROM distributed_table JOIN postgres_table USING(key);
SELECT count(*) FROM reference_table JOIN postgres_table USING(key);
SELECT count(*) FROM distributed_table JOIN postgres_table USING(key) JOIN reference_table USING (key);



-- this is a contreversial part that we should discuss further
-- if the distributed table has at least one filter, we prefer
-- recursively planning of the distributed table
SELECT count(*) FROM distributed_table JOIN postgres_table USING(key) WHERE distributed_table.value = 'test';

-- but if the filters can be pushed downn to the local table via  the join
-- we are smart about recursively planning the local table
SELECT count(*) FROM distributed_table JOIN postgres_table USING(key) WHERE distributed_table.key = 1;


-- if both local and distributed tables have a filter, we prefer local
SELECT count(*) FROM distributed_table JOIN postgres_table USING(key) WHERE distributed_table.value = 'test' AND postgres_table.value = 'test';
SELECT count(*) FROM distributed_table JOIN postgres_table USING(key) WHERE distributed_table.value = 'test' OR postgres_table.value = 'test';


-- multiple local/distributed tables
-- only local tables are recursively planned
SELECT count(*) FROM distributed_table d1 JOIN postgres_table p1 USING(key) JOIN distributed_table d2 USING(key) JOIN postgres_table p2 USING(key);


-- if one of the distributed tables have a filter, we'll prefer recursive planning of it as well
-- it actually leads to a poor plan as we need to recursively plan local tables anyway as it is
-- joined with another distributed table
SELECT
	count(*)
FROM
	distributed_table d1 JOIN postgres_table p1 USING(key) JOIN distributed_table d2 USING(key) JOIN postgres_table p2 USING(key)
WHERE
	d1.value = '1';

-- if the filter is on the JOIN key, we can recursively plan the local
-- tables as filters are pushded down to the local tables
SELECT
	count(*)
FROM
	distributed_table d1 JOIN postgres_table p1 USING(key) JOIN distributed_table d2 USING(key) JOIN postgres_table p2 USING(key)
WHERE
	d1.key = 1;


-- we can support modification queries as well
UPDATE
	postgres_table
SET
	value = 'test'
FROM
	distributed_table
WHERE
	distributed_table.key = postgres_table.key;


UPDATE
	distributed_table
SET
	value = 'test'
FROM
	postgres_table
WHERE
	distributed_table.key = postgres_table.key;

-- modifications with multiple tables
UPDATE
	distributed_table
SET
	value = 'test'
FROM
	postgres_table p1, postgres_table p2
WHERE
	distributed_table.key = p1.key AND p1.key = p2.key;


UPDATE
	distributed_table
SET
	value = 'test'
FROM
	postgres_table p1, distributed_table d2
WHERE
	distributed_table.key = p1.key AND p1.key = d2.key;

-- pretty inefficient plan as it requires
-- recursive planninng of 2 distributed tables
UPDATE
	postgres_table
SET
	value = 'test'
FROM
	distributed_table d1, distributed_table d2
WHERE
	postgres_table.key = d1.key AND d1.key = d2.key;


\set VERBOSITY terse
RESET client_min_messages;
DROP SCHEMA local_table_join CASCADE;
