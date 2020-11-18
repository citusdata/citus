CREATE SCHEMA local_table_join;
SET search_path TO local_table_join;


CREATE TABLE postgres_table (key int, value text, value_2 jsonb);
CREATE TABLE reference_table (key int, value text, value_2 jsonb);
SELECT create_reference_table('reference_table');
CREATE TABLE distributed_table (key int, value text, value_2 jsonb);
SELECT create_distributed_table('distributed_table', 'key');

CREATE TABLE distributed_table_pkey (key int primary key, value text, value_2 jsonb);
SELECT create_distributed_table('distributed_table_pkey', 'key');

CREATE TABLE distributed_table_windex (key int, value text, value_2 jsonb);
SELECT create_distributed_table('distributed_table_windex', 'key');
CREATE UNIQUE INDEX key_index ON distributed_table_windex (key);

SET client_min_messages TO DEBUG1;


-- the user doesn't allow local / distributed table joinn
SET citus.local_table_join_policy TO 'never';
SELECT count(*) FROM postgres_table JOIN distributed_table USING(key);
SELECT count(*) FROM postgres_table JOIN reference_table USING(key);

-- the user prefers local table recursively planned
SET citus.local_table_join_policy TO 'prefer-local';
SELECT count(*) FROM postgres_table JOIN distributed_table USING(key);
SELECT count(*) FROM postgres_table JOIN reference_table USING(key);


-- the user prefers distributed table recursively planned
SET citus.local_table_join_policy TO 'prefer-distributed';
SELECT count(*) FROM postgres_table JOIN distributed_table USING(key);
SELECT count(*) FROM postgres_table JOIN reference_table USING(key);


-- update/delete
-- auto tests

-- switch back to the default policy, which is auto
SET citus.local_table_join_policy to 'auto';

-- on the auto mode, the local tables should be recursively planned 
-- unless a unique index exists in a column for distributed table
SELECT count(*) FROM distributed_table JOIN postgres_table USING(key);
SELECT count(*) FROM reference_table JOIN postgres_table USING(key);
SELECT count(*) FROM distributed_table JOIN postgres_table USING(key) JOIN reference_table USING (key);

-- a unique index on key so dist table should be recursively planned
SELECT count(*) FROM postgres_table JOIN distributed_table_pkey USING(key);
SELECT count(*) FROM postgres_table JOIN distributed_table_pkey USING(value);
SELECT count(*) FROM postgres_table JOIN distributed_table_pkey ON postgres_table.key = distributed_table_pkey.key;
SELECT count(*) FROM postgres_table JOIN distributed_table_pkey ON distributed_table_pkey.key = 10;


-- a unique index on key so dist table should be recursively planned
SELECT count(*) FROM postgres_table JOIN distributed_table_windex USING(key);
SELECT count(*) FROM postgres_table JOIN distributed_table_windex USING(value);
SELECT count(*) FROM postgres_table JOIN distributed_table_windex ON postgres_table.key = distributed_table_windex.key;
SELECT count(*) FROM postgres_table JOIN distributed_table_windex ON distributed_table_windex.key = 10;

-- no unique index on value so local table should be recursively planned.
SELECT count(*) FROM distributed_table JOIN postgres_table USING(key) WHERE distributed_table.value = 'test';

SELECT count(*) FROM distributed_table JOIN postgres_table USING(key) WHERE distributed_table.key = 1;


-- if both local and distributed tables have a filter, we prefer local unless distributed table has unique indexes on any equality filter 
SELECT count(*) FROM distributed_table JOIN postgres_table USING(key) WHERE distributed_table.value = 'test' AND postgres_table.value = 'test';
SELECT count(*) FROM distributed_table JOIN postgres_table USING(key) WHERE distributed_table.value = 'test' OR postgres_table.value = 'test';


-- multiple local/distributed tables
-- only local tables are recursively planned
SELECT count(*) FROM distributed_table d1 JOIN postgres_table p1 USING(key) JOIN distributed_table d2 USING(key) JOIN postgres_table p2 USING(key);


SELECT
	count(*)
FROM
	distributed_table d1 JOIN postgres_table p1 USING(key) JOIN distributed_table d2 USING(key) JOIN postgres_table p2 USING(key)
WHERE
	d1.value = '1';

-- if the filter is on the JOIN key, we can recursively plan the local
-- tables as filters are pushed down to the local tables
SELECT
	count(*)
FROM
	distributed_table d1 JOIN postgres_table p1 USING(key) JOIN distributed_table d2 USING(key) JOIN postgres_table p2 USING(key)
WHERE
	d1.key = 1;


SET citus.local_table_join_policy to 'auto';

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

UPDATE
	distributed_table_pkey
SET
	value = 'test'
FROM
	postgres_table
WHERE
	distributed_table_pkey.key = postgres_table.key;

UPDATE
	distributed_table_windex
SET
	value = 'test'
FROM
	postgres_table
WHERE
	distributed_table_windex.key = postgres_table.key;		

-- in case of update/delete we always recursively plan 
-- the tables other than target table no matter what the policy is

SET citus.local_table_join_policy TO 'prefer-local';

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

UPDATE
	distributed_table_pkey
SET
	value = 'test'
FROM
	postgres_table
WHERE
	distributed_table_pkey.key = postgres_table.key;

UPDATE
	distributed_table_windex
SET
	value = 'test'
FROM
	postgres_table
WHERE
	distributed_table_windex.key = postgres_table.key;	


SET citus.local_table_join_policy TO 'prefer-distributed';

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

UPDATE
	distributed_table_pkey
SET
	value = 'test'
FROM
	postgres_table
WHERE
	distributed_table_pkey.key = postgres_table.key;

UPDATE
	distributed_table_windex
SET
	value = 'test'
FROM
	postgres_table
WHERE
	distributed_table_windex.key = postgres_table.key;	

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
