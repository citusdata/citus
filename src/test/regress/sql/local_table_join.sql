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
-- it should favor distributed table only if it has equality on the unique column
SELECT count(*) FROM postgres_table JOIN distributed_table_pkey ON distributed_table_pkey.key > 10;
SELECT count(*) FROM postgres_table JOIN distributed_table_pkey ON distributed_table_pkey.key < 10;
SELECT count(*) FROM postgres_table JOIN distributed_table_pkey ON distributed_table_pkey.key = 10;
SELECT count(*) FROM postgres_table JOIN distributed_table_pkey ON distributed_table_pkey.key = 10 AND distributed_table_pkey.key > 10 ;
SELECT count(*) FROM postgres_table JOIN distributed_table_pkey ON distributed_table_pkey.key = 10 AND distributed_table_pkey.key > 10 ;
SELECT count(*) FROM postgres_table JOIN distributed_table_pkey ON distributed_table_pkey.key = 10 AND distributed_table_pkey.key > 10 AND postgres_table.key = 5;

SELECT count(*) FROM postgres_table JOIN distributed_table_pkey ON distributed_table_pkey.key = 10 OR distributed_table_pkey.key > 10;
SELECT count(*) FROM postgres_table JOIN distributed_table_pkey ON distributed_table_pkey.key = 10 OR distributed_table_pkey.key = 20;
SELECT count(*) FROM postgres_table JOIN distributed_table_pkey ON distributed_table_pkey.key = 10 OR distributed_table_pkey.key = 20 OR distributed_table_pkey.key = 30;
SELECT count(*) FROM postgres_table JOIN distributed_table_pkey ON distributed_table_pkey.key = 10 OR distributed_table_pkey.key = (
	SELECT count(*) FROM distributed_table_pkey
);
SELECT count(*) FROM postgres_table JOIN distributed_table_pkey ON distributed_table_pkey.key = 10 OR (distributed_table_pkey.key = 5 and distributed_table_pkey.key > 15);
SELECT count(*) FROM postgres_table JOIN distributed_table_pkey ON distributed_table_pkey.key = 10 OR (distributed_table_pkey.key > 10 and distributed_table_pkey.key > 15);
SELECT count(*) FROM postgres_table JOIN distributed_table_pkey ON distributed_table_pkey.key = 10 OR (distributed_table_pkey.key > 10 and distributed_table_pkey.value = 'notext');
SELECT count(*) FROM postgres_table JOIN distributed_table_pkey ON distributed_table_pkey.key = 10 OR (distributed_table_pkey.key = 10 and distributed_table_pkey.value = 'notext');


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

-- currently can't plan subquery-local table join
SELECT count(*) 
FROM 
	(SELECT * FROM (SELECT * FROM distributed_table) d1) d2
JOIN postgres_table
USING(key);



---------------------------------------------------------

SET client_min_messages to ERROR;
SELECT master_add_node('localhost', :master_port, groupId => 0);


CREATE TABLE citus_local(key int, value text);
SELECT create_citus_local_table('citus_local');
SET client_min_messages TO DEBUG1;

-- same for citus local table - distributed table joins
-- a unique index on key so dist table should be recursively planned
SELECT count(*) FROM citus_local JOIN distributed_table_windex USING(key);
SELECT count(*) FROM citus_local JOIN distributed_table_windex USING(value);
SELECT count(*) FROM citus_local JOIN distributed_table_windex ON citus_local.key = distributed_table_windex.key;
SELECT count(*) FROM citus_local JOIN distributed_table_windex ON distributed_table_windex.key = 10;

-- no unique index, citus local table should be recursively planned
SELECT count(*) FROM citus_local JOIN distributed_table USING(key);
SELECT count(*) FROM citus_local JOIN distributed_table USING(value);
SELECT count(*) FROM citus_local JOIN distributed_table ON citus_local.key = distributed_table.key;
SELECT count(*) FROM citus_local JOIN distributed_table ON distributed_table.key = 10;

SELECT count(*) FROM citus_local JOIN distributed_table USING(key) JOIN postgres_table USING (key) JOIN reference_table USING(key);

-- update
UPDATE
	distributed_table_windex
SET
	value = 'test'
FROM
	citus_local
WHERE
	distributed_table_windex.key = citus_local.key;	

UPDATE
	citus_local
SET
	value = 'test'
FROM
	distributed_table_windex
WHERE
	distributed_table_windex.key = citus_local.key;		

DROP TABLE citus_local;
RESET client_min_messages;
SELECT master_remove_node('localhost', :master_port);
\set VERBOSITY terse
DROP SCHEMA local_table_join CASCADE;
