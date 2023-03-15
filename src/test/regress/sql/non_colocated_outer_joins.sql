CREATE SCHEMA non_colocated_outer_joins;
SET search_path TO non_colocated_outer_joins;

CREATE TABLE test_hash1(col1 INT, col2 INT);
SELECT create_distributed_table('test_hash1', 'col1');
INSERT INTO test_hash1 SELECT i, i FROM generate_series(1,10) i;

CREATE TABLE test_hash2(col1 INT, col2 INT);
SELECT create_distributed_table('test_hash2', 'col2');
INSERT INTO test_hash2 SELECT i, i FROM generate_series(6,15) i;

CREATE TABLE test_hash3(col1 INT, col2 INT);
SELECT create_distributed_table('test_hash3', 'col1');
INSERT INTO test_hash3 SELECT i, i FROM generate_series(11,20) i;

SET citus.enable_repartition_joins TO ON;
SET citus.log_multi_join_order TO ON;
SET client_min_messages TO LOG;


-- join order planner can handle left outer join between tables with simple join clause

-- outer table restricted on partition column whereas inner one is not restricted on the partition column
SELECT t1.*, t2.* FROM test_hash1 t1 LEFT JOIN test_hash2 t2 ON (t1.col1 = t2.col1) ORDER BY 1,2,3,4;
-- inner table restricted on partition column whereas outer one is not restricted on the partition column
SELECT t1.*, t2.* FROM test_hash1 t1 LEFT JOIN test_hash2 t2 ON (t1.col2 = t2.col2) ORDER BY 1,2,3,4;
-- both tables are not restricted on partition column
SELECT t1.*, t2.* FROM test_hash1 t1 LEFT JOIN test_hash2 t2 ON (t1.col2 = t2.col1) ORDER BY 1,2,3,4;


-- join order planner can handle right outer join between tables with simple join clause

-- outer table restricted on partition column whereas inner one is not restricted on the partition column
SELECT t1.*, t2.* FROM test_hash1 t1 RIGHT JOIN test_hash2 t2 ON (t1.col1 = t2.col1) ORDER BY 1,2,3,4;
-- inner table restricted on partition column whereas outer one is not restricted on the partition column
SELECT t1.*, t2.* FROM test_hash1 t1 RIGHT JOIN test_hash2 t2 ON (t1.col2 = t2.col2) ORDER BY 1,2,3,4;
-- both tables are not restricted on partition column
SELECT t1.*, t2.* FROM test_hash1 t1 RIGHT JOIN test_hash2 t2 ON (t1.col2 = t2.col1) ORDER BY 1,2,3,4;


-- join order planner can handle full outer join between tables with simple join clause

-- left outer table restricted on partition column whereas right outer one is not restricted on the partition column
SELECT t1.*, t2.* FROM test_hash1 t1 FULL JOIN test_hash2 t2 ON (t1.col1 = t2.col1) ORDER BY 1,2,3,4;
-- right outer table restricted on partition column whereas left outer one is not restricted on the partition column
SELECT t1.*, t2.* FROM test_hash1 t1 FULL JOIN test_hash2 t2 ON (t1.col2 = t2.col2) ORDER BY 1,2,3,4;
-- both tables are not restricted on partition column
SELECT t1.*, t2.* FROM test_hash1 t1 FULL JOIN test_hash2 t2 ON (t1.col2 = t2.col1) ORDER BY 1,2,3,4;


-- join order planner can handle queries with multi joins consisting of outer joins with simple join clause

SELECT t1.*, t2.*, t3.* FROM test_hash1 t1 RIGHT JOIN test_hash2 t2 ON (t1.col1 = t2.col1) INNER JOIN test_hash3 t3 ON (t3.col1 = t1.col1) ORDER BY 1,2,3,4,5,6;
SELECT t1.*, t2.*, t3.* FROM test_hash1 t1 RIGHT JOIN test_hash2 t2 ON (t1.col1 = t2.col1) INNER JOIN test_hash3 t3 ON (t3.col1 = t2.col1) ORDER BY 1,2,3,4,5,6;
SELECT t1.*, t2.*, t3.* FROM test_hash1 t1 RIGHT JOIN test_hash2 t2 ON (t1.col2 = t2.col2) INNER JOIN test_hash3 t3 ON (t3.col2 = t1.col2) ORDER BY 1,2,3,4,5,6;
SELECT t1.*, t2.*, t3.* FROM test_hash1 t1 RIGHT JOIN test_hash2 t2 ON (t1.col2 = t2.col2) INNER JOIN test_hash3 t3 ON (t3.col2 = t2.col2) ORDER BY 1,2,3,4,5,6;
SELECT t1.*, t2.*, t3.* FROM test_hash1 t1 RIGHT JOIN test_hash2 t2 ON (t1.col2 = t2.col1) INNER JOIN test_hash3 t3 ON (t3.col2 = t1.col1) ORDER BY 1,2,3,4,5,6;
SELECT t1.*, t2.*, t3.* FROM test_hash1 t1 RIGHT JOIN test_hash2 t2 ON (t1.col2 = t2.col1) INNER JOIN test_hash3 t3 ON (t3.col2 = t2.col1) ORDER BY 1,2,3,4,5,6;

SELECT t1.*, t2.*, t3.* FROM test_hash2 t2 LEFT JOIN test_hash1 t1 ON (t1.col1 = t2.col1) INNER JOIN test_hash3 t3 ON (t3.col1 = t1.col1) ORDER BY 1,2,3,4,5,6;
SELECT t1.*, t2.*, t3.* FROM test_hash2 t2 LEFT JOIN test_hash1 t1 ON (t1.col1 = t2.col1) INNER JOIN test_hash3 t3 ON (t3.col1 = t2.col1) ORDER BY 1,2,3,4,5,6;
SELECT t1.*, t2.*, t3.* FROM test_hash2 t2 LEFT JOIN test_hash1 t1 ON (t1.col2 = t2.col2) INNER JOIN test_hash3 t3 ON (t3.col2 = t1.col2) ORDER BY 1,2,3,4,5,6;
SELECT t1.*, t2.*, t3.* FROM test_hash2 t2 LEFT JOIN test_hash1 t1 ON (t1.col2 = t2.col2) INNER JOIN test_hash3 t3 ON (t3.col2 = t2.col2) ORDER BY 1,2,3,4,5,6;
SELECT t1.*, t2.*, t3.* FROM test_hash2 t2 LEFT JOIN test_hash1 t1 ON (t1.col2 = t2.col1) INNER JOIN test_hash3 t3 ON (t3.col2 = t1.col1) ORDER BY 1,2,3,4,5,6;
SELECT t1.*, t2.*, t3.* FROM test_hash2 t2 LEFT JOIN test_hash1 t1 ON (t1.col2 = t2.col1) INNER JOIN test_hash3 t3 ON (t3.col2 = t2.col1) ORDER BY 1,2,3,4,5,6;

SELECT t1.*, t2.*, t3.* FROM test_hash1 t1 FULL JOIN test_hash2 t2 ON (t1.col1 = t2.col1) INNER JOIN test_hash3 t3 ON (t3.col1 = t1.col1) ORDER BY 1,2,3,4,5,6;
SELECT t1.*, t2.*, t3.* FROM test_hash1 t1 FULL JOIN test_hash2 t2 ON (t1.col1 = t2.col1) INNER JOIN test_hash3 t3 ON (t3.col1 = t2.col1) ORDER BY 1,2,3,4,5,6;
SELECT t1.*, t2.*, t3.* FROM test_hash1 t1 FULL JOIN test_hash2 t2 ON (t1.col2 = t2.col2) INNER JOIN test_hash3 t3 ON (t3.col2 = t1.col2) ORDER BY 1,2,3,4,5,6;
SELECT t1.*, t2.*, t3.* FROM test_hash1 t1 FULL JOIN test_hash2 t2 ON (t1.col2 = t2.col2) INNER JOIN test_hash3 t3 ON (t3.col2 = t2.col2) ORDER BY 1,2,3,4,5,6;
SELECT t1.*, t2.*, t3.* FROM test_hash1 t1 FULL JOIN test_hash2 t2 ON (t1.col2 = t2.col1) INNER JOIN test_hash3 t3 ON (t3.col2 = t1.col1) ORDER BY 1,2,3,4,5,6;
SELECT t1.*, t2.*, t3.* FROM test_hash1 t1 FULL JOIN test_hash2 t2 ON (t1.col2 = t2.col1) INNER JOIN test_hash3 t3 ON (t3.col2 = t2.col1) ORDER BY 1,2,3,4,5,6;

-- join order planner handles left outer join between tables with nonsimple join or where clause

SELECT t1.*, t2.* FROM test_hash1 t1 LEFT JOIN test_hash2 t2 ON (t1.col1 = t2.col1) WHERE (t1.col1 IS NULL or t2.col2 IS NULL) ORDER BY 1,2,3,4;
SELECT t1.*, t2.* FROM test_hash1 t1 LEFT JOIN test_hash2 t2 ON (t1.col1 = t2.col1 and t1.col1 < 0) ORDER BY 1,2,3,4;

-- join order planner supports repartition join between append distributed tables

CREATE TABLE test_append1 (col1 INT, col2 INT);
SELECT create_distributed_table('test_append1', 'col1', 'append');
SELECT master_create_empty_shard('test_append1') AS new_shard_id \gset
UPDATE pg_dist_shard SET shardminvalue = 1, shardmaxvalue = 5 WHERE shardid = :new_shard_id;
SELECT master_create_empty_shard('test_append1') AS new_shard_id \gset
UPDATE pg_dist_shard SET shardminvalue = 6, shardmaxvalue = 10 WHERE shardid = :new_shard_id;
INSERT INTO test_append1 VALUES (1,1),(2,2),(3,3),(4,4),(5,5),(6,6),(7,7),(8,8),(9,9),(10,10);

CREATE TABLE test_append2 (col1 INT, col2 INT);
SELECT create_distributed_table('test_append2', 'col2', 'append');
SELECT master_create_empty_shard('test_append2') AS new_shard_id \gset
UPDATE pg_dist_shard SET shardminvalue = 6, shardmaxvalue = 10 WHERE shardid = :new_shard_id;
SELECT master_create_empty_shard('test_append2') AS new_shard_id \gset
UPDATE pg_dist_shard SET shardminvalue = 11, shardmaxvalue = 15 WHERE shardid = :new_shard_id;
INSERT INTO test_append2 VALUES (6,6),(7,7),(8,8),(9,9),(10,10),(11,11),(12,12),(13,13),(14,14),(15,15);

CREATE TABLE test_append3(col1 INT, col2 INT);
SELECT create_distributed_table('test_append3', 'col1', 'append');
SELECT master_create_empty_shard('test_append3') AS new_shard_id \gset
UPDATE pg_dist_shard SET shardminvalue = 11, shardmaxvalue = 15 WHERE shardid = :new_shard_id;
SELECT master_create_empty_shard('test_append3') AS new_shard_id \gset
UPDATE pg_dist_shard SET shardminvalue = 16, shardmaxvalue = 20 WHERE shardid = :new_shard_id;
INSERT INTO test_append3 VALUES (11,11),(12,12),(13,13),(14,14),(15,15),(16,16),(17,17),(18,18),(19,19),(20,20);

-- join order planner supports repartition join between append-append distributed tables

SELECT t1.*, t2.* FROM test_append1 t1 LEFT JOIN test_append2 t2 ON (t1.col1 = t2.col1) ORDER BY 1,2,3,4;
SELECT t1.*, t2.* FROM test_append1 t1 RIGHT JOIN test_append2 t2 ON (t1.col1 = t2.col1) ORDER BY 1,2,3,4;
SELECT t1.*, t2.* FROM test_append1 t1 FULL JOIN test_append2 t2 ON (t1.col1 = t2.col1) ORDER BY 1,2,3,4;
SELECT t1.*, t2.*, t3.* FROM test_append1 t1 RIGHT JOIN test_append2 t2 ON (t1.col1 = t2.col1) INNER JOIN test_append3 t3 ON (t3.col1 = t2.col1) ORDER BY 1,2,3,4,5,6;
SELECT t1.*, t2.*, t3.* FROM test_append2 t2 LEFT JOIN test_append1 t1 ON (t1.col1 = t2.col1) INNER JOIN test_append3 t3 ON (t3.col1 = t2.col1) ORDER BY 1,2,3,4,5,6;
SELECT t1.*, t2.*, t3.* FROM test_append1 t1 FULL JOIN test_append2 t2 ON (t1.col1 = t2.col1) INNER JOIN test_append3 t3 ON (t3.col1 = t2.col1) ORDER BY 1,2,3,4,5,6;

-- join order planner supports repartition join between append-hash distributed tables

SELECT t1.*, t2.* FROM test_append1 t1 LEFT JOIN test_hash2 t2 ON (t1.col1 = t2.col1) ORDER BY 1,2,3,4;
SELECT t1.*, t2.* FROM test_hash1 t1 LEFT JOIN test_append2 t2 ON (t1.col1 = t2.col1) ORDER BY 1,2,3,4;
SELECT t1.*, t2.* FROM test_append1 t1 RIGHT JOIN test_hash2 t2 ON (t1.col1 = t2.col1) ORDER BY 1,2,3,4;
SELECT t1.*, t2.* FROM test_hash1 t1 RIGHT JOIN test_append2 t2 ON (t1.col1 = t2.col1) ORDER BY 1,2,3,4;
SELECT t1.*, t2.* FROM test_append1 t1 FULL JOIN test_hash2 t2 ON (t1.col1 = t2.col1) ORDER BY 1,2,3,4;
SELECT t1.*, t2.* FROM test_hash1 t1 FULL JOIN test_append2 t2 ON (t1.col1 = t2.col1) ORDER BY 1,2,3,4;
SELECT t1.*, t2.*, t3.* FROM test_hash1 t1 RIGHT JOIN test_append2 t2 ON (t1.col1 = t2.col1) INNER JOIN test_hash3 t3 ON (t3.col1 = t2.col1) ORDER BY 1,2,3,4,5,6;
SELECT t1.*, t2.*, t3.* FROM test_append2 t2 LEFT JOIN test_hash1 t1 ON (t1.col1 = t2.col1) INNER JOIN test_hash3 t3 ON (t3.col1 = t2.col1) ORDER BY 1,2,3,4,5,6;
SELECT t1.*, t2.*, t3.* FROM test_hash1 t1 FULL JOIN test_append2 t2 ON (t1.col1 = t2.col1) INNER JOIN test_hash3 t3 ON (t3.col1 = t2.col1) ORDER BY 1,2,3,4,5,6;

-- join order planner supports repartition join between range distributed tables

CREATE TABLE test_range1(col1 INT, col2 INT);
SELECT create_distributed_table('test_range1', 'col1', 'range');
CALL public.create_range_partitioned_shards('test_range1',
                                            '{0,6,11,16}',
                                            '{5,10,15,20}');
INSERT INTO test_range1 SELECT i, i FROM generate_series(1,10) i;

CREATE TABLE test_range2(col1 INT, col2 INT);
SELECT create_distributed_table('test_range2', 'col2', 'range');
CALL public.create_range_partitioned_shards('test_range2',
                                            '{0,6,11,16}',
                                            '{5,10,15,20}');
INSERT INTO test_range2 SELECT i, i FROM generate_series(6,15) i;

CREATE TABLE test_range3(col1 INT, col2 INT);
SELECT create_distributed_table('test_range3', 'col1', 'range');
CALL public.create_range_partitioned_shards('test_range3',
                                            '{0,6,11,16}',
                                            '{5,10,15,20}');
INSERT INTO test_range3 SELECT i, i FROM generate_series(11,20) i;

-- join order planner supports repartition join between range-range distributed tables

SELECT t1.*, t2.* FROM test_range1 t1 LEFT JOIN test_range2 t2 ON (t1.col1 = t2.col1) ORDER BY 1,2,3,4;
SELECT t1.*, t2.* FROM test_range1 t1 RIGHT JOIN test_range2 t2 ON (t1.col1 = t2.col1) ORDER BY 1,2,3,4;
SELECT t1.*, t2.* FROM test_range1 t1 FULL JOIN test_range2 t2 ON (t1.col1 = t2.col1) ORDER BY 1,2,3,4;
SELECT t1.*, t2.*, t3.* FROM test_range1 t1 RIGHT JOIN test_range2 t2 ON (t1.col1 = t2.col1) INNER JOIN test_range3 t3 ON (t3.col1 = t2.col1) ORDER BY 1,2,3,4,5,6;
SELECT t1.*, t2.*, t3.* FROM test_range2 t2 LEFT JOIN test_range1 t1 ON (t1.col1 = t2.col1) INNER JOIN test_range3 t3 ON (t3.col1 = t2.col1) ORDER BY 1,2,3,4,5,6;
SELECT t1.*, t2.*, t3.* FROM test_range1 t1 FULL JOIN test_range2 t2 ON (t1.col1 = t2.col1) INNER JOIN test_range3 t3 ON (t3.col1 = t2.col1) ORDER BY 1,2,3,4,5,6;

-- join order planner supports repartition join between range-hash distributed tables

SELECT t1.*, t2.* FROM test_range1 t1 LEFT JOIN test_hash2 t2 ON (t1.col1 = t2.col1) ORDER BY 1,2,3,4;
SELECT t1.*, t2.* FROM test_hash1 t1 LEFT JOIN test_range2 t2 ON (t1.col1 = t2.col1) ORDER BY 1,2,3,4;
SELECT t1.*, t2.* FROM test_range1 t1 RIGHT JOIN test_hash2 t2 ON (t1.col1 = t2.col1) ORDER BY 1,2,3,4;
SELECT t1.*, t2.* FROM test_hash1 t1 RIGHT JOIN test_range2 t2 ON (t1.col1 = t2.col1) ORDER BY 1,2,3,4;
SELECT t1.*, t2.* FROM test_range1 t1 FULL JOIN test_hash2 t2 ON (t1.col1 = t2.col1) ORDER BY 1,2,3,4;
SELECT t1.*, t2.* FROM test_hash1 t1 FULL JOIN test_range2 t2 ON (t1.col1 = t2.col1) ORDER BY 1,2,3,4;
SELECT t1.*, t2.*, t3.* FROM test_hash1 t1 RIGHT JOIN test_range2 t2 ON (t1.col1 = t2.col1) INNER JOIN test_hash3 t3 ON (t3.col1 = t2.col1) ORDER BY 1,2,3,4,5,6;
SELECT t1.*, t2.*, t3.* FROM test_range2 t2 LEFT JOIN test_hash1 t1 ON (t1.col1 = t2.col1) INNER JOIN test_hash3 t3 ON (t3.col1 = t2.col1) ORDER BY 1,2,3,4,5,6;
SELECT t1.*, t2.*, t3.* FROM test_hash1 t1 FULL JOIN test_range2 t2 ON (t1.col1 = t2.col1) INNER JOIN test_hash3 t3 ON (t3.col1 = t2.col1) ORDER BY 1,2,3,4,5,6;

-- join order planner supports repartition join between range-append distributed tables

SELECT t1.*, t2.* FROM test_range1 t1 LEFT JOIN test_append2 t2 ON (t1.col1 = t2.col1) ORDER BY 1,2,3,4;
SELECT t1.*, t2.* FROM test_append1 t1 LEFT JOIN test_range2 t2 ON (t1.col1 = t2.col1) ORDER BY 1,2,3,4;
SELECT t1.*, t2.* FROM test_range1 t1 RIGHT JOIN test_append2 t2 ON (t1.col1 = t2.col1) ORDER BY 1,2,3,4;
SELECT t1.*, t2.* FROM test_append1 t1 RIGHT JOIN test_range2 t2 ON (t1.col1 = t2.col1) ORDER BY 1,2,3,4;
SELECT t1.*, t2.* FROM test_range1 t1 FULL JOIN test_append2 t2 ON (t1.col1 = t2.col1) ORDER BY 1,2,3,4;
SELECT t1.*, t2.* FROM test_append1 t1 FULL JOIN test_range2 t2 ON (t1.col1 = t2.col1) ORDER BY 1,2,3,4;
SELECT t1.*, t2.*, t3.* FROM test_append1 t1 RIGHT JOIN test_range2 t2 ON (t1.col1 = t2.col1) INNER JOIN test_hash3 t3 ON (t3.col1 = t2.col1) ORDER BY 1,2,3,4,5,6;
SELECT t1.*, t2.*, t3.* FROM test_range2 t2 LEFT JOIN test_append1 t1 ON (t1.col1 = t2.col1) INNER JOIN test_hash3 t3 ON (t3.col1 = t2.col1) ORDER BY 1,2,3,4,5,6;
SELECT t1.*, t2.*, t3.* FROM test_append1 t1 FULL JOIN test_range2 t2 ON (t1.col1 = t2.col1) INNER JOIN test_hash3 t3 ON (t3.col1 = t2.col1) ORDER BY 1,2,3,4,5,6;

-- join order planner cannot handle semi joins

SELECT t1.* FROM test_hash1 t1 WHERE EXISTS (SELECT * FROM test_hash2 t2 WHERE t1.col1 = t2.col1) ORDER BY 1,2;
SELECT t1.* FROM test_hash1 t1 WHERE EXISTS (SELECT * FROM test_hash2 t2 WHERE t1.col2 = t2.col2) ORDER BY 1,2;
SELECT t2.* FROM test_hash2 t2 WHERE EXISTS (SELECT * FROM test_hash1 t1 WHERE t1.col1 = t2.col1) ORDER BY 1,2;
SELECT t2.* FROM test_hash2 t2 WHERE EXISTS (SELECT * FROM test_hash1 t1 WHERE t1.col2 = t2.col2) ORDER BY 1,2;

-- join order planner cannot handle anti joins

SELECT t1.* FROM test_hash1 t1 WHERE NOT EXISTS (SELECT * FROM test_hash2 t2 WHERE t1.col1 = t2.col1) ORDER BY 1,2;
SELECT t1.* FROM test_hash1 t1 WHERE NOT EXISTS (SELECT * FROM test_hash2 t2 WHERE t1.col2 = t2.col2) ORDER BY 1,2;
SELECT t2.* FROM test_hash2 t2 WHERE NOT EXISTS (SELECT * FROM test_hash1 t1 WHERE t1.col1 = t2.col1) ORDER BY 1,2;
SELECT t2.* FROM test_hash2 t2 WHERE NOT EXISTS (SELECT * FROM test_hash1 t1 WHERE t1.col2 = t2.col2) ORDER BY 1,2;

-- join order planner cannot handle lateral outer joins

SELECT t1.*, tt2.* FROM test_hash1 t1 LEFT JOIN LATERAL (SELECT * FROM test_hash2 t2 WHERE t1.col1 = t2.col1) tt2 ON (t1.col1 = tt2.col1) ORDER BY 1,2,3,4;

-- join order planner cannot handle cartesian joins

SELECT tt1.*, t3.* FROM (SELECT t1.* FROM test_hash1 t1, test_hash2 t2) tt1 LEFT JOIN test_hash3 t3 ON (tt1.col1 = t3.col1) ORDER BY 1,2,3,4;

-- join order planner cannot handle right recursive joins

SELECT t1.*, t2.* FROM test_hash1 t1 LEFT JOIN ( test_hash2 t2 JOIN test_hash3 t3 ON t2.col2 = t3.col1) ON (t1.col1 = t2.col1) ORDER BY 1,2,3,4;


-- sometimes join filters are pushed down and applied before join by PG
CREATE TABLE dist1 (x INT, y INT);
CREATE TABLE dist2 (x INT, y INT);
SELECT create_distributed_table('dist1','x');
SELECT create_distributed_table('dist2','x');
INSERT INTO dist1 VALUES (1,2);
INSERT INTO dist1 VALUES (3,4);
INSERT INTO dist2 VALUES (1,2);
INSERT INTO dist2 VALUES (5,6);

-- single join condition
SELECT * FROM dist1 LEFT JOIN dist2 ON (dist1.x = dist2.x) ORDER BY 1,2,3,4;
-- single join condition and dist2.x >2 will be pushed down as it is on inner part of the join. e.g. filter out dist2.x <= 2 beforehand
SELECT * FROM dist1 LEFT JOIN dist2 ON (dist1.x = dist2.x AND dist2.x >2) ORDER BY 1,2,3,4;
-- single join condition and dist2.x >2 is regular filter and applied after join
SELECT * FROM dist1 LEFT JOIN dist2 ON (dist1.x = dist2.x) WHERE dist2.x >2 ORDER BY 1,2,3,4;
-- single join condition and dist1.x >2 will not be pushed down as it is on outer part of the join
SELECT * FROM dist1 LEFT JOIN dist2 ON (dist1.x = dist2.x AND dist1.x >2) ORDER BY 1,2,3,4;
-- single join condition and dist1.x >2 is regular filter and applied after join
SELECT * FROM dist1 LEFT JOIN dist2 ON (dist1.x = dist2.x) WHERE dist1.x >2 ORDER BY 1,2,3,4;


--- constant false filter as join filter for left join.
-- inner table will be converted to empty result. Constant filter will be applied before join but will not be pushdowned.
SELECT * FROM dist1 LEFT JOIN dist2 ON (dist1.y = dist2.y AND false) ORDER BY 1,2,3,4;
--- constant false filter as base filter for left join.
-- both tables will be converted to empty result .e.g RTE_RESULT
SELECT * FROM dist1 LEFT JOIN dist2 ON (dist1.y = dist2.y) WHERE false ORDER BY 1,2,3,4;
--- constant false filter as join filter for inner join.
-- both tables will be converted to empty result .e.g RTE_RESULT
SELECT * FROM dist1 INNER JOIN dist2 ON (dist1.y = dist2.y AND false) ORDER BY 1,2,3,4;
--- constant false filter as base filter for inner join.
-- both tables will be converted to empty result .e.g RTE_RESULT
SELECT * FROM dist1 INNER JOIN dist2 ON (dist1.y = dist2.y) WHERE false ORDER BY 1,2,3,4;


DROP SCHEMA non_colocated_outer_joins CASCADE;
RESET client_min_messages;
RESET citus.log_multi_join_order;
RESET citus.enable_repartition_joins;
