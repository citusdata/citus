ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 1660000;

CREATE TABLE partitioning_test(id int, time date) PARTITION BY RANGE (time);
 
-- create its partitions
CREATE TABLE partitioning_test_2009 PARTITION OF partitioning_test FOR VALUES FROM ('2009-01-01') TO ('2010-01-01');
CREATE TABLE partitioning_test_2010 PARTITION OF partitioning_test FOR VALUES FROM ('2010-01-01') TO ('2011-01-01');

-- load some data and distribute tables
INSERT INTO partitioning_test VALUES (1, '2009-06-06');
INSERT INTO partitioning_test VALUES (2, '2010-07-07');

INSERT INTO partitioning_test_2009 VALUES (3, '2009-09-09');
INSERT INTO partitioning_test_2010 VALUES (4, '2010-03-03');

SET citus.shard_count TO 4;
SET citus.shard_replication_factor TO 1;

-- this should error out given that parent of the partition is not distributed
SELECT create_distributed_table('partitioning_test_2010', 'id');

-- this should suceed
SELECT create_distributed_table('partitioning_test', 'id');

-- check the data
SELECT * FROM partitioning_test ORDER BY 1;

-- check the metadata
SELECT 
	* 
FROM 
	pg_dist_partition 
WHERE 
	logicalrelid IN ('partitioning_test', 'partitioning_test_2009', 'partitioning_test_2010')
ORDER BY 1;

SELECT 
	logicalrelid, count(*) 
FROM pg_dist_shard 
	WHERE logicalrelid IN ('partitioning_test', 'partitioning_test_2009', 'partitioning_test_2010')
GROUP BY
	logicalrelid
ORDER BY
	1,2;

SELECT 
	nodename, nodeport, count(*)	
FROM
	pg_dist_shard_placement
WHERE
	shardid IN (SELECT shardid FROM pg_dist_shard WHERE logicalrelid IN ('partitioning_test', 'partitioning_test_2009', 'partitioning_test_2010') )
GROUP BY
	nodename, nodeport
ORDER BY
	1,2,3;

-- now create a partition and see that it also becomes a distributed table
CREATE TABLE partitioning_test_2011 PARTITION OF partitioning_test FOR VALUES FROM ('2011-01-01') TO ('2012-01-01');

SELECT 
	* 
FROM 
	pg_dist_partition 
WHERE 
	logicalrelid IN ('partitioning_test', 'partitioning_test_2009', 'partitioning_test_2010', 'partitioning_test_2011')
ORDER BY 1;

SELECT 
	logicalrelid, count(*) 
FROM pg_dist_shard 
	WHERE logicalrelid IN ('partitioning_test', 'partitioning_test_2009', 'partitioning_test_2010', 'partitioning_test_2011')
GROUP BY
	logicalrelid
ORDER BY
	1,2;

SELECT 
	nodename, nodeport, count(*)	
FROM
	pg_dist_shard_placement
WHERE
	shardid IN (SELECT shardid FROM pg_dist_shard WHERE logicalrelid IN ('partitioning_test', 'partitioning_test_2009', 'partitioning_test_2010', 'partitioning_test_2011') )
GROUP BY
	nodename, nodeport
ORDER BY
	1,2,3;


-- citus can also support ALTER TABLE .. ATTACH PARTITION 
-- even if the partition is not distributed
CREATE TABLE partitioning_test_2012(id int, time date);
ALTER TABLE partitioning_test ATTACH PARTITION partitioning_test_2012 FOR VALUES FROM ('2012-01-01') TO ('2013-01-01');

SELECT 
	* 
FROM 
	pg_dist_partition 
WHERE 
	logicalrelid IN ('partitioning_test', 'partitioning_test_2009', 'partitioning_test_2010', 'partitioning_test_2011', 'partitioning_test_2012')
ORDER BY 1;

SELECT 
	logicalrelid, count(*) 
FROM pg_dist_shard 
	WHERE logicalrelid IN ('partitioning_test', 'partitioning_test_2009', 'partitioning_test_2010', 'partitioning_test_2011', 'partitioning_test_2012')
GROUP BY
	logicalrelid
ORDER BY
	1,2;

SELECT 
	nodename, nodeport, count(*)	
FROM
	pg_dist_shard_placement
WHERE
	shardid IN (SELECT shardid FROM pg_dist_shard WHERE logicalrelid IN ('partitioning_test', 'partitioning_test_2009', 'partitioning_test_2010', 'partitioning_test_2011', 'partitioning_test_2012') )
GROUP BY
	nodename, nodeport
ORDER BY
	1,2,3;


-- dropping the parent should CASCADE to the children as well
DROP TABLE partitioning_test;

\d+ partitioning_test*
