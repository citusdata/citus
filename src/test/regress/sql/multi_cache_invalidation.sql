-- test that we are tolerant to the relation ID of a shard being changed
-- and do not cache invalid metadata
CREATE SCHEMA mci_1;
CREATE SCHEMA mci_2;

SET citus.next_shard_id TO 1601000;
CREATE TABLE mci_1.test (test_id integer NOT NULL, data int);
CREATE TABLE mci_2.test (test_id integer NOT NULL, data int);
SELECT create_distributed_table('mci_1.test', 'test_id');
SELECT create_distributed_table('mci_2.test', 'test_id', 'append');

INSERT INTO mci_1.test VALUES (1,2), (3,4);

-- move shards into other append-distributed table
SELECT run_command_on_placements('mci_1.test', 'ALTER TABLE %s SET SCHEMA mci_2');
UPDATE pg_dist_shard
SET logicalrelid = 'mci_2.test'::regclass, shardminvalue = NULL, shardmaxvalue = NULL
WHERE logicalrelid = 'mci_1.test'::regclass;

SELECT * FROM mci_2.test ORDER BY test_id;

DROP SCHEMA mci_1 CASCADE;
DROP SCHEMA mci_2 CASCADE;
