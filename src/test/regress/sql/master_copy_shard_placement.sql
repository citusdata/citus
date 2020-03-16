-- Tests for master_copy_shard_placement, which can be used for adding replicas in statement-based replication
CREATE SCHEMA mcsp;
SET search_path TO mcsp;
SET citus.next_shard_id TO 8139000;
SET citus.shard_replication_factor TO 1;
SET citus.replication_model TO 'statement';

CREATE TABLE ref_table(a int);
SELECT create_reference_table('ref_table');

CREATE TABLE data (
  key text primary key,
  value text not null,
  check (value <> '')
);
CREATE INDEX ON data (value);
SELECT create_distributed_table('data','key');

CREATE TABLE history (
  key text not null,
  t timestamptz not null,
  value text not null
) PARTITION BY RANGE (t);
CREATE TABLE history_p1 PARTITION OF history FOR VALUES FROM ('2019-01-01') TO ('2020-01-01');
CREATE TABLE history_p2 PARTITION OF history FOR VALUES FROM ('2020-01-01') TO ('2021-01-01');
SELECT create_distributed_table('history','key');

INSERT INTO data VALUES ('key-1', 'value-1');
INSERT INTO data VALUES ('key-2', 'value-2');

INSERT INTO history VALUES ('key-1', '2020-02-01', 'old');
INSERT INTO history VALUES ('key-1', '2019-10-01', 'older');

-- verify we error out if no healthy placement exists at source
SELECT master_copy_shard_placement(
           get_shard_id_for_distribution_column('data', 'key-1'),
           'localhost', :worker_1_port,
           'localhost', :worker_2_port,
           do_repair := false);

-- verify we error out if source and destination are the same
SELECT master_copy_shard_placement(
           get_shard_id_for_distribution_column('data', 'key-1'),
           'localhost', :worker_2_port,
           'localhost', :worker_2_port,
           do_repair := false);

-- verify we error out if target already contains a healthy placement
SELECT master_copy_shard_placement(
           (SELECT shardid FROM pg_dist_shard WHERE logicalrelid='ref_table'::regclass::oid),
           'localhost', :worker_1_port,
           'localhost', :worker_2_port,
           do_repair := false);

-- replicate shard that contains key-1
SELECT master_copy_shard_placement(
           get_shard_id_for_distribution_column('data', 'key-1'),
           'localhost', :worker_2_port,
           'localhost', :worker_1_port,
           do_repair := false);

-- forcefully mark the old replica as inactive
UPDATE pg_dist_shard_placement SET shardstate = 3
WHERE shardid = get_shard_id_for_distribution_column('data', 'key-1') AND nodeport = :worker_2_port;

UPDATE pg_dist_shard_placement SET shardstate = 3
WHERE shardid = get_shard_id_for_distribution_column('history', 'key-1') AND nodeport = :worker_2_port;

-- should still have all data available thanks to new replica
SELECT count(*) FROM data;
SELECT count(*) FROM history;

-- test we can not replicate MX tables
SET citus.shard_replication_factor TO 1;
SET citus.replication_model TO 'streaming';

SELECT start_metadata_sync_to_node('localhost', :worker_1_port);

CREATE TABLE mx_table(a int);
SELECT create_distributed_table('mx_table', 'a');

SELECT master_copy_shard_placement(
           get_shard_id_for_distribution_column('mx_table', '1'),
           'localhost', :worker_1_port,
           'localhost', :worker_2_port,
           do_repair := false);

SELECT stop_metadata_sync_to_node('localhost', :worker_1_port);

SET client_min_messages TO ERROR;
DROP SCHEMA mcsp CASCADE;
