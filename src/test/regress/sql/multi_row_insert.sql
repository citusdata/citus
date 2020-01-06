CREATE OR REPLACE PROCEDURE create_range_partitioned_shards(rel regclass, minvalues text[], maxvalues text[])
AS $$
DECLARE
  new_shardid bigint;
  idx int;
BEGIN
  FOR idx IN SELECT * FROM generate_series(1, array_length(minvalues, 1))
  LOOP
    SELECT master_create_empty_shard(rel::text) INTO new_shardid;
    UPDATE pg_dist_shard SET shardminvalue=minvalues[idx], shardmaxvalue=maxvalues[idx] WHERE shardid=new_shardid;
  END LOOP;
END;
$$ LANGUAGE plpgsql;

SET citus.next_shard_id TO 4213581;

CREATE TYPE composite_key_type AS (f1 int, f2 text);
SET citus.shard_replication_factor TO 1;

-- source
DROP TABLE IF EXISTS source_table_xyz;
CREATE TABLE source_table_xyz(key composite_key_type, value int, mapped_key composite_key_type);
SELECT create_distributed_table('source_table_xyz', 'key', 'range');
CALL public.create_range_partitioned_shards('source_table_xyz', '{"(0,a)","(25,a)"}','{"(24,z)","(49,z)"}');

SELECT * FROM pg_dist_shard WHERE logicalrelid='source_table_xyz'::regclass::oid;
SELECT shardid, nodename, nodeport FROM pg_dist_shard_placement WHERE EXISTS(SELECT shardid FROM pg_dist_shard WHERE shardid=pg_dist_shard_placement.shardid AND logicalrelid='source_table_xyz'::regclass::oid);

INSERT INTO source_table_xyz VALUES ((0, 'a'), 1, (0, 'a')),  -- shard 1 -> shard 1
                                ((1, 'b'), 2, (26, 'b')), -- shard 1 -> shard 2
                                ((2, 'c'), 3, (3, 'c')),  -- shard 1 -> shard 1
                                ((4, 'd'), 4, (27, 'd')), -- shard 1 -> shard 2
                                ((30, 'e'), 5, (30, 'e')), -- shard 2 -> shard 2
                                ((31, 'f'), 6, (31, 'f')), -- shard 2 -> shard 2
                                ((32, 'g'), 7, (8, 'g'));  -- shard 2 -> shard 1

SELECT * FROM source_table_xyz;
EXPLAIN SELECT * FROM source_table_xyz;

DROP TABLE source_table_xyz;
DROP TYPE composite_key_type;
