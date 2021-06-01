-- Test multi row insert with composite type as partition key
-- https://github.com/citusdata/citus/issues/3357

SET citus.next_shard_id TO 4213581;
SET citus.shard_replication_factor TO 1;

CREATE TYPE composite_key_type AS (f1 int, f2 text);
CREATE TABLE source_table_xyz(key composite_key_type, value int, mapped_key composite_key_type);
SELECT create_distributed_table('source_table_xyz', 'key', 'range');
CALL public.create_range_partitioned_shards('source_table_xyz', '{"(0,a)","(25,z)"}','{"(24,a)","(49,z)"}');

SELECT * FROM pg_dist_shard WHERE logicalrelid='source_table_xyz'::regclass::oid ORDER BY shardid;
SELECT shardid, nodename, nodeport FROM pg_dist_shard_placement WHERE EXISTS(SELECT shardid FROM pg_dist_shard WHERE shardid=pg_dist_shard_placement.shardid AND logicalrelid='source_table_xyz'::regclass::oid) ORDER BY 1, 2, 3;

INSERT INTO source_table_xyz VALUES ((0, 'a'), 1, (0, 'a')),
                                ((1, 'b'), 2, (26, 'b')),
                                ((2, 'c'), 3, (3, 'c')),
                                ('(4,d)', 4, (27, 'd')),
                                ((30, 'e'), 5, (30, 'e')),
                                ((31, 'f'), 6, (31, 'f')),
                                ((32, 'g'), 7, (8, 'g'));

INSERT INTO source_table_xyz VALUES ((27, 'h'), 8, (26, 'b'));


SELECT * FROM source_table_xyz ORDER BY 1;

DROP TABLE source_table_xyz;
DROP TYPE composite_key_type;
