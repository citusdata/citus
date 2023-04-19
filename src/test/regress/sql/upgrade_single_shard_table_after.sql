-- check that we properly retained the single-shard table
SELECT 1 FROM pg_dist_partition
WHERE logicalrelid = 'citus_schema.null_shard_key'::regclass AND
      partmethod = 'n' AND repmodel = 's' AND colocationid != 0;

INSERT INTO citus_schema.null_shard_key (name) VALUES ('c');
SELECT * FROM citus_schema.null_shard_key WHERE id <= 2 ORDER BY id;

-- Tests in after schedule are run twice, so we would insert a row with id=3
-- at the first run and a row with id=4 at the second run.
SELECT COUNT(*) IN (1,2) FROM citus_schema.null_shard_key WHERE id IN (3,4);

-- Check that we can create a distributed table with a single-shard
-- after upgrade.
CREATE TABLE citus_schema.null_shard_key_after_upgrade (id bigserial, name text);
SELECT create_distributed_table('citus_schema.null_shard_key_after_upgrade', null);
INSERT INTO citus_schema.null_shard_key_after_upgrade (name) VALUES ('c');
SELECT * FROM citus_schema.null_shard_key_after_upgrade ORDER BY id;

DROP TABLE citus_schema.null_shard_key_after_upgrade;
