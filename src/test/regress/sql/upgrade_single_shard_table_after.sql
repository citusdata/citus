-- check that we properly retained the single-shard table
SELECT 1 FROM pg_dist_partition
WHERE logicalrelid = 'citus_schema.null_shard_key'::regclass AND
      partmethod = 'n' AND repmodel = 's' AND colocationid != 0;

BEGIN;
  INSERT INTO citus_schema.null_shard_key (name) VALUES ('c');
  SELECT * FROM citus_schema.null_shard_key ORDER BY id;
ROLLBACK;

-- Check that we can create a distributed table with a single-shard
-- after upgrade.
CREATE TABLE citus_schema.null_shard_key_after_upgrade (id bigserial, name text);
SELECT create_distributed_table('citus_schema.null_shard_key_after_upgrade', null);
INSERT INTO citus_schema.null_shard_key_after_upgrade (name) VALUES ('c');
SELECT * FROM citus_schema.null_shard_key_after_upgrade ORDER BY id;

DROP TABLE citus_schema.null_shard_key_after_upgrade;
