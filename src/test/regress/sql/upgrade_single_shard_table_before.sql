CREATE TABLE null_shard_key (id bigserial, name text);
SELECT create_distributed_table('null_shard_key', null);
INSERT INTO null_shard_key (name) VALUES ('a'), ('b');
