/*
 * nodeport should be 32-bit, not 64-bit
 */
ALTER TABLE pg_dist_shard_placement ALTER nodeport TYPE int4;
