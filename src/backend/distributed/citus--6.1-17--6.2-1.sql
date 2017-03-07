/* citus--6.1-17--6.2-1.sql */

SET search_path = 'pg_catalog';

DROP FUNCTION IF EXISTS master_get_local_first_candidate_nodes();
DROP FUNCTION IF EXISTS master_get_round_robin_candidate_nodes();

DROP FUNCTION IF EXISTS master_stage_shard_row();
DROP FUNCTION IF EXISTS master_stage_shard_placement_row();

RESET search_path;
