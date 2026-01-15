-- citus--15.0-1--14.0-1
-- downgrade version to 14.0-1

DROP FUNCTION IF EXISTS citus_internal.get_next_colocation_id();
