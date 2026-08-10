-- citus--13.4-1--13.3-1
-- downgrade version to 13.3-1

DROP FUNCTION IF EXISTS citus_internal.distribute_object(oid, oid, int, boolean);
