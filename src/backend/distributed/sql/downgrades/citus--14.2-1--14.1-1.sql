-- citus--14.2-1--14.1-1
-- downgrade version to 14.1-1

DROP FUNCTION IF EXISTS citus_internal.distribute_object(oid, oid, int, boolean);
