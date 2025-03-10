-- In PG17, Auto-generated array types, multirange types, and relation rowtypes
-- are treated as dependent objects, hence changing the output of the
-- print_extension_changes function.
-- Relevant PG commit: e5bc9454e527b1cba97553531d8d4992892fdeef
-- Here we create a table with only the basic extension types
-- in order to avoid printing extra ones for now
-- This can be removed when we drop PG16 support.
CREATE TABLE extension_basic_types (description text);
INSERT INTO extension_basic_types VALUES ('type citus.distribution_type'),
                                         ('type citus.shard_transfer_mode'),
                                         ('type citus_copy_format'),
                                         ('type noderole'),
                                         ('type citus_job_status'),
                                         ('type citus_task_status'),
                                         ('type replication_slot_info'),
                                         ('type split_copy_info'),
                                         ('type split_shard_info'),
                                         ('type cluster_clock');

-- list all postgres objects belonging to the citus extension
SELECT pg_catalog.pg_describe_object(classid, objid, 0) AS description
FROM pg_catalog.pg_depend, pg_catalog.pg_extension e
WHERE refclassid = 'pg_catalog.pg_extension'::pg_catalog.regclass
  AND refobjid = e.oid
  AND deptype = 'e'
  AND e.extname='citus'
  AND (pg_catalog.pg_describe_object(classid, objid, 0) NOT LIKE 'type%'
       OR
       pg_catalog.pg_describe_object(classid, objid, 0) IN (SELECT * FROM extension_basic_types))
  AND pg_catalog.pg_describe_object(classid, objid, 0) != 'function any_value(anyelement)'
  AND pg_catalog.pg_describe_object(classid, objid, 0) != 'function any_value_agg(anyelement,anyelement)'
ORDER BY 1;

DROP TABLE extension_basic_types;
