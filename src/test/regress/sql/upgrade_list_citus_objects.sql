-- list all postgres objects belonging to the citus extension
SELECT pg_catalog.pg_describe_object(classid, objid, 0) AS description
FROM pg_catalog.pg_depend, pg_catalog.pg_extension e
WHERE refclassid = 'pg_catalog.pg_extension'::pg_catalog.regclass
  AND refobjid = e.oid
  AND deptype = 'e'
  AND e.extname='citus'
  AND pg_catalog.pg_describe_object(classid, objid, 0) != 'function any_value(anyelement)'
  AND pg_catalog.pg_describe_object(classid, objid, 0) != 'function any_value_agg(anyelement,anyelement)'
ORDER BY 1;
