-- print version above 11 (eg. 12 and above)
SHOW server_version \gset
SELECT substring(:'server_version', '\d+')::int > 11 AS version_above_eleven;

-- list all postgres objects belonging to the citus extension
SELECT pg_catalog.pg_describe_object(classid, objid, 0) AS description
FROM pg_catalog.pg_depend, pg_catalog.pg_extension e
WHERE refclassid = 'pg_catalog.pg_extension'::pg_catalog.regclass
  AND refobjid = e.oid
  AND deptype = 'e'
  AND e.extname='citus'
ORDER BY 1;
