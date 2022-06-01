CREATE OR REPLACE FUNCTION citus_internal.columnar_ensure_am_depends_catalog()
  RETURNS void
  LANGUAGE plpgsql
  SET search_path = pg_catalog
AS $func$
BEGIN
  INSERT INTO pg_depend
  WITH columnar_schema_members(relid) AS (
    SELECT pg_class.oid AS relid FROM pg_class
      WHERE relnamespace =
            COALESCE(
	       (SELECT pg_namespace.oid FROM pg_namespace WHERE nspname = 'columnar_internal'),
	       (SELECT pg_namespace.oid FROM pg_namespace WHERE nspname = 'columnar')
	    )
        AND relname IN ('chunk',
                        'chunk_group',
                        'chunk_group_pkey',
                        'chunk_pkey',
                        'options',
                        'options_pkey',
                        'storageid_seq',
                        'stripe',
                        'stripe_first_row_number_idx',
                        'stripe_pkey')
  )
  SELECT -- Define a dependency edge from "columnar table access method" ..
         'pg_am'::regclass::oid as classid,
         (select oid from pg_am where amname = 'columnar') as objid,
         0 as objsubid,
         -- ... to each object that is registered to pg_class and that lives
         -- in "columnar" schema. That contains catalog tables, indexes
         -- created on them and the sequences created in "columnar" schema.
         --
         -- Given the possibility of user might have created their own objects
         -- in columnar schema, we explicitly specify list of objects that we
         -- are interested in.
         'pg_class'::regclass::oid as refclassid,
         columnar_schema_members.relid as refobjid,
         0 as refobjsubid,
         'n' as deptype
  FROM columnar_schema_members
  -- Avoid inserting duplicate entries into pg_depend.
  EXCEPT TABLE pg_depend;
END;
$func$;
COMMENT ON FUNCTION citus_internal.columnar_ensure_am_depends_catalog()
  IS 'internal function responsible for creating dependencies from columnar '
     'table access method to the rel objects in columnar schema';
