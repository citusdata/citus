-- When there are no relations using the columnar access method, we don't automatically create
-- "citus_columnar" extension together with "citus" extension.
SELECT NOT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'citus_columnar') as citus_columnar_not_exists;

-- Likely, we should not have any columnar objects leftover from "old columnar", i.e., the
-- columnar access method that we had before Citus 11.1, around.
SELECT NOT EXISTS (SELECT 1 FROM pg_am WHERE pg_am.amname = 'columnar') as columnar_am_not_exists;
SELECT NOT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname IN ('columnar', 'columnar_internal')) as columnar_catalog_schemas_not_exists;
SELECT NOT EXISTS (SELECT 1 FROM pg_proc WHERE proname IN ('alter_columnar_table_set', 'alter_columnar_table_reset', 'upgrade_columnar_storage', 'downgrade_columnar_storage', 'columnar_ensure_am_depends_catalog')) as columnar_utilities_not_exists;
