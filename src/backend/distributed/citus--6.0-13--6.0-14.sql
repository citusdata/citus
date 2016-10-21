/* citus--6.0-13--6.0-14.sql */

DO $ff$
BEGIN
	-- fix functions created in wrong namespace
	ALTER FUNCTION public.recover_prepared_transactions()
	SET SCHEMA pg_catalog;

	ALTER FUNCTION public.column_name_to_column(table_name regclass, column_name text)
	SET SCHEMA pg_catalog;

	ALTER FUNCTION public.worker_drop_distributed_table(logicalrelid Oid)
	SET SCHEMA pg_catalog;

	ALTER FUNCTION public.master_get_new_placementid()
	SET SCHEMA pg_catalog;

	ALTER FUNCTION public.master_expire_table_cache(table_name regclass)
	SET SCHEMA pg_catalog;

-- some installations don't need this corrective, so just skip...
EXCEPTION WHEN undefined_function THEN
	-- do nothing
END
$ff$;
