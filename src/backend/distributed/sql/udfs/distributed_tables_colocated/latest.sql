--
-- distributed_tables_colocated returns true if given tables are co-located, false otherwise.
-- The function checks shard definitions, matches shard placements for given tables.
--
CREATE OR REPLACE FUNCTION pg_catalog.distributed_tables_colocated(table1 regclass,
																   table2 regclass)
RETURNS bool
LANGUAGE plpgsql
AS $function$
DECLARE
	table1_colocationid int;
	table2_colocationid int;
BEGIN
	SELECT colocationid INTO table1_colocationid
	FROM pg_catalog.pg_dist_partition WHERE logicalrelid = table1;

	SELECT colocationid INTO table2_colocationid
	FROM pg_catalog.pg_dist_partition WHERE logicalrelid = table2;

	RETURN table1_colocationid = table2_colocationid;
END;
$function$;
