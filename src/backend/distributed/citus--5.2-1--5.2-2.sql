/* citus--5.2-1--5.2-2.sql */

CREATE OR REPLACE FUNCTION pg_catalog.citus_truncate_trigger()
	RETURNS trigger
	LANGUAGE plpgsql
	SET search_path = 'pg_catalog'
	AS $cdbtt$
DECLARE
	partitionType char;
	commandText text;
BEGIN
	SELECT partmethod INTO partitionType
	FROM pg_dist_partition WHERE logicalrelid = TG_RELID;
	IF NOT FOUND THEN
		RETURN NEW;
	END IF;
	
	IF (partitionType = 'a') THEN
		PERFORM master_drop_all_shards(TG_RELID, TG_TABLE_SCHEMA, TG_TABLE_NAME);
	ELSE
		SELECT format('TRUNCATE TABLE %I.%I CASCADE', TG_TABLE_SCHEMA, TG_TABLE_NAME)
		INTO commandText;
		PERFORM master_modify_multiple_shards(commandText);
	END IF;

	RETURN NEW;
END;
$cdbtt$;
