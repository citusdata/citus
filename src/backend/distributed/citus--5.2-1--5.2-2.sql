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
	IF FOUND THEN
		IF (partitionType = 'a') THEN
			PERFORM master_drop_all_shards(TG_RELID, TG_TABLE_SCHEMA, TG_TABLE_NAME);
		ELSE
			SELECT format('truncate table %s.%s', TG_TABLE_SCHEMA, TG_TABLE_NAME)
			INTO commandText;
			PERFORM master_modify_multiple_shards(commandText);
		END IF;
	END IF;
	RETURN NEW;
END;
$cdbtt$;
