/* cstore_fdw/cstore_fdw--1.4--1.5.sql */

CREATE FUNCTION cstore_clean_table_resources(oid)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE OR REPLACE FUNCTION cstore_drop_trigger()
	RETURNS event_trigger
	LANGUAGE plpgsql
	AS $csdt$
DECLARE v_obj record;
BEGIN
	FOR v_obj IN SELECT * FROM pg_event_trigger_dropped_objects() LOOP

		IF v_obj.object_type NOT IN ('table', 'foreign table') THEN
			CONTINUE;
		END IF;

		PERFORM cstore_clean_table_resources(v_obj.objid);

	END LOOP;
END;
$csdt$;

CREATE EVENT TRIGGER cstore_drop_event
	ON SQL_DROP
	EXECUTE PROCEDURE cstore_drop_trigger();
