/* citus--7.3-2--7.3-3 */

/*****************************************************************************
 * Citus json aggregate helpers
 *****************************************************************************/

CREATE FUNCTION pg_catalog.citus_jsonb_concatenate(state jsonb, val jsonb)
	RETURNS jsonb
	LANGUAGE SQL
AS $function$
	SELECT CASE 
		WHEN val IS NULL THEN state
		WHEN jsonb_typeof(state) = 'null' THEN val
		ELSE state || val
	END;
$function$;

CREATE FUNCTION pg_catalog.citus_jsonb_concatenate_final(state jsonb)
	RETURNS jsonb
	LANGUAGE SQL
AS $function$
	SELECT CASE WHEN jsonb_typeof(state) = 'null' THEN NULL ELSE state END;
$function$;

CREATE FUNCTION pg_catalog.citus_json_concatenate(state json, val json)
	RETURNS json
	LANGUAGE SQL
AS $function$
	SELECT CASE 
		WHEN val IS NULL THEN state
		WHEN json_typeof(state) = 'null' THEN val
		WHEN json_typeof(state) = 'object' THEN
 			(SELECT json_object_agg(key, value) FROM (
		 		SELECT * FROM json_each(state)
		 		UNION ALL
		 		SELECT * FROM json_each(val)
	 		) t)
		ELSE 
	 		(SELECT json_agg(a) FROM (
	 			SELECT json_array_elements(state) AS a
		 		UNION ALL
		 		SELECT json_array_elements(val) AS a
	 		) t)
	END;
$function$;

CREATE FUNCTION pg_catalog.citus_json_concatenate_final(state json)
	RETURNS json
	LANGUAGE SQL
AS $function$
	SELECT CASE WHEN json_typeof(state) = 'null' THEN NULL ELSE state END;
$function$;


/*****************************************************************************
 * Citus json aggregates
 *****************************************************************************/

CREATE AGGREGATE pg_catalog.jsonb_cat_agg(jsonb) (
    SFUNC = citus_jsonb_concatenate,
    FINALFUNC = citus_jsonb_concatenate_final,
    STYPE = jsonb,
    INITCOND = 'null'
);
COMMENT ON AGGREGATE pg_catalog.jsonb_cat_agg(jsonb)
    IS 'concatenate input jsonbs into a single jsonb';
    
CREATE AGGREGATE pg_catalog.json_cat_agg(json) (
    SFUNC = citus_json_concatenate,
    FINALFUNC = citus_json_concatenate_final,
    STYPE = json,
    INITCOND = 'null'
);
COMMENT ON AGGREGATE pg_catalog.json_cat_agg(json)
    IS 'concatenate input jsons into a single json';
