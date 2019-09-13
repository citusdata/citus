/* citus--7.3-1--7.3-2 */

CREATE FUNCTION pg_catalog.citus_text_send_as_jsonb(text)
RETURNS bytea
LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT
AS 'MODULE_PATHNAME', $$citus_text_send_as_jsonb$$;
