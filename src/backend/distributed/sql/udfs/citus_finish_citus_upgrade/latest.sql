CREATE OR REPLACE PROCEDURE pg_catalog.citus_finish_citus_upgrade()
    LANGUAGE plpgsql
    SET search_path = pg_catalog
    AS $cppu$
DECLARE
    current_version_string text;
    last_upgrade_version_string text;
    last_upgrade_major_version int;
    last_upgrade_minor_version int;
    last_upgrade_sqlpatch_version int;
    performed_upgrade bool := false;
BEGIN
	SELECT extversion INTO current_version_string
	FROM pg_extension WHERE extname = 'citus';

	-- assume some arbitrarily old version when no last upgrade version is defined
	SELECT coalesce(metadata->>'last_upgrade_version', '8.0-1') INTO last_upgrade_version_string
	FROM pg_dist_node_metadata;

	SELECT r[1], r[2], r[3]
	FROM regexp_matches(last_upgrade_version_string,'([0-9]+)\.([0-9]+)-([0-9]+)','') r
	INTO last_upgrade_major_version, last_upgrade_minor_version, last_upgrade_sqlpatch_version;

	IF last_upgrade_major_version IS NULL OR last_upgrade_minor_version IS NULL OR last_upgrade_sqlpatch_version IS NULL THEN
		-- version string is not valid, use an arbitrarily old version number
		last_upgrade_major_version := 8;
		last_upgrade_minor_version := 0;
		last_upgrade_sqlpatch_version := 1;
	END IF;

	IF last_upgrade_major_version < 11 THEN
		PERFORM citus_finalize_upgrade_to_citus11();
		performed_upgrade := true;
	END IF;

	-- add new upgrade steps here

	IF NOT performed_upgrade THEN
		RAISE NOTICE 'already at the latest distributed schema version (%)', last_upgrade_version_string;
		RETURN;
	END IF;

	UPDATE pg_dist_node_metadata
	SET metadata = jsonb_set(metadata, array['last_upgrade_version'], to_jsonb(current_version_string));
END;
$cppu$;

COMMENT ON PROCEDURE pg_catalog.citus_finish_citus_upgrade()
    IS 'after upgrading Citus on all nodes call this function to upgrade the distributed schema';
