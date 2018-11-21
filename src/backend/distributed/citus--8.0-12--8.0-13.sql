/* citus--8.0-12--8.0-13 */
CREATE FUNCTION citus_check_defaults_for_sslmode()
    RETURNS void
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$citus_check_defaults_for_sslmode$$;

DO LANGUAGE plpgsql
$$
BEGIN
    -- Citus 8.1 and higher default to requiring SSL for all outgoing connections
    -- (specified by citus.node_conninfo).
    -- If it looks like we are about to enforce ssl for outgoing connections on a postgres
    -- installation that does not have ssl turned on we fall back to sslmode=prefer for
    -- outgoing connections.
    -- This will only be the case for upgrades from previous versions of Citus, on new
    -- installations we will have turned on ssl in an earlier stage of the extension
    -- creation.
    IF
        NOT current_setting('ssl')::boolean
	THEN
	    PERFORM citus_check_defaults_for_sslmode();
	END IF;
END;
$$;

DROP FUNCTION citus_check_defaults_for_sslmode();
