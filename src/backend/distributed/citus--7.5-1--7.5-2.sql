/* citus--7.5-1--7.5-2 */
SET search_path = 'pg_catalog';

-- note that we're not dropping the older version of the function
CREATE FUNCTION pg_catalog.role_exists(name)
    RETURNS boolean
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$role_exists$$;
COMMENT ON FUNCTION role_exists(name) IS 'returns whether a role exists';

CREATE FUNCTION pg_catalog.authinfo_valid(text)
	RETURNS boolean
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$authinfo_valid$$;
COMMENT ON FUNCTION authinfo_valid(text) IS 'returns whether an authinfo is valid';

CREATE TABLE citus.pg_dist_authinfo (
	nodeid integer NOT NULL,
	rolename name NOT NULL
	              CONSTRAINT role_exists
								CHECK (role_exists(rolename)),
	authinfo text NOT NULL
	              CONSTRAINT authinfo_valid
								CHECK (authinfo_valid(authinfo))
);

CREATE UNIQUE INDEX pg_dist_authinfo_identification_index
ON citus.pg_dist_authinfo (rolename, nodeid DESC);

ALTER TABLE citus.pg_dist_authinfo SET SCHEMA pg_catalog;

REVOKE ALL ON pg_catalog.pg_dist_authinfo FROM PUBLIC;

RESET search_path;
