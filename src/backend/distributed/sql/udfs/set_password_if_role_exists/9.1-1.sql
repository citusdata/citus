CREATE OR REPLACE FUNCTION set_password_if_role_exists(
    role_name regrole,
    new_password text)
    RETURNS void
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$set_password_if_role_exists$$;

COMMENT ON FUNCTION set_password_if_role_exists(
    role_name regrole,
    new_password text)
    IS 'sets password of a role, if the role exists';