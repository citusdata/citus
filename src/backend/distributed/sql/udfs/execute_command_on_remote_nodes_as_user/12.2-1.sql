CREATE OR REPLACE FUNCTION citus_internal.execute_command_on_remote_nodes_as_user(query text, username text)
    RETURNS VOID
    LANGUAGE C
AS 'MODULE_PATHNAME', $$execute_command_on_remote_nodes_as_user$$;

COMMENT ON FUNCTION citus_internal.execute_command_on_remote_nodes_as_user(query text, username text)
    IS 'executes a query on the nodes other than the current one';
