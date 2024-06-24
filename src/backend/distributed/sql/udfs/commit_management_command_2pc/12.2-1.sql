CREATE OR REPLACE FUNCTION citus_internal.commit_management_command_2pc()
    RETURNS VOID
    LANGUAGE C
AS 'MODULE_PATHNAME', $$commit_management_command_2pc$$;

COMMENT ON FUNCTION citus_internal.commit_management_command_2pc()
    IS 'commits the coordinated remote transactions, is a wrapper function for CoordinatedRemoteTransactionsCommit';
