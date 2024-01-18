CREATE OR REPLACE FUNCTION citus_internal.start_management_transaction(outer_xid xid8)
    RETURNS VOID
    LANGUAGE C
AS 'MODULE_PATHNAME', $$start_management_transaction$$;

COMMENT ON FUNCTION citus_internal.start_management_transaction(outer_xid xid8)
    IS 'internal Citus function that starts a management transaction in the main database';
