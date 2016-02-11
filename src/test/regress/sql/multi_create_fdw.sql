-- ===================================================================
-- get ready for the foreign data wrapper tests
-- ===================================================================

-- create fake fdw for use in tests
CREATE FUNCTION fake_fdw_handler()
RETURNS fdw_handler
AS 'citusdb'
LANGUAGE C STRICT;

CREATE FOREIGN DATA WRAPPER fake_fdw HANDLER fake_fdw_handler;
CREATE SERVER fake_fdw_server FOREIGN DATA WRAPPER fake_fdw;
