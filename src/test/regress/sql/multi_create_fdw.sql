
SET citus.next_shard_id TO 390000;


-- ===================================================================
-- get ready for the foreign data wrapper tests
-- ===================================================================

-- create fake fdw for use in tests
SET client_min_messages TO WARNING;
DROP SERVER IF EXISTS fake_fdw_server CASCADE;
DROP FOREIGN DATA WRAPPER IF EXISTS fake_fdw CASCADE;
RESET client_min_messages;

CREATE OR REPLACE FUNCTION fake_fdw_handler()
RETURNS fdw_handler
AS 'citus'
LANGUAGE C STRICT;

CREATE FOREIGN DATA WRAPPER fake_fdw HANDLER fake_fdw_handler;

-- Since we are assuming fdw should be part of the extension, add and drop it manually.
ALTER EXTENSION citus ADD FOREIGN DATA WRAPPER fake_fdw;
CREATE SERVER fake_fdw_server FOREIGN DATA WRAPPER fake_fdw;
ALTER EXTENSION citus DROP FOREIGN DATA WRAPPER fake_fdw;
