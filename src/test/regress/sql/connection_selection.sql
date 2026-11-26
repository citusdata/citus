CREATE SCHEMA connection_selection;
SET search_path TO connection_selection;

CREATE FUNCTION test_metadata_connection_selection(text, integer)
RETURNS void
LANGUAGE C STRICT
AS 'citus', $$test_metadata_connection_selection$$;

SELECT test_metadata_connection_selection('localhost', :worker_1_port);

-- run the checks again in a new transaction
SELECT test_metadata_connection_selection('localhost', :worker_1_port);

CREATE FUNCTION test_metadata_connection_id(text, integer, outside_transaction boolean)
RETURNS bigint
LANGUAGE C STRICT
AS 'citus', $$test_metadata_connection_id$$;

SELECT test_metadata_connection_id('localhost', :worker_1_port, false) AS metadata_id \gset
-- after commit, the same connection can be used outside of a transaction, as
-- the next transaction does not inherit the metadata connection
SELECT test_metadata_connection_id('localhost', :worker_1_port, true) = :metadata_id AS reused;

BEGIN;
SELECT test_metadata_connection_id('localhost', :worker_1_port, false) AS metadata_id \gset
SAVEPOINT s1;
SELECT test_metadata_connection_id('localhost', :worker_1_port, true) <> :metadata_id AS not_shared;
ROLLBACK TO SAVEPOINT s1;
-- rolling back to a savepoint keeps the metadata connection
SELECT test_metadata_connection_id('localhost', :worker_1_port, false) = :metadata_id AS kept;
COMMIT;

DROP FUNCTION test_metadata_connection_id(text, integer, boolean);
DROP FUNCTION test_metadata_connection_selection(text, integer);
DROP SCHEMA connection_selection;
