CREATE SCHEMA collation_conflict;

\c - - - :worker_1_port
SET search_path TO collation_conflict;

SET citus.enable_metadata_sync TO off;

CREATE COLLATION caseinsensitive (
	provider = icu,
	locale = 'und-u-ks-level2'
);

\c - - - :master_port
SET search_path TO collation_conflict;

CREATE COLLATION caseinsensitive (
	provider = icu,
	locale = 'und-u-ks-level2'
);
CREATE TABLE tblcoll(val text COLLATE caseinsensitive);
SELECT create_reference_table('tblcoll');

\c - - - :worker_1_port
SELECT c.collname, nsp.nspname, a.rolname
FROM pg_collation c
JOIN pg_namespace nsp ON nsp.oid = c.collnamespace
JOIN pg_authid a ON a.oid = c.collowner
WHERE collname like 'caseinsensitive%'
ORDER BY 1,2,3;
\c - - - :master_port
SET search_path TO collation_conflict;

-- Now drop & recreate in order to make sure rename detects the existing renamed objects
-- hide cascades
--SET client_min_messages TO error;
DROP TABLE tblcoll;
DROP COLLATION caseinsensitive;

\c - - - :worker_1_port
SET citus.enable_metadata_sync TO off;

SET search_path TO collation_conflict;

CREATE COLLATION caseinsensitive (
	provider = icu,
	locale = 'und-u-ks-level1'
);

\c - - - :master_port
SET search_path TO collation_conflict;

CREATE COLLATION caseinsensitive (
	provider = icu,
	locale = 'und-u-ks-level2'
);
CREATE TABLE tblcoll(val text COLLATE caseinsensitive);
SELECT create_reference_table('tblcoll');

\c - - - :worker_1_port
SELECT c.collname, nsp.nspname, a.rolname
FROM pg_collation c
JOIN pg_namespace nsp ON nsp.oid = c.collnamespace
JOIN pg_authid a ON a.oid = c.collowner
WHERE collname like 'caseinsensitive%'
ORDER BY 1,2,3;
\c - - - :master_port
SET search_path TO collation_conflict;

-- now test worker_create_or_replace_object directly
SELECT worker_create_or_replace_object($$CREATE COLLATION collation_conflict.caseinsensitive (provider = 'icu', locale = 'und-u-ks-level2')$$);
SELECT worker_create_or_replace_object($$CREATE COLLATION collation_conflict.caseinsensitive (provider = 'icu', locale = 'und-u-ks-level2')$$);

-- hide cascades
SET client_min_messages TO error;
DROP SCHEMA collation_conflict CASCADE;

