CREATE SCHEMA collation_conflict;
SELECT run_command_on_workers($$CREATE SCHEMA collation_conflict;$$);

\c - - :public_worker_1_host :worker_1_port
SET search_path TO collation_conflict;

CREATE COLLATION caseinsensitive (
	provider = icu,
	locale = 'und-u-ks-level2'
);

\c - - :master_host :master_port
SET search_path TO collation_conflict;

CREATE COLLATION caseinsensitive (
	provider = icu,
	locale = 'und-u-ks-level2'
);
CREATE TABLE tblcoll(val text COLLATE caseinsensitive);
SELECT create_reference_table('tblcoll');

\c - - :public_worker_1_host :worker_1_port
SELECT c.collname, nsp.nspname, a.rolname
FROM pg_collation c
JOIN pg_namespace nsp ON nsp.oid = c.collnamespace
JOIN pg_authid a ON a.oid = c.collowner
WHERE collname like 'caseinsensitive%'
ORDER BY 1,2,3;
\c - - :master_host :master_port
SET search_path TO collation_conflict;

-- Now drop & recreate in order to make sure rename detects the existing renamed objects
-- hide cascades
--SET client_min_messages TO error;
DROP TABLE tblcoll;
DROP COLLATION caseinsensitive;

\c - - :public_worker_1_host :worker_1_port
SET search_path TO collation_conflict;

CREATE COLLATION caseinsensitive (
	provider = icu,
	locale = 'und-u-ks-level1'
);

\c - - :master_host :master_port
SET search_path TO collation_conflict;

CREATE COLLATION caseinsensitive (
	provider = icu,
	locale = 'und-u-ks-level2'
);
CREATE TABLE tblcoll(val text COLLATE caseinsensitive);
SELECT create_reference_table('tblcoll');

\c - - :public_worker_1_host :worker_1_port
SELECT c.collname, nsp.nspname, a.rolname
FROM pg_collation c
JOIN pg_namespace nsp ON nsp.oid = c.collnamespace
JOIN pg_authid a ON a.oid = c.collowner
WHERE collname like 'caseinsensitive%'
ORDER BY 1,2,3;
\c - - :master_host :master_port
SET search_path TO collation_conflict;

-- now test worker_create_or_replace_object directly
SELECT worker_create_or_replace_object($$CREATE COLLATION collation_conflict.caseinsensitive (provider = 'icu', lc_collate = 'und-u-ks-level2', lc_ctype = 'und-u-ks-level2')$$);
SELECT worker_create_or_replace_object($$CREATE COLLATION collation_conflict.caseinsensitive (provider = 'icu', lc_collate = 'und-u-ks-level2', lc_ctype = 'und-u-ks-level2')$$);

-- hide cascades
SET client_min_messages TO error;
DROP SCHEMA collation_conflict CASCADE;

