CREATE SCHEMA adv_lock_permission;
SET search_path to adv_lock_permission;

-- do not cache any connections, we change some settings and don't want old ones cached
SET citus.max_cached_conns_per_worker TO 0;

CREATE ROLE user_1 WITH LOGIN;

CREATE TABLE reference_table_1 (A int);
SELECT create_reference_table('reference_table_1');

CREATE TABLE reference_table_2 (A int);
SELECT create_reference_table('reference_table_2');

GRANT USAGE ON SCHEMA adv_lock_permission TO user_1;
GRANT SELECT ON reference_table_1 TO user_1;
GRANT INSERT, UPDATE ON reference_table_2 TO user_1;

SET ROLE user_1;

-- do not cache any connections, we change some settings and don't want old ones cached
SET citus.max_cached_conns_per_worker TO 0;
SET search_path to adv_lock_permission;

INSERT INTO reference_table_2 SELECT * FROM reference_table_1;

SET ROLE postgres;
-- do not cache any connections, we change some settings and don't want old ones cached
SET citus.max_cached_conns_per_worker TO 0;

-- change the role so that it can skip permission checks
ALTER ROLE user_1 SET citus.skip_advisory_lock_permission_checks TO on;

SET ROLE user_1;

SET citus.max_cached_conns_per_worker TO 0;
INSERT INTO reference_table_2 SELECT * FROM reference_table_1;

SET ROLE postgres;
SET client_min_messages TO ERROR;
DROP SCHEMA adv_lock_permission CASCADE;
DROP ROLE user_1;
