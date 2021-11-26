-- Make sure changes citus.node_conninfo shutdown connections with old settings
CREATE SCHEMA node_conninfo_reload;
SET search_path TO node_conninfo_reload;
SET citus.shard_count TO 4;
SET citus.shard_replication_factor TO 1;
SET citus.force_max_query_parallelization TO ON;
SET citus.next_shard_id TO 278000;

create table test(a int);
select create_distributed_table('test', 'a');

-- Make sure a connection is opened and cached
select count(*) from test where a = 0;
show citus.node_conninfo;

-- Set sslmode to something that does not work when connecting
ALTER SYSTEM SET citus.node_conninfo = 'sslmode=doesnotexist';
select pg_reload_conf();
select pg_sleep(0.1); -- wait for config reload to apply
show citus.node_conninfo;

-- Should give a connection error because of bad sslmode
select count(*) from test where a = 0;

-- Reset it again
ALTER SYSTEM RESET citus.node_conninfo;
select pg_reload_conf();
select pg_sleep(0.1); -- wait for config reload to apply
show citus.node_conninfo;

-- Should work again
select count(*) from test where a = 0;
ALTER SYSTEM SET citus.node_conninfo = 'sslmode=doesnotexist';

BEGIN;
-- Should still work (no SIGHUP yet);
select count(*) from test where a = 0;
select pg_reload_conf();
select pg_sleep(0.1); -- wait for config reload to apply
show citus.node_conninfo;
-- Should work since a connection was already taken from pool for this shard,
-- since the same placement is accessed it will reuse that connection for this
-- query
select count(*) from test where a = 0;
COMMIT;
-- Should fail now with connection error, when transaction is finished
select count(*) from test where a = 0;
-- Reset it again
ALTER SYSTEM RESET citus.node_conninfo;
select pg_reload_conf();
select pg_sleep(0.1); -- wait for config reload to apply
show citus.node_conninfo;
-- Should work again
select count(*) from test where a = 0;
ALTER SYSTEM SET citus.node_conninfo = 'sslmode=doesnotexist';
BEGIN;
-- Should still work (no SIGHUP yet);
INSERT INTO test VALUES(0);
select pg_reload_conf();
select pg_sleep(0.1); -- wait for config reload to apply
show citus.node_conninfo;
-- Should work since a connection was already taken from pool for this shard,
-- since the same placement is accessed it will reuse that connection for this
-- query
select count(*) from test where a = 0;
COMMIT;
-- Should fail now, when transaction is finished
select count(*) from test where a = 0;
-- Reset it again
ALTER SYSTEM RESET citus.node_conninfo;
select pg_reload_conf();
select pg_sleep(0.1); -- wait for config reload to apply
show citus.node_conninfo;

-- Should work again
select count(*) from test where a = 0;

ALTER SYSTEM SET citus.node_conninfo = 'sslmode=doesnotexist';
BEGIN;
-- Should still work (no SIGHUP yet);
INSERT INTO test VALUES(1);
select pg_reload_conf();
select pg_sleep(0.1); -- wait for config reload to apply
show citus.node_conninfo;
-- Should fail since a different shard is accessed and thus a new connection
-- will to be created.
select count(*) from test where a = 0;
COMMIT;

-- Should still fail now, when transaction is finished
select count(*) from test where a = 0;

-- Reset it again
ALTER SYSTEM RESET citus.node_conninfo;
select pg_reload_conf();
select pg_sleep(0.1); -- wait for config reload to apply
show citus.node_conninfo;

-- Should work again
select count(*) from test where a = 0;

ALTER SYSTEM SET citus.node_conninfo = 'sslmode=doesnotexist';
BEGIN;
-- Should still work (no SIGHUP yet);
TRUNCATE test;
select pg_reload_conf();
select pg_sleep(0.1); -- wait for config reload to apply
show citus.node_conninfo;
-- Should work since truncate grabbed connections for all shards and these are
-- reused
select count(*) from test;
COMMIT;

-- Should fail now, when transaction is finished
select count(*) from test;

-- Reset it again
ALTER SYSTEM RESET citus.node_conninfo;
select pg_reload_conf();
select pg_sleep(0.1); -- wait for config reload to apply
show citus.node_conninfo;

-- Should work again
select count(*) from test where a = 0;

ALTER SYSTEM SET citus.node_conninfo = 'sslmode=doesnotexist';
BEGIN;
-- Should still work (no SIGHUP yet);
TRUNCATE test;
select pg_reload_conf();
select pg_sleep(0.1); -- wait for config reload to apply
show citus.node_conninfo;
-- Should fail because of divede by 0 on the coordinator.
select count(*)/0 from test;
ROLLBACK;

-- Should fail now, when transaction is finished
select count(*) from test;

-- Reset it again
ALTER SYSTEM RESET citus.node_conninfo;
select pg_reload_conf();
select pg_sleep(0.1); -- wait for config reload to apply
show citus.node_conninfo;

-- Should work again
select count(*) from test where a = 0;
-- Set sslmode to something that does work when connecting
ALTER SYSTEM SET citus.node_conninfo = 'sslmode=allow';
select pg_reload_conf();
select pg_sleep(0.1); -- wait for config reload to apply
show citus.node_conninfo;

-- Should still work, since sslmode=allow is valid
select count(*) from test where a = 0;

-- Set sslmode to the same again (to get more coverage)
ALTER SYSTEM SET citus.node_conninfo = 'sslmode=allow';
select pg_reload_conf();
select pg_sleep(0.1); -- wait for config reload to apply
show citus.node_conninfo;

-- Should still work
select count(*) from test where a = 0;

-- Reset it again
ALTER SYSTEM RESET citus.node_conninfo;
select pg_reload_conf();
select pg_sleep(0.1); -- wait for config reload to apply
show citus.node_conninfo;

-- Should work
select count(*) from test where a = 0;

-- Test connecting all the shards
ALTER SYSTEM SET citus.node_conninfo = 'sslmode=doesnotexist';
BEGIN;
ALTER TABLE test ADD COLUMN b INT;
select pg_reload_conf();
select pg_sleep(0.1); -- wait for config reload to apply
show citus.node_conninfo;
-- Should work since connections to the same shards that BEGIN is sent
-- are reused.
ALTER TABLE test ADD COLUMN c INT;
COMMIT;

-- Should fail now, when transaction is finished
ALTER TABLE test ADD COLUMN d INT;

-- Reset it again
ALTER SYSTEM RESET citus.node_conninfo;
select pg_reload_conf();
select pg_sleep(0.1); -- wait for config reload to apply
show citus.node_conninfo;

-- Should work again
ALTER TABLE test ADD COLUMN e INT;

DROP SCHEMA node_conninfo_reload CASCADE;
