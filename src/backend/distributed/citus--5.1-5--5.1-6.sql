CREATE OR REPLACE FUNCTION pg_catalog.worker_apply_shard_ddl_command(bigint, text)
    RETURNS void
    LANGUAGE sql
AS $worker_apply_shard_ddl_command$
    SELECT pg_catalog.worker_apply_shard_ddl_command($1, 'public', $2);
$worker_apply_shard_ddl_command$;
COMMENT ON FUNCTION worker_apply_shard_ddl_command(bigint, text)
    IS 'extend ddl command with shardId and apply on database';

CREATE OR REPLACE FUNCTION pg_catalog.worker_fetch_foreign_file(text, bigint, text[], integer[])
    RETURNS void
    LANGUAGE sql
AS $worker_fetch_foreign_file$
    SELECT pg_catalog.worker_fetch_foreign_file('public', $1, $2, $3, $4);
$worker_fetch_foreign_file$;
COMMENT ON FUNCTION pg_catalog.worker_fetch_foreign_file(text, bigint, text[], integer[])
    IS 'fetch foreign file from remote node and apply file';

CREATE OR REPLACE FUNCTION pg_catalog.worker_fetch_regular_table(text, bigint, text[], integer[])
    RETURNS void
    LANGUAGE sql
AS $worker_fetch_regular_table$
    SELECT pg_catalog.worker_fetch_regular_table('public', $1, $2, $3, $4);
$worker_fetch_regular_table$;
COMMENT ON FUNCTION pg_catalog.worker_fetch_regular_table(text, bigint, text[], integer[])
    IS 'fetch PostgreSQL table from remote node';
