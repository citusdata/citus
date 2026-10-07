// Recovery must use the group ID it read together with the worker list, even if
// the group ID changes while it is connecting to the workers. Otherwise it would
// treat prepared transactions named after the new group ID as its own, find no
// pg_dist_transaction records for them, and roll them back.

setup
{
    SELECT success FROM master_run_on_worker(
        ARRAY['localhost']::text[], ARRAY[57637]::int[],
        ARRAY['BEGIN; SELECT 1; PREPARE TRANSACTION ''citus_99999_1_999999999_1''']::text[],
        false);
}

teardown
{
    UPDATE pg_dist_local_group SET groupid = 0;
    SELECT success FROM master_run_on_worker(
        ARRAY['localhost']::text[], ARRAY[57637]::int[],
        ARRAY['ROLLBACK PREPARED ''citus_99999_1_999999999_1''']::text[],
        false);
}

session "s1"

step "s1-block-connections"
{
    BEGIN;
    LOCK TABLE pg_dist_authinfo IN ACCESS EXCLUSIVE MODE;
}

step "s1-change-group-id"
{
    UPDATE pg_dist_local_group SET groupid = 99999;
}

step "s1-commit"
{
    COMMIT;
}

step "s1-check-prepared-transaction"
{
    SELECT result FROM master_run_on_worker(
        ARRAY['localhost']::text[], ARRAY[57637]::int[],
        ARRAY['SELECT count(*) FROM pg_prepared_xacts WHERE gid = ''citus_99999_1_999999999_1''']::text[],
        false);
}

session "s2"

setup
{
    SET citus.max_cached_conns_per_worker = 0;
}

step "s2-recover"
{
    SELECT recover_prepared_transactions();
}

// Change the group ID while recovery waits for authentication metadata to connect.
permutation "s1-block-connections" "s2-recover" "s1-change-group-id" "s1-commit" "s1-check-prepared-transaction"
