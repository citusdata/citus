-- Test passing off function call to mx workers
-- Create worker-local tables to test procedure calls were routed
CREATE SCHEMA multi_mx_function_call_delegation;
SET search_path TO multi_mx_function_call_delegation;
SET citus.replication_model TO 'streaming';
SET citus.shard_replication_factor TO 1;

create table mx_call_dist_table(id int, val int);
select create_distributed_table('mx_call_dist_table', 'id');

CREATE FUNCTION mx_call_func(INOUT x int, INOUT y int) RETURNS record LANGUAGE plpgsql AS $$
BEGIN
    y := (select groupid from pg_dist_local_group) + x + y;
END;$$;

CREATE FUNCTION mx_call_func_raise(x int) RETURNS void LANGUAGE plpgsql AS $$
BEGIN
    RAISE WARNING 'warning';
END;$$;

CREATE FUNCTION mx_call_add(int, int) RETURNS int
    AS 'select $1 + $2;' LANGUAGE SQL IMMUTABLE;

CREATE FUNCTION mx_call_dist_object(procname text, tablerelid regclass, argument_index int) RETURNS void LANGUAGE plpgsql AS $$
BEGIN
    update citus.pg_dist_object
    set distribution_argument_index = argument_index, colocationid = pg_dist_partition.colocationid
    from pg_proc, pg_dist_partition
    where proname = procname and oid = objid and pg_dist_partition.logicalrelid = tablerelid;
END;$$;


-- Test that undistributed procedures have no issue executing
select mx_call_func(2, 0);

-- Test some straight forward distributed calls
select create_distributed_function('mx_call_add(int,int)');
select create_distributed_function('mx_call_func(int,int)', 'x', colocate_with := 'mx_call_dist_table');
select create_distributed_function('mx_call_func_raise(int)', '$1');

set client_min_messages to DEBUG;

select mx_call_func(2, 0);
select mx_call_func_raise(2);

-- We don't allow distributing calls inside transactions
begin;
select mx_call_func(2, 0);
commit;

-- Make sure we do bounds checking on distributed argument index
-- This also tests that we have cache invalidation for pg_dist_object updates
select mx_call_dist_object('mx_call_func', 'mx_call_dist_table'::regclass, -1);
select mx_call_func(2, 0);
select mx_call_dist_object('mx_call_func', 'mx_call_dist_table'::regclass, 2);
select mx_call_func(2, 0);
select mx_call_dist_object('mx_call_func', 'mx_call_dist_table'::regclass, 1);

-- test non Const distribution parameter
select mx_call_func(2, mx_call_add(3, 4));

-- non const parameter can be pushed down
select mx_call_func(mx_call_add(3, 4), 2);

-- volatile parameter cannot be pushed down
select mx_call_func(least(random()::int,0), 2);

RESET client_min_messages;
\set VERBOSITY terse
DROP SCHEMA multi_mx_function_call_delegation CASCADE;
