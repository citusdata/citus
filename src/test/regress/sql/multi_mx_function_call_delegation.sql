-- Test passing off "SELECT func(...)" to mx workers

create schema multi_mx_select_func;
set search_path to multi_mx_select_func, public;

set citus.shard_replication_factor to 2;
set citus.replication_model to 'statement';

-- This table requires specific settings, create before getting into things
create table mx_call_dist_table_replica(id int, val int);
SELECT create_distributed_table('mx_call_dist_table_replica', 'id');
insert into mx_call_dist_table_replica values (9,1),(8,2),(7,3),(6,4),(5,5);

set citus.shard_replication_factor to 1;
set citus.replication_model to 'streaming';

--
-- Create tables and procedures we want to use in tests
--
create table mx_func_dist_table_1(id int, val int);
SELECT create_distributed_table('mx_func_dist_table_1', 'id');
insert into mx_func_dist_table_1 values (3,1),(4,5),(9,2),(6,5),(3,5);

create table mx_func_dist_table_2(id int, val int);
SELECT create_distributed_table('mx_func_dist_table_2', 'id');
insert into mx_func_dist_table_2 values (1,1),(1,2),(2,2),(3,3),(3,4);

create table mx_func_dist_table_ref(id int, val int);
SELECT create_reference_table('mx_func_dist_table_ref');
insert into mx_func_dist_table_ref values (2,7),(1,8),(2,8),(1,8),(2,8);

create type mx_func_enum as enum ('A', 'S', 'D', 'F');
create table mx_func_dist_table_enum(id int, key mx_func_enum);
SELECT create_distributed_table('mx_func_dist_table_enum', 'key');
insert into mx_func_dist_table_enum values (1,'S'),(2,'A'),(3,'D'),(4,'F');


CREATE FUNCTION mx_dist_func(x int, y int)
RETURNS INT
LANGUAGE plpgsql AS $$
BEGIN
    -- groupid is 0 in coordinator and non-zero in workers, so by using it here
    -- we make sure the function is being executed in the worker.
    y := x + (SELECT case groupid when 0 then 1 else 0 end from pg_dist_local_group);
    -- we also make sure that we can run distributed queries in the functions
    -- that are routed to the workers.
    y := y + (SELECT sum(t1.val + t2.val) from multi_mx_select_func.mx_func_dist_table_1 t1 join multi_mx_select_func.mx_func_dist_table_2 t2 on t1.id = t2.id);

    RETURN y;
END;$$;

-- create another function which verifies:
-- 1. we work fine with returning records
-- 2. we work fine in combination with custom types
CREATE FUNCTION mx_dist_func_custom_types(INOUT x mx_func_enum, INOUT y mx_func_enum)
RETURNS RECORD
LANGUAGE plpgsql AS $$
BEGIN
    y := x;
    x := (SELECT case groupid when 0 then 'F' else 'S' end from pg_dist_local_group);
END;$$;

-- Test that undistributed functions have no issue executing
SELECT multi_mx_select_func.mx_dist_func(2, 0);
SELECT multi_mx_select_func.mx_dist_func_custom_types('S', 'A');

-- Mark both functions as distributed ...
SELECT create_distributed_function('mx_dist_func(int,int)');
SELECT create_distributed_function('mx_dist_func_custom_types(mx_func_enum,mx_func_enum)');

-- We still don't route them to the workers, because they aren't
-- colocated with any distributed tables.
SET client_min_messages TO DEBUG1;
SELECT multi_mx_select_func.mx_dist_func(2, 0);
SELECT multi_mx_select_func.mx_dist_func_custom_types('S', 'A');

-- Mark them as colocated with a table. Now we should route them to workers.
CALL colocate_proc_with_table('mx_dist_func', 'mx_func_dist_table_1'::regclass, 1);
CALL colocate_proc_with_table('mx_dist_func_custom_types', 'mx_func_dist_table_enum'::regclass, 1);
SELECT multi_mx_select_func.mx_dist_func(2, 0);
SELECT multi_mx_select_func.mx_dist_func_custom_types('S', 'A');

-- We don't route function calls inside transactions.
begin;
SELECT multi_mx_select_func.mx_dist_func(2, 0);
commit;

-- Drop the table colocated with mx_dist_func_custom_types. Now it shouldn't
-- be routed to workers anymore.
SET client_min_messages TO NOTICE;
DROP TABLE mx_func_dist_table_enum;
SET client_min_messages TO DEBUG1;
SELECT multi_mx_select_func.mx_dist_func_custom_types('S', 'A');

-- Make sure we do bounds checking on distributed argument index
-- This also tests that we have cache invalidation for pg_dist_object updates
CALL colocate_proc_with_table('mx_dist_func', 'mx_func_dist_table_1'::regclass, -1);
SELECT multi_mx_select_func.mx_dist_func(2, 0);
CALL colocate_proc_with_table('mx_dist_func', 'mx_func_dist_table_1'::regclass, 2);
SELECT multi_mx_select_func.mx_dist_func(2, 0);

-- We don't currently support routing functions colocated with reference tables
CALL colocate_proc_with_table('mx_dist_func', 'mx_func_dist_table_ref'::regclass, 1);
SELECT multi_mx_select_func.mx_dist_func(2, 0);

-- We don't currently support routing functions colocated with replicated tables
CALL colocate_proc_with_table('mx_dist_func', 'mx_call_dist_table_replica'::regclass, 1);
SELECT multi_mx_select_func.mx_dist_func(2, 0);
SET client_min_messages TO NOTICE;
DROP TABLE mx_call_dist_table_replica;
SET client_min_messages TO DEBUG1;

CALL colocate_proc_with_table('mx_dist_func', 'mx_func_dist_table_1'::regclass, 1);

-- Test that we properly propagate errors raised from functions.
CREATE FUNCTION mx_call_proc_raise(x int)
RETURNS VOID
LANGUAGE plpgsql AS $$
BEGIN
    RAISE WARNING 'warning';
    RAISE EXCEPTION 'error';
END;$$;
SELECT create_distributed_function('mx_call_proc_raise(int)', '$1', 'mx_func_dist_table_1');
SELECT mx_call_proc_raise(2);


-- Test that we don't propagate to non-metadata worker nodes
SELECT stop_metadata_sync_to_node('localhost', :worker_1_port);
SELECT stop_metadata_sync_to_node('localhost', :worker_2_port);
SELECT multi_mx_select_func.mx_dist_func(2, 0);
SET client_min_messages TO NOTICE;
SELECT start_metadata_sync_to_node('localhost', :worker_1_port);
SELECT start_metadata_sync_to_node('localhost', :worker_2_port);
SET client_min_messages TO DEBUG1;

--
-- Test non-const parameter values
--
CREATE FUNCTION mx_call_add(int, int) RETURNS int
    AS 'SELECT $1 + $2;' LANGUAGE SQL IMMUTABLE;
SELECT create_distributed_function('mx_call_add(int,int)', '$1');

-- non-const distribution parameters cannot be pushed down
SELECT multi_mx_select_func.mx_dist_func(2, mx_call_add(3, 4));

-- non-const parameter can be pushed down
SELECT multi_mx_select_func.mx_dist_func(multi_mx_select_func.mx_call_add(3, 4), 2);

-- volatile parameter cannot be pushed down
SELECT multi_mx_select_func.mx_dist_func(floor(random())::int, 2);

reset client_min_messages;
\set VERBOSITY terse

DROP SCHEMA multi_mx_select_func cascade;

