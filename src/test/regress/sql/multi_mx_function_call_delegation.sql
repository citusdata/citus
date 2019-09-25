-- Test passing off function call to mx workers
-- Create worker-local tables to test function calls were routed
CREATE SCHEMA multi_mx_function_call_delegation;
SET search_path TO multi_mx_function_call_delegation, public;

SET citus.shard_replication_factor TO 2;
SET citus.replication_model TO 'statement';

-- This table requires specific settings, create before getting into things
create table mx_call_dist_table_replica(id int, val int);
select create_distributed_table('mx_call_dist_table_replica', 'id');
insert into mx_call_dist_table_replica values (9,1),(8,2),(7,3),(6,4),(5,5);

SET citus.shard_replication_factor TO 1;
SET citus.replication_model TO 'streaming';

--
-- Create tables and functions we want to use in tests
--
create table mx_call_dist_table_1(id int, val int);
select create_distributed_table('mx_call_dist_table_1', 'id');
insert into mx_call_dist_table_1 values (3,1),(4,5),(9,2),(6,5),(3,5);

create table mx_call_dist_table_2(id int, val int);
select create_distributed_table('mx_call_dist_table_2', 'id');
insert into mx_call_dist_table_2 values (1,1),(1,2),(2,2),(3,3),(3,4);

create table mx_call_dist_table_ref(id int, val int);
select create_reference_table('mx_call_dist_table_ref');
insert into mx_call_dist_table_ref values (2,7),(1,8),(2,8),(1,8),(2,8);

create type mx_call_enum as enum ('A', 'S', 'D', 'F');
create table mx_call_dist_table_enum(id int, key mx_call_enum);
select create_distributed_table('mx_call_dist_table_enum', 'key');
insert into mx_call_dist_table_enum values (1,'S'),(2,'A'),(3,'D'),(4,'F');


CREATE FUNCTION mx_call_func(x int, INOUT y int)
LANGUAGE plpgsql AS $$
BEGIN
    -- groupid is 0 in coordinator and non-zero in workers, so by using it here
    -- we make sure the function is being executed in the worker.
    y := x + (select case groupid when 0 then 1 else 0 end from pg_dist_local_group);
    -- we also make sure that we can run distributed queries in the functions
    -- that are routed to the workers.
    y := y + (select sum(t1.val + t2.val) from multi_mx_function_call_delegation.mx_call_dist_table_1 t1 join multi_mx_function_call_delegation.mx_call_dist_table_2 t2 on t1.id = t2.id);
END;$$;

-- create another function which verifies:
-- 1. we work fine with multiple return columns
-- 2. we work fine in combination with custom types
CREATE FUNCTION mx_call_func_custom_types(INOUT x mx_call_enum, INOUT y mx_call_enum)
LANGUAGE plpgsql AS $$
BEGIN
    y := x;
    x := (select case groupid when 0 then 'F' else 'S' end from pg_dist_local_group);
END;$$;

-- Test that undistributed functions have no issue executing
select multi_mx_function_call_delegation.mx_call_func(2, 0);
select multi_mx_function_call_delegation.mx_call_func_custom_types('S', 'A');

-- Same for unqualified names
select mx_call_func(2, 0);
select mx_call_func_custom_types('S', 'A');

-- Mark both functions as distributed ...
select create_distributed_function('mx_call_func(int,int)');
select create_distributed_function('mx_call_func_custom_types(mx_call_enum,mx_call_enum)');

-- We still don't route them to the workers, because they aren't
-- colocated with any distributed tables.
SET client_min_messages TO DEBUG1;
select mx_call_func(2, 0);
select mx_call_func_custom_types('S', 'A');

-- Mark them as colocated with a table. Now we should route them to workers.
select colocate_proc_with_table('mx_call_func', 'mx_call_dist_table_1'::regclass, 1);
select colocate_proc_with_table('mx_call_func_custom_types', 'mx_call_dist_table_enum'::regclass, 1);

select mx_call_func(2, 0);
select mx_call_func_custom_types('S', 'A');
select multi_mx_function_call_delegation.mx_call_func(2, 0);
select multi_mx_function_call_delegation.mx_call_func_custom_types('S', 'A');

-- We don't allow distributing calls inside transactions
begin;
select mx_call_func(2, 0);
commit;

-- Drop the table colocated with mx_call_func_custom_types. Now it shouldn't
-- be routed to workers anymore.
SET client_min_messages TO NOTICE;
drop table mx_call_dist_table_enum;
SET client_min_messages TO DEBUG1;
select mx_call_func_custom_types('S', 'A');

-- Make sure we do bounds checking on distributed argument index
-- This also tests that we have cache invalidation for pg_dist_object updates
select colocate_proc_with_table('mx_call_func', 'mx_call_dist_table_1'::regclass, -1);
select mx_call_func(2, 0);
select colocate_proc_with_table('mx_call_func', 'mx_call_dist_table_1'::regclass, 2);
select mx_call_func(2, 0);

-- We don't currently support colocating with reference tables
select colocate_proc_with_table('mx_call_func', 'mx_call_dist_table_ref'::regclass, 1);
select mx_call_func(2, 0);

-- We don't currently support colocating with replicated tables
select colocate_proc_with_table('mx_call_func', 'mx_call_dist_table_replica'::regclass, 1);
select mx_call_func(2, 0);
SET client_min_messages TO NOTICE;
drop table mx_call_dist_table_replica;
SET client_min_messages TO DEBUG1;

select colocate_proc_with_table('mx_call_func', 'mx_call_dist_table_1'::regclass, 1);

-- Test table returning functions.
CREATE FUNCTION mx_call_func_tbl(x int)
RETURNS TABLE (p0 int, p1 int)
LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO multi_mx_function_call_delegation.mx_call_dist_table_1 VALUES (x, -1), (x+1, 4);
    UPDATE multi_mx_function_call_delegation.mx_call_dist_table_1 SET val = val+1 WHERE id >= x;
    UPDATE multi_mx_function_call_delegation.mx_call_dist_table_1 SET val = val-1 WHERE id >= x;
    RETURN QUERY
        SELECT id, val
        FROM multi_mx_function_call_delegation.mx_call_dist_table_1 t
        WHERE id >= x
        ORDER BY 1, 2;
END;$$;

-- before distribution ...
select mx_call_func_tbl(10);
-- after distribution ...
select create_distributed_function('mx_call_func_tbl(int)', '$1', 'mx_call_dist_table_1');
select mx_call_func_tbl(20);

-- Test that we properly propagate errors raised from procedures.
CREATE FUNCTION mx_call_func_raise(x int)
RETURNS void LANGUAGE plpgsql AS $$
BEGIN
    RAISE WARNING 'warning';
    RAISE EXCEPTION 'error';
END;$$;
select create_distributed_function('mx_call_func_raise(int)', '$1', 'mx_call_dist_table_1');
select mx_call_func_raise(2);

-- Test that we don't propagate to non-metadata worker nodes
select stop_metadata_sync_to_node('localhost', :worker_1_port);
select stop_metadata_sync_to_node('localhost', :worker_2_port);
select mx_call_func(2, 0);
SET client_min_messages TO NOTICE;
select start_metadata_sync_to_node('localhost', :worker_1_port);
select start_metadata_sync_to_node('localhost', :worker_2_port);
SET client_min_messages TO DEBUG1;

--
-- Test non-const parameter values
--
CREATE FUNCTION mx_call_add(int, int) RETURNS int
    AS 'select $1 + $2;' LANGUAGE SQL IMMUTABLE;
SELECT create_distributed_function('mx_call_add(int,int)', '$1');

-- non-const distribution parameters cannot be pushed down
select mx_call_func(2, (select x + 1 from mx_call_add(3, 4) x));

-- non-const parameter can be pushed down
select mx_call_func((select x + 1 from mx_call_add(3, 4) x), 2);

-- volatile parameter cannot be pushed down
select mx_call_func(floor(random())::int, 2);

-- test forms we don't distribute
select * from mx_call_func(2, 0);
select mx_call_func(2, 0) from mx_call_dist_table_1;
select mx_call_func(2, 0) where mx_call_func(0, 2) = 0;
select mx_call_func(2, 0), mx_call_func(0, 2);
do $$ begin select mx_call_func(2, 0); END; $$;

RESET client_min_messages;
\set VERBOSITY terse
DROP SCHEMA multi_mx_function_call_delegation CASCADE;
