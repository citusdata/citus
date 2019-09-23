-- Test passing off CALL to mx workers

-- Create worker-local tables to test procedure calls were routed

set citus.shard_replication_factor to 1;
set citus.replication_model to 'streaming';

create schema multi_mx_call;
set search_path to multi_mx_call, public;

create table mx_call_dist_table(id int, val int);
select create_distributed_table('mx_call_dist_table', 'id');
insert into mx_call_dist_table values (3,1),(4,5),(9,2),(6,5),(3,5);
create table mx_call_dist_table2(id int, val int);
select create_distributed_table('mx_call_dist_table2', 'id');
insert into mx_call_dist_table2 values (1,1),(1,2),(2,2),(3,3),(3,4);

create type mx_call_enum as enum ('A', 'S', 'D', 'F');
create table mx_call_dist_table_enum(id int, key mx_call_enum);
select create_distributed_table('mx_call_dist_table_enum', 'key');
insert into mx_call_dist_table_enum values (1,'S'),(2,'A'),(3,'D'),(4,'F');


CREATE PROCEDURE mx_call_dist_object(procname text, tablerelid regclass, argument_index int) LANGUAGE plpgsql AS $$
BEGIN
    update citus.pg_dist_object
    set distribution_argument_index = argument_index, colocationid = pg_dist_partition.colocationid
    from pg_proc, pg_dist_partition
    where proname = procname and oid = objid and pg_dist_partition.logicalrelid = tablerelid;
END;$$;

CREATE PROCEDURE mx_call_proc(x int, INOUT y int) LANGUAGE plpgsql AS $$
BEGIN
    y := x + (select case groupid when 0 then 1 else 0 end from pg_dist_local_group);
    y := y + (select sum(t1.val + t2.val) from mx_call_dist_table t1 join mx_call_dist_table2 t2 on t1.id = t2.id);
END;$$;

CREATE PROCEDURE mx_call_proc_asdf(INOUT x mx_call_enum, INOUT y mx_call_enum) LANGUAGE plpgsql AS $$
BEGIN
    y := x;
    x := (select case groupid when 0 then 'F' else 'S' end from pg_dist_local_group);
END;$$;

CREATE PROCEDURE mx_call_proc_commit(x int) LANGUAGE plpgsql AS $$
BEGIN
    insert into mx_call_dist_table values (100 + x, 1);
    ROLLBACK;
    insert into mx_call_dist_table values (100 + x, 2);
    COMMIT;
    insert into mx_call_dist_table values (101 + x, 1);
    ROLLBACK;
END;$$;

CREATE PROCEDURE mx_call_proc_raise(x int) LANGUAGE plpgsql AS $$
BEGIN
    RAISE WARNING 'warning';
    RAISE EXCEPTION 'error';
END;$$;

CREATE FUNCTION mx_call_add(int, int) RETURNS int
    AS 'select $1 + $2;' LANGUAGE SQL IMMUTABLE;

-- Test that undistributed procedures have no issue executing
call mx_call_proc(2, 0);
call mx_call_proc_asdf('S', 'A');

-- Test some straight forward distributed calls
select create_distributed_function('mx_call_add(int,int)');
select create_distributed_function('mx_call_proc(int,int)');
select create_distributed_function('mx_call_proc_commit(int)');
select create_distributed_function('mx_call_proc_raise(int)');
select create_distributed_function('mx_call_proc_asdf(mx_call_enum,mx_call_enum)');
call mx_call_dist_object('mx_call_proc', 'mx_call_dist_table'::regclass, 1);
call mx_call_dist_object('mx_call_proc_commit', 'mx_call_dist_table'::regclass, 0);
call mx_call_dist_object('mx_call_proc_raise', 'mx_call_dist_table'::regclass, 0);
call mx_call_dist_object('mx_call_proc_asdf', 'mx_call_dist_table_enum'::regclass, 1);

call mx_call_proc(2, 0);
call mx_call_proc_asdf('S', 'A');

drop table mx_call_dist_table_enum;
set client_min_messages to DEBUG2;

-- Procedure calls can't be distributed without a distributed table to guide process
call mx_call_proc_asdf('S', 'A');

-- We don't allow distributing calls inside transactions
begin;
call mx_call_proc(2, 0);
commit;
call mx_call_proc_raise(2);
call mx_call_proc_commit(2);
select id, val from mx_call_dist_table where id >= 100 order by id, val;

-- Make sure we do bounds checking on distributed argument index
-- This also tests that we have cache invalidation for pg_dist_object updates
call mx_call_dist_object('mx_call_proc', 'mx_call_dist_table'::regclass, -1);
call mx_call_proc(2, 0);
call mx_call_dist_object('mx_call_proc', 'mx_call_dist_table'::regclass, 2);
call mx_call_proc(2, 0);
call mx_call_dist_object('mx_call_proc', 'mx_call_dist_table'::regclass, 1);

-- test non Const distribution parameter
call mx_call_proc(2, mx_call_add(3, 4));

-- non const parameter can be pushed down
call mx_call_proc(mx_call_add(3, 4), 2);

-- volatile parameter cannot be pushed down
call mx_call_proc(random()::int, 2);

reset client_min_messages;
reset citus.shard_replication_factor;
reset citus.replication_model;

drop table mx_call_dist_table;
drop table mx_call_dist_table2;
drop function mx_call_add;
drop procedure mx_call_dist_object;
drop procedure mx_call_proc;
drop procedure mx_call_proc_asdf;
drop procedure mx_call_proc_raise;
drop procedure mx_call_proc_commit;
drop type mx_call_enum;

drop schema multi_mx_call cascade;

