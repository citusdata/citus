-- Test passing off CALL to mx workers

-- Create worker-local tables to test procedure calls were routed

set citus.shard_replication_factor to 1;
set citus.replication_model to 'streaming';

CREATE TABLE mx_call_dist_table(id int);
select create_distributed_table('mx_call_dist_table', 'id');
insert into mx_call_dist_table values (1),(2),(3),(4),(5);

create type mx_call_enum as enum ('A', 'S', 'D', 'F');
CREATE TABLE mx_call_dist_table_enum(id int, key mx_call_enum);

CREATE PROCEDURE mx_call_proc(x int, INOUT y int) LANGUAGE plpgsql AS $$
BEGIN
    y := x + (select case groupid when 0 then 1 else 0 end from pg_dist_local_group);
    y := y + (select sum(id) from mx_call_dist_table);
END;
$$;

CREATE PROCEDURE mx_call_proc_asdf(INOUT x mx_call_enum, INOUT y mx_call_enum) LANGUAGE plpgsql AS $$
BEGIN
    y := x;
    x := (select case groupid when 0 then 'F' else 'S' end from pg_dist_local_group);
END;
$$;

CREATE FUNCTION mx_call_add(int, int) RETURNS int
    AS 'select $1 + $2;'
    LANGUAGE SQL
    IMMUTABLE
    RETURNS NULL ON NULL INPUT;

call mx_call_proc(2, 0);
call mx_call_proc_asdf('S', 'A');

select create_distributed_table('mx_call_dist_table_enum', 'key');

select create_distributed_function('mx_call_proc(int,int)');
update citus.pg_dist_object
set distribution_argument_index = 1, colocationid = pg_dist_partition.colocationid
from pg_proc, pg_dist_partition
where proname = 'mx_call_proc' and oid = objid and pg_dist_partition.logicalrelid = 'mx_call_dist_table'::regclass;

select create_distributed_function('mx_call_proc_asdf(mx_call_enum,mx_call_enum)');
update citus.pg_dist_object
set distribution_argument_index = 1, colocationid = pg_dist_partition.colocationid
from pg_proc, pg_dist_partition
where proname = 'mx_call_proc_asdf' and oid = objid and pg_dist_partition.logicalrelid = 'mx_call_dist_table_enum'::regclass;

call mx_call_proc(2, 0);
call mx_call_proc_asdf('S', 'A');

set client_min_messages to DEBUG2;
begin;
select sum(id) from mx_call_dist_table;
call mx_call_proc(2, 0);
commit;

update citus.pg_dist_object
set distribution_argument_index = -1, colocationid = pg_dist_partition.colocationid
from pg_proc, pg_dist_partition
where proname = 'mx_call_proc' and oid = objid and pg_dist_partition.logicalrelid = 'mx_call_dist_table'::regclass;
call mx_call_proc(2, 0);
update citus.pg_dist_object
set distribution_argument_index = 1, colocationid = pg_dist_partition.colocationid
from pg_proc, pg_dist_partition
where proname = 'mx_call_proc' and oid = objid and pg_dist_partition.logicalrelid = 'mx_call_dist_table'::regclass;

call mx_call_proc(2, mx_call_add(3, 4));

reset client_min_messages;
DROP TABLE mx_call_dist_table;
DROP PROCEDURE mx_call_proc;
reset citus.shard_replication_factor;
reset citus.replication_model;
