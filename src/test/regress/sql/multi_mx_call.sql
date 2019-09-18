-- Test passing off CALL to mx workers

-- Create worker-local tables to test procedure calls were routed

set citus.shard_replication_factor to 1;

SELECT run_command_on_workers($$
create table mx_call_table (val int);
insert into mx_call_table values (2);
$$);

CREATE TABLE mx_call_dist_table(id int);
select create_distributed_table('mx_call_dist_table', 'id');

CREATE PROCEDURE mx_call_proc(x int, INOUT y int) LANGUAGE plpgsql AS $$
BEGIN
    y := x + (select val from mx_call_table);
END;
$$;

select create_distributed_function('mx_call_proc(int,int)');
update citus.pg_dist_object
set distribution_argument_index = 1, colocationid = pg_dist_partition.colocationid
from pg_proc, pg_dist_partition
where proname = 'mx_call_proc' and oid = objid and pg_dist_partition.logicalrelid = 'mx_call_dist_table'::regclass;

call mx_call_proc(2, 0);

SELECT run_command_on_workers($$
drop table mx_call_table;
$$);

DROP TABLE mx_call_dist_table;
DROP PROCEDURE mx_call_proc;
reset citus.shard_replication_factor;
