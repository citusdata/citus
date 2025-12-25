
-- in order to make the enterprise and community
-- tests outputs the same, disable enable_ddl_propagation
-- and create the roles/schema manually
SET citus.enable_ddl_propagation TO OFF;
CREATE SCHEMA "Update Colocation";
SET client_min_messages TO ERROR;
CREATE ROLE mx_update_colocation WITH LOGIN;
GRANT ALL ON SCHEMA "Update Colocation" TO mx_update_colocation;

\c - - - :worker_1_port
SET citus.enable_ddl_propagation TO OFF;
CREATE SCHEMA "Update Colocation";
SET client_min_messages TO ERROR;
CREATE ROLE mx_update_colocation WITH LOGIN;
GRANT ALL ON SCHEMA "Update Colocation" TO mx_update_colocation;

\c - - - :worker_2_port
SET citus.enable_ddl_propagation TO OFF;
CREATE SCHEMA "Update Colocation";
SET client_min_messages TO ERROR;
CREATE ROLE mx_update_colocation WITH LOGIN;
GRANT ALL ON SCHEMA "Update Colocation" TO mx_update_colocation;
\c - mx_update_colocation - :master_port
SET citus.shard_replication_factor TO 1;
SET search_path TO "Update Colocation";

CREATE TABLE t1(a int);
CREATE TABLE t2(a int);
SELECT create_distributed_table('t1', 'a', colocate_with:='none');
SELECT create_distributed_table('t2', 'a', colocate_with:='none');
SELECT update_distributed_table_colocation('t1', 't2');

-- show that we successfuly updated the colocationids to the same value
SELECT count(DISTINCT colocationid) FROM pg_dist_partition WHERE logicalrelid IN ('t1'::regclass, 't2'::regclass);
\c - - - :worker_1_port
SET search_path TO "Update Colocation";
SELECT count(DISTINCT colocationid) FROM pg_dist_partition WHERE logicalrelid IN ('t1'::regclass, 't2'::regclass);
\c - - - :worker_2_port
SET search_path TO "Update Colocation";
SELECT count(DISTINCT colocationid) FROM pg_dist_partition WHERE logicalrelid IN ('t1'::regclass, 't2'::regclass);

\c - - - :master_port
SET search_path TO "Update Colocation";
SELECT update_distributed_table_colocation('t1', 'none');

-- show that we successfuly updated the colocationids different values
SELECT count(DISTINCT colocationid) FROM pg_dist_partition WHERE logicalrelid IN ('t1'::regclass, 't2'::regclass);
\c - - - :worker_1_port
SET search_path TO "Update Colocation";
SELECT count(DISTINCT colocationid) FROM pg_dist_partition WHERE logicalrelid IN ('t1'::regclass, 't2'::regclass);
\c - - - :worker_2_port
SET search_path TO "Update Colocation";
SELECT count(DISTINCT colocationid) FROM pg_dist_partition WHERE logicalrelid IN ('t1'::regclass, 't2'::regclass);

\c - - - :master_port
SET search_path TO "Update Colocation";

create table test_a_tbl_1(text_col text collate "C" unique);
create table test_a_tbl_2(text_col text collate "en-x-icu" unique);

select create_distributed_table('test_a_tbl_1', 'text_col');
select create_distributed_table('test_a_tbl_2', 'text_col');

-- make sure we assign them to different colocation groups
select result as colocationids_different from run_command_on_all_nodes($$
    select count(*) = 2 from (
        select distinct(colocationid)
        from pg_dist_partition
        join pg_class on (logicalrelid = pg_class.oid)
        join pg_namespace on (relnamespace = pg_namespace.oid)
        join pg_dist_colocation using (colocationid)
        where pg_namespace.nspname = 'Update Colocation'
          and pg_class.relname in ('test_a_tbl_1', 'test_a_tbl_2')
    ) q;
$$);

DROP TABLE test_a_tbl_1, test_a_tbl_2;

create table test_d_tbl_1(text_col text collate "C" unique);
create table test_d_tbl_2(text_col text collate "en-x-icu" unique);

select create_distributed_table('test_d_tbl_1', 'text_col', shard_count=>4);
select create_distributed_table('test_d_tbl_2', 'text_col', shard_count=>6);
select alter_distributed_table('test_d_tbl_2', shard_count=>4);

-- make sure we assign them to different colocation groups
select result as colocationids_different from run_command_on_all_nodes($$
    select count(*) = 2 from (
        select distinct(colocationid)
        from pg_dist_partition
        join pg_class on (logicalrelid = pg_class.oid)
        join pg_namespace on (relnamespace = pg_namespace.oid)
        join pg_dist_colocation using (colocationid)
        where pg_namespace.nspname = 'Update Colocation'
          and pg_class.relname in ('test_d_tbl_1', 'test_d_tbl_2')
    ) q;
$$);

DROP TABLE test_d_tbl_1, test_d_tbl_2;

create table test_b_tbl_1(text_col text collate "C" unique);
create table test_b_tbl_2(text_col text collate "en-x-icu" unique);

select create_distributed_table('test_b_tbl_1', 'text_col');

-- errors
select create_distributed_table('test_b_tbl_2', 'text_col', colocate_with => 'test_b_tbl_1');

DROP TABLE test_b_tbl_1, test_b_tbl_2;

create table test_c_tbl_1(text_col text collate "C" unique);
create table test_c_tbl_2(text_col text collate "en-x-icu" unique);

select create_distributed_table('test_c_tbl_1', 'text_col');
select create_distributed_table('test_c_tbl_2', 'text_col', colocate_with => 'none');

-- errors
select alter_distributed_table('test_c_tbl_2', colocate_with=>'test_c_tbl_1');

create table test_c_tbl_3(int_col int, text_col text collate "C");
create table test_c_tbl_4(text_col text collate "en-x-icu");

select create_distributed_table('test_c_tbl_3', 'int_col');
select create_distributed_table('test_c_tbl_4', 'text_col', colocate_with => 'none');

-- errors
select alter_distributed_table('test_c_tbl_3', colocate_with=>'test_c_tbl_4', distribution_column:='text_col');

DROP TABLE test_c_tbl_1, test_c_tbl_2;

\c - postgres - :master_port
SET client_min_messages TO ERROR;
DROP SCHEMA "Update Colocation" cascade;

SET citus.enable_ddl_propagation TO OFF;
DROP ROLE mx_update_colocation;

\c - postgres - :worker_1_port
SET citus.enable_ddl_propagation TO OFF;
DROP ROLE mx_update_colocation;

\c - postgres - :worker_2_port
SET citus.enable_ddl_propagation TO OFF;
DROP ROLE mx_update_colocation;
