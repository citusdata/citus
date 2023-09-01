\set VERBOSITY terse

SET citus.next_shard_id TO 1508000;
SET citus.shard_replication_factor TO 1;
SET citus.enable_local_execution TO ON;

CREATE SCHEMA foreign_tables_schema_mx;
SET search_path TO foreign_tables_schema_mx;


SET client_min_messages to ERROR;

-- ensure that coordinator is added to pg_dist_node
SELECT 1 FROM master_add_node('localhost', :master_port, groupId => 0);

RESET client_min_messages;

-- test adding foreign table to metadata with the guc
SET citus.use_citus_managed_tables TO ON;
CREATE TABLE foreign_table_test (id integer NOT NULL, data text, a bigserial);
INSERT INTO foreign_table_test VALUES (1, 'text_test');
CREATE EXTENSION postgres_fdw;
CREATE SERVER foreign_server
        FOREIGN DATA WRAPPER postgres_fdw
        OPTIONS (host 'localhost', port :'master_port', dbname 'regression');
CREATE USER MAPPING FOR CURRENT_USER
        SERVER foreign_server
        OPTIONS (user 'postgres');
CREATE FOREIGN TABLE foreign_table (
        id integer NOT NULL,
        data text,
        a bigserial
)
        SERVER foreign_server
        OPTIONS (schema_name 'foreign_tables_schema_mx', table_name 'foreign_table_test');

--verify
SELECT partmethod, repmodel FROM pg_dist_partition WHERE logicalrelid = 'foreign_table'::regclass ORDER BY logicalrelid;

-- COPY FROM doesn't work for Citus foreign tables
COPY foreign_table FROM stdin;
1	1foo	2
\.

CREATE TABLE parent_for_foreign_tables (
    project_id integer
) PARTITION BY HASH (project_id);

CREATE SERVER IF NOT EXISTS srv1 FOREIGN DATA WRAPPER postgres_fdw OPTIONS (dbname 'regression', host 'localhost', port :'master_port');
CREATE SERVER IF NOT EXISTS srv2 FOREIGN DATA WRAPPER postgres_fdw OPTIONS (dbname 'regression', host 'localhost', port :'master_port');
CREATE SERVER IF NOT EXISTS srv3 FOREIGN DATA WRAPPER postgres_fdw OPTIONS (dbname 'regression', host 'localhost', port :'master_port');

CREATE FOREIGN TABLE foreign_partition_1 PARTITION OF parent_for_foreign_tables FOR VALUES WITH (modulus 3, remainder 0) SERVER srv1 OPTIONS (table_name 'dummy');
CREATE FOREIGN TABLE foreign_partition_2 PARTITION OF parent_for_foreign_tables FOR VALUES WITH (modulus 3, remainder 1) SERVER srv2 OPTIONS (table_name 'dummy');
CREATE FOREIGN TABLE foreign_partition_3 PARTITION OF parent_for_foreign_tables FOR VALUES WITH (modulus 3, remainder 2) SERVER srv3 OPTIONS (table_name 'dummy');

SELECT partmethod, repmodel FROM pg_dist_partition
    WHERE logicalrelid IN ('parent_for_foreign_tables'::regclass, 'foreign_partition_1'::regclass, 'foreign_partition_2'::regclass, 'foreign_partition_3'::regclass)
    ORDER BY logicalrelid;

ALTER FOREIGN TABLE foreign_table SET SCHEMA public;
ALTER FOREIGN TABLE public.foreign_table RENAME TO foreign_table_newname;
ALTER FOREIGN TABLE public.foreign_table_newname RENAME COLUMN id TO id_test;
ALTER FOREIGN TABLE public.foreign_table_newname ADD dummy_col bigint NOT NULL DEFAULT 1;
ALTER FOREIGN TABLE public.foreign_table_newname ALTER dummy_col DROP DEFAULT;
ALTER FOREIGN TABLE public.foreign_table_newname ALTER dummy_col SET DEFAULT 2;
ALTER FOREIGN TABLE public.foreign_table_newname ALTER dummy_col TYPE int;
ALTER TABLE foreign_table_test RENAME COLUMN id TO id_test;
ALTER TABLE foreign_table_test ADD dummy_col int NOT NULL DEFAULT 1;
INSERT INTO public.foreign_table_newname VALUES (2, 'test_2');
INSERT INTO foreign_table_test VALUES (3, 'test_3');

ALTER FOREIGN TABLE public.foreign_table_newname ADD CONSTRAINT check_c check(id_test < 1000);
ALTER FOREIGN TABLE public.foreign_table_newname DROP constraint check_c;

ALTER FOREIGN TABLE public.foreign_table_newname ADD CONSTRAINT check_c_2 check(id_test < 1000) NOT VALID;
ALTER FOREIGN TABLE public.foreign_table_newname VALIDATE CONSTRAINT check_c_2;
ALTER FOREIGN TABLE public.foreign_table_newname DROP constraint IF EXISTS check_c_2;

-- trigger test
CREATE TABLE table42(value int);

CREATE FUNCTION insert_42() RETURNS trigger AS $insert_42$
BEGIN
    INSERT INTO table42 VALUES (42);
    RETURN NEW;
END;
$insert_42$ LANGUAGE plpgsql;

CREATE TRIGGER insert_42_trigger
AFTER DELETE ON public.foreign_table_newname
FOR EACH ROW EXECUTE FUNCTION insert_42();

-- do the same pattern from the workers as well
INSERT INTO public.foreign_table_newname VALUES (99, 'test_2');
delete from public.foreign_table_newname where id_test = 99;
select * from table42 ORDER BY value;

-- disable trigger
alter foreign table public.foreign_table_newname disable trigger insert_42_trigger;
INSERT INTO public.foreign_table_newname VALUES (99, 'test_2');
delete from public.foreign_table_newname where id_test = 99;
-- should not insert again as trigger disabled
select * from table42 ORDER BY value;

DROP TRIGGER insert_42_trigger ON public.foreign_table_newname;

-- should throw errors
select alter_table_set_access_method('public.foreign_table_newname', 'columnar');
select alter_distributed_table('public.foreign_table_newname', shard_count:=4);

ALTER FOREIGN TABLE public.foreign_table_newname OWNER TO pg_monitor;
SELECT run_command_on_workers($$select r.rolname from pg_roles r join pg_class c on r.oid=c.relowner where relname = 'foreign_table_newname';$$);
ALTER FOREIGN TABLE public.foreign_table_newname OWNER TO postgres;
SELECT run_command_on_workers($$select r.rolname from pg_roles r join pg_class c on r.oid=c.relowner where relname = 'foreign_table_newname';$$);

\c - - - :worker_1_port
SET search_path TO foreign_tables_schema_mx;
SELECT * FROM public.foreign_table_newname ORDER BY id_test;
SELECT * FROM foreign_table_test ORDER BY id_test;
-- should error out
ALTER FOREIGN TABLE public.foreign_table_newname DROP COLUMN id;
SELECT partmethod, repmodel FROM pg_dist_partition
    WHERE logicalrelid IN ('parent_for_foreign_tables'::regclass, 'foreign_partition_1'::regclass, 'foreign_partition_2'::regclass, 'foreign_partition_3'::regclass)
    ORDER BY logicalrelid;
\c - - - :master_port
ALTER FOREIGN TABLE foreign_table_newname RENAME TO foreign_table;
SET search_path TO foreign_tables_schema_mx;
ALTER FOREIGN TABLE public.foreign_table SET SCHEMA foreign_tables_schema_mx;
ALTER FOREIGN TABLE IF EXISTS foreign_table RENAME COLUMN id_test TO id;
ALTER TABLE foreign_table_test RENAME COLUMN id_test TO id;
ALTER FOREIGN TABLE foreign_table DROP COLUMN id;
ALTER FOREIGN TABLE foreign_table DROP COLUMN dummy_col;
ALTER TABLE foreign_table_test DROP COLUMN dummy_col;
ALTER FOREIGN TABLE foreign_table OPTIONS (DROP schema_name, SET table_name 'notable');

SELECT run_command_on_workers($$SELECT f.ftoptions FROM pg_foreign_table f JOIN pg_class c ON f.ftrelid=c.oid WHERE c.relname = 'foreign_table';$$);

ALTER FOREIGN TABLE foreign_table OPTIONS (ADD schema_name 'foreign_tables_schema_mx', SET table_name 'foreign_table_test');
SELECT * FROM foreign_table ORDER BY a;
-- test alter user mapping
ALTER USER MAPPING FOR postgres SERVER foreign_server OPTIONS (SET user 'nonexistiniguser');
-- should fail
SELECT * FROM foreign_table ORDER BY a;
ALTER USER MAPPING FOR postgres SERVER foreign_server OPTIONS (SET user 'postgres');
-- test undistributing
DELETE FROM foreign_table;
SELECT undistribute_table('foreign_table');

-- both should error out
SELECT create_distributed_table('foreign_table','data');
SELECT create_reference_table('foreign_table');

INSERT INTO foreign_table_test VALUES (1, 'testt');
SELECT * FROM foreign_table ORDER BY a;
SELECT * FROM foreign_table_test ORDER BY a;

DROP TABLE parent_for_foreign_tables;

CREATE TABLE parent_for_foreign_tables (id integer NOT NULL, data text, a bigserial)
    PARTITION BY HASH (id);

CREATE FOREIGN TABLE foreign_partition_1 PARTITION OF parent_for_foreign_tables FOR VALUES WITH (modulus 3, remainder 0) SERVER srv1 OPTIONS (schema_name 'foreign_tables_schema_mx', table_name 'foreign_table_test');
CREATE FOREIGN TABLE foreign_partition_2 PARTITION OF parent_for_foreign_tables FOR VALUES WITH (modulus 3, remainder 1) SERVER srv2 OPTIONS (schema_name 'foreign_tables_schema_mx', table_name 'foreign_table_test');

SELECT citus_add_local_table_to_metadata('parent_for_foreign_tables');

CREATE FOREIGN TABLE foreign_partition_3 PARTITION OF parent_for_foreign_tables FOR VALUES WITH (modulus 3, remainder 2) SERVER srv2 OPTIONS (schema_name 'foreign_tables_schema_mx', table_name 'foreign_table_test');

SELECT partmethod, repmodel FROM pg_dist_partition
    WHERE logicalrelid IN ('parent_for_foreign_tables'::regclass, 'foreign_partition_1'::regclass, 'foreign_partition_2'::regclass, 'foreign_partition_3'::regclass)
    ORDER BY logicalrelid;

CREATE USER MAPPING FOR CURRENT_USER
        SERVER srv1
        OPTIONS (user 'postgres');
CREATE USER MAPPING FOR CURRENT_USER
        SERVER srv2
        OPTIONS (user 'postgres');

SELECT * FROM parent_for_foreign_tables ORDER BY id;
SELECT * FROM foreign_partition_1 ORDER BY id;
SELECT * FROM foreign_partition_2 ORDER BY id;
SELECT * FROM foreign_partition_3 ORDER BY id;

\c - - - :worker_1_port
SET search_path TO foreign_tables_schema_mx;
SELECT partmethod, repmodel FROM pg_dist_partition
    WHERE logicalrelid IN ('parent_for_foreign_tables'::regclass, 'foreign_partition_1'::regclass, 'foreign_partition_2'::regclass, 'foreign_partition_3'::regclass)
    ORDER BY logicalrelid;

SELECT * FROM parent_for_foreign_tables ORDER BY id;
SELECT * FROM foreign_partition_1 ORDER BY id;
SELECT * FROM foreign_partition_2 ORDER BY id;
SELECT * FROM foreign_partition_3 ORDER BY id;
\c - - - :master_port

SET search_path TO foreign_tables_schema_mx;
--verify
SELECT partmethod, repmodel FROM pg_dist_partition WHERE logicalrelid = 'foreign_table'::regclass ORDER BY logicalrelid;

CREATE SERVER foreign_server_local
        FOREIGN DATA WRAPPER postgres_fdw
        OPTIONS (host 'localhost', port :'master_port', dbname 'regression');
CREATE USER MAPPING FOR CURRENT_USER
        SERVER foreign_server_local
        OPTIONS (user 'postgres');
CREATE FOREIGN TABLE foreign_table_local (
        id integer NOT NULL,
        data text
)
        SERVER foreign_server_local
        OPTIONS (schema_name 'foreign_tables_schema_mx', table_name 'foreign_table_test');

CREATE TABLE dist_tbl(a int);
INSERT INTO dist_tbl VALUES (1);
SELECT create_distributed_table('dist_tbl','a');
SELECT * FROM dist_tbl d JOIN foreign_table_local f ON d.a=f.id ORDER BY f.id;

CREATE TABLE ref_tbl(a int);
INSERT INTO ref_tbl VALUES (1);
SELECT create_reference_table('ref_tbl');
SELECT * FROM ref_tbl d JOIN foreign_table_local f ON d.a=f.id ORDER BY f.id;

SELECT citus_add_local_table_to_metadata('foreign_table_local');

\c - - - :worker_1_port
SET search_path TO foreign_tables_schema_mx;
SELECT * FROM dist_tbl d JOIN foreign_table_local f ON d.a=f.id ORDER BY f.id;
SELECT * FROM ref_tbl d JOIN foreign_table_local f ON d.a=f.id ORDER BY f.id;
\c - - - :master_port

SET search_path TO foreign_tables_schema_mx;

CREATE FOREIGN TABLE foreign_table_local_fails (
        id integer NOT NULL,
        data text
)
        SERVER foreign_server_local
        OPTIONS (schema_name 'foreign_tables_schema_mx');

-- should error out because doesn't have a table_name field
SELECT citus_add_local_table_to_metadata('foreign_table_local_fails');

-- should work since it has a table_name
ALTER FOREIGN TABLE foreign_table_local_fails OPTIONS (table_name 'foreign_table_test');
SELECT citus_add_local_table_to_metadata('foreign_table_local_fails');

INSERT INTO foreign_table_test VALUES (1, 'test');

SELECT undistribute_table('foreign_table_local_fails');

DROP FOREIGN TABLE foreign_table_local;

-- disallow dropping table_name when foreign table is in metadata
CREATE TABLE table_name_drop(id int);
CREATE FOREIGN TABLE foreign_table_name_drop_fails (
        id INT
)
        SERVER foreign_server_local
        OPTIONS (schema_name 'foreign_tables_schema_mx', table_name 'table_name_drop');

SELECT citus_add_local_table_to_metadata('foreign_table_name_drop_fails');

-- table_name option is already added
ALTER FOREIGN TABLE foreign_table_name_drop_fails OPTIONS (ADD table_name 'table_name_drop');

-- throw error if user tries to drop table_name option from a foreign table inside metadata
ALTER FOREIGN TABLE foreign_table_name_drop_fails OPTIONS (DROP table_name);

-- case sensitive option name
ALTER FOREIGN TABLE foreign_table_name_drop_fails OPTIONS (DROP Table_Name);

-- other options are allowed to drop
ALTER FOREIGN TABLE foreign_table_name_drop_fails OPTIONS (DROP schema_name);

CREATE FOREIGN TABLE foreign_table_name_drop (
        id INT
)
        SERVER foreign_server_local
        OPTIONS (schema_name 'foreign_tables_schema_mx', table_name 'table_name_drop');

-- user can drop table_option if foreign table is not in metadata
ALTER FOREIGN TABLE foreign_table_name_drop OPTIONS (DROP table_name);

-- we should not intercept data wrappers other than postgres_fdw
CREATE EXTENSION file_fdw;

-- remove validator method to add table_name option; otherwise, table_name option is not allowed
SELECT result FROM run_command_on_all_nodes('ALTER FOREIGN DATA WRAPPER file_fdw NO VALIDATOR');

CREATE SERVER citustest FOREIGN DATA WRAPPER file_fdw;

\copy (select i from generate_series(0,100)i) to '/tmp/test_file_fdw.data';
CREATE FOREIGN TABLE citustest_filefdw (
        data text
)
        SERVER citustest
        OPTIONS ( filename '/tmp/test_file_fdw.data');


-- add non-postgres_fdw table into metadata even if it does not have table_name option
SELECT citus_add_local_table_to_metadata('citustest_filefdw');

ALTER FOREIGN TABLE citustest_filefdw OPTIONS (ADD table_name 'unused_table_name_option');

-- drop table_name option of non-postgres_fdw table even if it is inside metadata
ALTER FOREIGN TABLE citustest_filefdw OPTIONS (DROP table_name);


-- cleanup at exit
set client_min_messages to error;
DROP SCHEMA foreign_tables_schema_mx CASCADE;
