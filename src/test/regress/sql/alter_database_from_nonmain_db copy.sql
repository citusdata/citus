SET citus.superuser TO 'postgres';
set citus.enable_create_database_propagation=on;
create database test_alter_db_from_nonmain_db;
create database altered_database;
reset citus.enable_create_database_propagation;
\c regression;
set citus.enable_create_database_propagation=on;
\c test_alter_db_from_nonmain_db
set citus.log_remote_commands = true;
alter database altered_database rename to altered_database_renamed;
alter database altered_database_renamed rename to altered_database;

alter database altered_database with
    ALLOW_CONNECTIONS false
    CONNECTION LIMIT 1
    IS_TEMPLATE true;
alter database altered_database with
    ALLOW_CONNECTIONS true
    CONNECTION LIMIT 0
    IS_TEMPLATE false;

\c regression
create role test_owner_non_main_db;
\c test_alter_db_from_nonmain_db
set citus.log_remote_commands = true;
set citus.enable_create_database_propagation=on;
alter database altered_database owner to test_owner_non_main_db;
alter database altered_database owner to CURRENT_USER;
\c regression
\set alter_db_tablespace_non_main :abs_srcdir '/tmp_check/ts3'
CREATE TABLESPACE alter_db_tablespace_non_main LOCATION :'alter_db_tablespace_non_main';
\c - - - :worker_1_port
\set alter_db_tablespace_non_main :abs_srcdir '/tmp_check/ts4'
CREATE TABLESPACE alter_db_tablespace_non_main LOCATION :'alter_db_tablespace_non_main';
\c - - - :worker_2_port
\set alter_db_tablespace_non_main :abs_srcdir '/tmp_check/ts5'
CREATE TABLESPACE alter_db_tablespace_non_main LOCATION :'alter_db_tablespace_non_main';
\c test_alter_db_from_nonmain_db
set citus.log_remote_commands = true;
set citus.enable_create_database_propagation=on;
alter database altered_database set TABLESPACE alter_db_tablespace_non_main;
ALTER DATABASE altered_database REFRESH COLLATION VERSION;
NOTICE:  version has not changed
alter database altered_database set default_transaction_read_only = true;
set default_transaction_read_only = false;
alter database altered_database set default_transaction_read_only from current;
alter database altered_database set default_transaction_read_only to DEFAULT;
alter database altered_database RESET default_transaction_read_only;
alter database altered_database SET TIME ZONE '-7';
alter database altered_database set TIME ZONE LOCAL;
alter database altered_database set TIME ZONE DEFAULT;
alter database altered_database RESET TIME ZONE;
alter database altered_database SET TIME ZONE INTERVAL '-08:00' HOUR TO MINUTE;
alter database altered_database RESET TIME ZONE;
alter database altered_database set default_transaction_isolation = 'serializable';
set default_transaction_isolation = 'read committed';
alter database altered_database set default_transaction_isolation from current;
alter database altered_database set default_transaction_isolation to DEFAULT;
alter database altered_database RESET default_transaction_isolation;
alter database altered_database set statement_timeout = 1000;
set statement_timeout = 2000;
alter database altered_database set statement_timeout from current;
alter database altered_database set statement_timeout to DEFAULT;
alter database altered_database RESET statement_timeout;
alter database altered_database set lock_timeout = 1201.5;
set lock_timeout = 1202.5;
alter database altered_database set lock_timeout from current;
alter database altered_database set lock_timeout to DEFAULT;
alter database altered_database RESET lock_timeout;
ALTER DATABASE altered_database RESET ALL;
\c regression
set citus.enable_create_database_propagation=on;
drop database altered_database;
drop database test_alter_db_from_nonmain_db;
reset citus.enable_create_database_propagation;
SELECT result FROM run_command_on_all_nodes(
  $$
  drop tablespace alter_db_tablespace_non_main
  $$
);
     result
---------------------------------------------------------------------
 DROP TABLESPACE
 DROP TABLESPACE
 DROP TABLESPACE
(3 rows)

