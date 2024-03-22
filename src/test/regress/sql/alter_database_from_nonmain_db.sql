SET citus.superuser TO 'postgres';
set citus.enable_create_database_propagation=on;
set citus.log_remote_commands = true;
set citus.grep_remote_commands = "%CREATE DATABASE%";
create database test_alter_db_from_nonmain_db;
create database "altered_database!'2";
reset citus.enable_create_database_propagation;
\c regression;
set citus.enable_create_database_propagation=on;

\set alter_db_tablespace :abs_srcdir '/tmp_check/ts3'
CREATE TABLESPACE "ts-needs\!escape2" LOCATION :'alter_db_tablespace';

\c - - - :worker_1_port
\set alter_db_tablespace :abs_srcdir '/tmp_check/ts4'
CREATE TABLESPACE "ts-needs\!escape2" LOCATION :'alter_db_tablespace';

\c - - - :worker_2_port
\set alter_db_tablespace :abs_srcdir '/tmp_check/ts5'
CREATE TABLESPACE "ts-needs\!escape2" LOCATION :'alter_db_tablespace';

\c test_alter_db_from_nonmain_db
set citus.enable_ddl_propagation=true;

set citus.log_remote_commands = true;
set citus.grep_remote_commands = "%CREATE DATABASE%";
create database test1;

set citus.grep_remote_commands = "%ALTER DATABASE%";

alter database "altered_database!'2" set tablespace "ts-needs\!escape2";
alter database "altered_database!'2" rename to altered_database_renamed;
alter database altered_database_renamed rename to "altered_database!'2";

alter database "altered_database!'2" with
    ALLOW_CONNECTIONS false
    CONNECTION LIMIT 1
    IS_TEMPLATE true;
alter database "altered_database!'2" with
    ALLOW_CONNECTIONS true
    CONNECTION LIMIT 0
    IS_TEMPLATE false;

\c regression
set citus.enable_ddl_propagation=true;
create role test_owner_non_main_db;
\c test_alter_db_from_nonmain_db
set citus.enable_ddl_propagation=true;
set citus.log_remote_commands = true;
set citus.grep_remote_commands = "%ALTER DATABASE%";
set citus.enable_create_database_propagation=on;
alter database "altered_database!'2" owner to test_owner_non_main_db;
alter database "altered_database!'2" owner to CURRENT_USER;
alter database "altered_database!'2" set default_transaction_read_only = true;
set default_transaction_read_only = false;
alter database "altered_database!'2" set default_transaction_read_only from current;
alter database "altered_database!'2" set default_transaction_read_only to DEFAULT;
alter database "altered_database!'2" RESET default_transaction_read_only;
alter database "altered_database!'2" SET TIME ZONE '-7';
alter database "altered_database!'2" set TIME ZONE LOCAL;
alter database "altered_database!'2" set TIME ZONE DEFAULT;
alter database "altered_database!'2" RESET TIME ZONE;
alter database "altered_database!'2" SET TIME ZONE INTERVAL '-08:00' HOUR TO MINUTE;
alter database "altered_database!'2" RESET TIME ZONE;
alter database "altered_database!'2" set default_transaction_isolation = 'serializable';
set default_transaction_isolation = 'read committed';
alter database "altered_database!'2" set default_transaction_isolation from current;
alter database "altered_database!'2" set default_transaction_isolation to DEFAULT;
alter database "altered_database!'2" RESET default_transaction_isolation;
alter database "altered_database!'2" set statement_timeout = 1000;
set statement_timeout = 2000;
alter database "altered_database!'2" set statement_timeout from current;
alter database "altered_database!'2" set statement_timeout to DEFAULT;
alter database "altered_database!'2" RESET statement_timeout;
alter database "altered_database!'2" set lock_timeout = 1201.5;
set lock_timeout = 1202.5;
alter database "altered_database!'2" set lock_timeout from current;
alter database "altered_database!'2" set lock_timeout to DEFAULT;
alter database "altered_database!'2" RESET lock_timeout;
ALTER DATABASE "altered_database!'2" RESET ALL;
\c regression
show citus.enable_ddl_propagation;
set citus.enable_ddl_propagation=true;
set citus.enable_create_database_propagation=on;
drop database "altered_database!'2";
drop database test_alter_db_from_nonmain_db;
reset citus.enable_create_database_propagation;

drop role test_owner_non_main_db;

SELECT result FROM run_command_on_all_nodes(
  $$
  drop tablespace "ts-needs\!escape2"
  $$
);
