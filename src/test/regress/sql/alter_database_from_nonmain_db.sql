SET citus.superuser TO 'postgres';
set citus.enable_create_database_propagation=on;
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

-- Below tests test the ALTER DATABASE command from main and non-main databases
-- The tests are run on the master and worker nodes to ensure that the command is
-- executed on all nodes.

\c test_alter_db_from_nonmain_db
set citus.log_remote_commands = true;
set citus.grep_remote_commands = "%ALTER DATABASE%";

alter database "altered_database!'2" set tablespace "ts-needs\!escape2";

\c test_alter_db_from_nonmain_db - - :worker_1_port
set citus.log_remote_commands = true;
set citus.grep_remote_commands = "%ALTER DATABASE%";
alter database "altered_database!'2" set tablespace "pg_default";
alter database "altered_database!'2" set tablespace "ts-needs\!escape2";
\c regression - - :master_port
SELECT * FROM public.check_database_on_all_nodes($$altered_database!''2$$) ORDER BY node_type;
\c test_alter_db_from_nonmain_db - - :worker_1_port
set citus.log_remote_commands = true;
set citus.grep_remote_commands = "%ALTER DATABASE%";
alter database "altered_database!'2" set tablespace "pg_default";
alter database "altered_database!'2" set tablespace "ts-needs\!escape2";

\c regression - - :master_port
SELECT * FROM public.check_database_on_all_nodes($$altered_database!''2$$) ORDER BY node_type;
\c test_alter_db_from_nonmain_db - - :worker_2_port
set citus.log_remote_commands = true;
set citus.grep_remote_commands = "%ALTER DATABASE%";
alter database "altered_database!'2" set tablespace "pg_default";

\c regression - - :master_port
SELECT * FROM public.check_database_on_all_nodes($$altered_database!''2$$) ORDER BY node_type;
\c test_alter_db_from_nonmain_db - - :worker_1_port
set citus.log_remote_commands = true;
set citus.grep_remote_commands = "%ALTER DATABASE%";
alter database "altered_database!'2" set tablespace "ts-needs\!escape2";
alter database "altered_database!'2" set tablespace "pg_default";
alter database "altered_database!'2" set tablespace "ts-needs\!escape2";
alter database "altered_database!'2" set tablespace "pg_default";
alter database "altered_database!'2" set tablespace "ts-needs\!escape2";

-- In the below tests, we test the ALTER DATABASE ..set tablespace command
-- repeatedly to ensure that the command does not stuck when executed multiple times

\c regression - - :worker_1_port
set citus.log_remote_commands = true;
set citus.grep_remote_commands = "%ALTER DATABASE%";
alter database "altered_database!'2" set tablespace "pg_default";
alter database "altered_database!'2" set tablespace "ts-needs\!escape2";
alter database "altered_database!'2" set tablespace "pg_default";
alter database "altered_database!'2" set tablespace "ts-needs\!escape2";
alter database "altered_database!'2" set tablespace "pg_default";
alter database "altered_database!'2" set tablespace "ts-needs\!escape2";
alter database "altered_database!'2" set tablespace "pg_default";
alter database "altered_database!'2" set tablespace "ts-needs\!escape2";

\c regression - - :worker_2_port
set citus.log_remote_commands = true;
set citus.grep_remote_commands = "%ALTER DATABASE%";

alter database "altered_database!'2" set tablespace "pg_default";
alter database "altered_database!'2" set tablespace "ts-needs\!escape2";
alter database "altered_database!'2" set tablespace "pg_default";
alter database "altered_database!'2" set tablespace "ts-needs\!escape2";
alter database "altered_database!'2" set tablespace "pg_default";
alter database "altered_database!'2" set tablespace "ts-needs\!escape2";
alter database "altered_database!'2" set tablespace "pg_default";
alter database "altered_database!'2" set tablespace "ts-needs\!escape2";

\c regression - - :master_port
set citus.log_remote_commands = true;
set citus.grep_remote_commands = "%ALTER DATABASE%";
alter database "altered_database!'2" set tablespace "pg_default";
alter database "altered_database!'2" set tablespace "ts-needs\!escape2";
SELECT * FROM public.check_database_on_all_nodes($$altered_database!''2$$) ORDER BY node_type;
alter database "altered_database!'2" set tablespace "pg_default";
SELECT * FROM public.check_database_on_all_nodes($$altered_database!''2$$) ORDER BY node_type;
alter database "altered_database!'2" set tablespace "ts-needs\!escape2";
alter database "altered_database!'2" set tablespace "pg_default";
alter database "altered_database!'2" set tablespace "ts-needs\!escape2";
alter database "altered_database!'2" set tablespace "pg_default";
alter database "altered_database!'2" set tablespace "ts-needs\!escape2";
alter database "altered_database!'2" set tablespace "pg_default";
alter database "altered_database!'2" set tablespace "ts-needs\!escape2";

\c test_alter_db_from_nonmain_db - - :worker_2_port
set citus.log_remote_commands = true;
set citus.grep_remote_commands = "%ALTER DATABASE%";
alter database "altered_database!'2"  rename to altered_database_renamed;
alter database altered_database_renamed  rename to "altered_database!'2";

alter database "altered_database!'2" with
    ALLOW_CONNECTIONS true
    CONNECTION LIMIT 0
    IS_TEMPLATE false;

\c regression - - :master_port
SELECT * FROM public.check_database_on_all_nodes($$altered_database!''2$$) ORDER BY node_type;

\c regression - - :worker_1_port
set citus.log_remote_commands = true;
set citus.grep_remote_commands = "%ALTER DATABASE%";

alter database "altered_database!'2" with
    ALLOW_CONNECTIONS false
    CONNECTION LIMIT 1
    IS_TEMPLATE true;

\c regression - - :master_port
SELECT * FROM public.check_database_on_all_nodes($$altered_database!''2$$) ORDER BY node_type;

\c regression - - :worker_2_port
set citus.log_remote_commands = true;
set citus.grep_remote_commands = "%ALTER DATABASE%";
alter database "altered_database!'2" with
    ALLOW_CONNECTIONS true
    CONNECTION LIMIT 0
    IS_TEMPLATE false;

\c regression
create role test_owner_non_main_db;

\c test_alter_db_from_nonmain_db
set citus.log_remote_commands = true;
set citus.grep_remote_commands = "%ALTER DATABASE%";
set citus.enable_create_database_propagation=on;
alter database "altered_database!'2" owner to test_owner_non_main_db;
\c regression - - :master_port
SELECT * FROM public.check_database_on_all_nodes($$altered_database!''2$$) ORDER BY node_type;

\c test_alter_db_from_nonmain_db - - :worker_1_port
set citus.log_remote_commands = true;
set citus.grep_remote_commands = "%ALTER DATABASE%";
alter database "altered_database!'2" owner to CURRENT_USER;
set default_transaction_read_only = false;
\c regression - - :master_port
SELECT * FROM public.check_database_on_all_nodes($$altered_database!''2$$) ORDER BY node_type;

\c regression - - :worker_2_port
set citus.log_remote_commands = true;
set citus.grep_remote_commands = "%ALTER DATABASE%";
alter database "altered_database!'2" owner to test_owner_non_main_db;
set default_transaction_read_only = false;
\c regression - - :master_port
SELECT * FROM public.check_database_on_all_nodes($$altered_database!''2$$) ORDER BY node_type;

\c regression - - :master_port
set citus.log_remote_commands = true;
set citus.grep_remote_commands = "%ALTER DATABASE%";
alter database "altered_database!'2" owner to CURRENT_USER;
set default_transaction_read_only = false;
SELECT * FROM public.check_database_on_all_nodes($$altered_database!''2$$) ORDER BY node_type;

set citus.enable_alter_database_owner to off;
alter database "altered_database!'2" owner to test_owner_non_main_db;
SELECT * FROM public.check_database_on_all_nodes($$altered_database!''2$$) ORDER BY node_type;
alter database "altered_database!'2" owner to CURRENT_USER;
SELECT * FROM public.check_database_on_all_nodes($$altered_database!''2$$) ORDER BY node_type;
reset citus.enable_alter_database_owner;

\c test_alter_db_from_nonmain_db
set citus.log_remote_commands = true;
set citus.grep_remote_commands = "%ALTER DATABASE%";

alter database "altered_database!'2" set default_transaction_read_only to true;

\c "altered_database!'2" - - :master_port
select name,setting from pg_settings
        where name ='default_transaction_read_only';
\c "altered_database!'2" - - :worker_1_port
select name,setting from pg_settings
        where name ='default_transaction_read_only';
\c "altered_database!'2" - - :worker_2_port
select name,setting from pg_settings
        where name ='default_transaction_read_only';

\c test_alter_db_from_nonmain_db
set citus.log_remote_commands = true;
set citus.grep_remote_commands = "%ALTER DATABASE%";
alter database "altered_database!'2" reset default_transaction_read_only;

\c "altered_database!'2" - - :master_port
select name,setting from pg_settings
        where name ='default_transaction_read_only';
\c "altered_database!'2" - - :worker_1_port
select name,setting from pg_settings
        where name ='default_transaction_read_only';
\c "altered_database!'2" - - :worker_2_port
select name,setting from pg_settings
        where name ='default_transaction_read_only';


\c test_alter_db_from_nonmain_db - - :worker_2_port
set citus.log_remote_commands = true;
set citus.grep_remote_commands = "%ALTER DATABASE%";
alter database "altered_database!'2" set log_duration from current;
alter database "altered_database!'2" set log_duration to DEFAULT;
alter database "altered_database!'2" set log_duration = true;
\c "altered_database!'2" - - :master_port
select name,setting from pg_settings
        where name ='log_duration';

\c "altered_database!'2" - - :worker_1_port
select name,setting from pg_settings
        where name ='log_duration';

\c "altered_database!'2" - - :worker_2_port
select name,setting from pg_settings
        where name ='log_duration';

\c test_alter_db_from_nonmain_db - - :worker_1_port
set citus.log_remote_commands = true;
set citus.grep_remote_commands = "%ALTER DATABASE%";
alter database "altered_database!'2" RESET log_duration;

\c "altered_database!'2" - - :master_port
select name,setting from pg_settings
        where name ='log_duration';
\c "altered_database!'2" - - :worker_1_port
select name,setting from pg_settings
        where name ='log_duration';
\c "altered_database!'2" - - :worker_2_port
select name,setting from pg_settings
        where name ='log_duration';

\c regression - - :worker_2_port
set citus.log_remote_commands = true;
set citus.grep_remote_commands = "%ALTER DATABASE%";
alter database "altered_database!'2" set statement_timeout = 1000;

\c "altered_database!'2" - - :master_port
select name,setting from pg_settings
        where name ='statement_timeout';
\c "altered_database!'2" - - :worker_1_port
select name,setting from pg_settings
        where name ='statement_timeout';
\c "altered_database!'2" - - :worker_2_port
select name,setting from pg_settings
        where name ='statement_timeout';

\c regression - - :worker_1_port
set citus.log_remote_commands = true;
set citus.grep_remote_commands = "%ALTER DATABASE%";
alter database "altered_database!'2" set statement_timeout to DEFAULT;
alter database "altered_database!'2" RESET statement_timeout;

\c "altered_database!'2" - - :master_port
select name,setting from pg_settings
        where name ='statement_timeout';
\c "altered_database!'2" - - :worker_1_port
select name,setting from pg_settings
        where name ='statement_timeout';
\c "altered_database!'2" - - :worker_2_port
select name,setting from pg_settings
        where name ='statement_timeout';

\c regression
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
