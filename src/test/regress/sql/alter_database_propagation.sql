set citus.log_remote_commands = true;
set citus.grep_remote_commands = '%ALTER DATABASE%';

alter database regression with CONNECTION LIMIT 100;
alter database regression with IS_TEMPLATE true CONNECTION LIMIT 50;
alter database regression with CONNECTION LIMIT -1;
alter database regression with IS_TEMPLATE true;
alter database regression with IS_TEMPLATE false;


alter database regression set default_transaction_read_only = true;

set default_transaction_read_only = false;

alter database regression set default_transaction_read_only from current;
alter database regression set default_transaction_read_only to DEFAULT;
alter database regression RESET default_transaction_read_only;

alter database regression SET TIME ZONE '-7';

alter database regression set TIME ZONE LOCAL;
alter database regression set TIME ZONE DEFAULT;
alter database regression RESET TIME ZONE;

alter database regression SET TIME ZONE INTERVAL '-08:00' HOUR TO MINUTE;

alter database regression RESET TIME ZONE;


alter database regression set default_transaction_isolation = 'serializable';
set default_transaction_isolation = 'read committed';

alter database regression set default_transaction_isolation from current;
alter database regression set default_transaction_isolation to DEFAULT;
alter database regression RESET default_transaction_isolation;

alter database regression set statement_timeout = 1000;
set statement_timeout = 2000;

alter database regression set statement_timeout from current;
alter database regression set statement_timeout to DEFAULT;
alter database regression RESET statement_timeout;

alter database regression set lock_timeout = 1201.5;
set lock_timeout = 1202.5;

alter database regression set lock_timeout from current;
alter database regression set lock_timeout to DEFAULT;
alter database regression RESET lock_timeout;

set citus.enable_create_database_propagation=on;
SET citus.next_operation_id TO 3000;
create database "regression!'2";
alter database "regression!'2" with CONNECTION LIMIT 100;
alter database "regression!'2" with IS_TEMPLATE true CONNECTION LIMIT 50;
alter database "regression!'2" with IS_TEMPLATE false;




\set alter_db_tablespace :abs_srcdir '/tmp_check/ts3'
CREATE TABLESPACE alter_db_tablespace LOCATION :'alter_db_tablespace';

\c - - - :worker_1_port
\set alter_db_tablespace :abs_srcdir '/tmp_check/ts4'
CREATE TABLESPACE alter_db_tablespace LOCATION :'alter_db_tablespace';

\c - - - :worker_2_port
\set alter_db_tablespace :abs_srcdir '/tmp_check/ts5'
CREATE TABLESPACE alter_db_tablespace LOCATION :'alter_db_tablespace';

\c - - - :master_port

set citus.log_remote_commands = true;
set citus.grep_remote_commands = '%ALTER DATABASE%';

alter database "regression!'2" set TABLESPACE alter_db_tablespace;

set citus.enable_create_database_propagation=on;
alter database "regression!'2" rename to regression3;

-- check that the local database rename and alter comnmand is not propagated
set citus.enable_create_database_propagation=off;
CREATE database local_regression;

alter DATABASE local_regression with CONNECTION LIMIT 100;
alter DATABASE local_regression rename to local_regression2;
drop database local_regression2;

set citus.enable_create_database_propagation=on;

drop database regression3;

SET citus.next_operation_id TO 3100;
create database "regression!'4";


SELECT result FROM run_command_on_all_nodes(
  $$
  ALTER TABLESPACE alter_db_tablespace RENAME TO "ts-needs\!escape"
  $$
);

alter database "regression!'4" set TABLESPACE "ts-needs\!escape";

drop database "regression!'4";

set citus.log_remote_commands = false;
set citus.enable_create_database_propagation=off;

SELECT result FROM run_command_on_all_nodes(
  $$
  drop tablespace "ts-needs\!escape"
  $$
);
