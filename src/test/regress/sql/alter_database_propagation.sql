set citus.log_remote_commands = true;
set citus.grep_remote_commands = '%ALTER DATABASE%';


-- since ALLOW_CONNECTIONS alter option should be executed in a different database
-- and since we don't have a multiple database support for now,
-- this statement will get error
alter database regression ALLOW_CONNECTIONS false;


alter database regression with CONNECTION LIMIT 100;
alter database regression with IS_TEMPLATE true CONNECTION LIMIT 50;
alter database regression with CONNECTION LIMIT -1;
alter database regression with IS_TEMPLATE true;
alter database regression with IS_TEMPLATE false;
-- this statement will get error since we don't have a multiple database support for now
alter database regression rename to regression2;

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

set citus.log_remote_commands = false;
