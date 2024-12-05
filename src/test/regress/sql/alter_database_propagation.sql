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

-- this database is not distributed so we will not see any remote commands
CREATE DATABASE db_to_test;
alter database db_to_test with CONNECTION LIMIT 100;
DROP DATABASE db_to_test;

set citus.log_remote_commands = false;
