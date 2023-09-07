set citus.log_remote_commands = true;
set citus.grep_remote_commands = '%ALTER DATABASE%';

--since ALLOW_CONNECTIONS alter option should be executed in a different database
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

alter database regression set archive_mode to on;

set archive_mode to off;
alter database regression set archive_mode from current;
alter database regression set archive_mode to DEFAULT;
alter database regression set RESET archive_mode;

alter database regression set archive_timeout to 100;

set archive_timeout to 50;
alter database regression set archive_timeout from current;
alter database regression set archive_timeout to DEFAULT;
alter database regression set RESET archive_timeout;


ALTER DATABASE regression SET checkpoint_completion_target TO 0.9;

set checkpoint_completion_target to 0.5;

alter database regression set checkpoint_completion_target from current;
alter database regression set checkpoint_completion_target to DEFAULT;
alter database regression set RESET checkpoint_completion_target;

alter database regression SET TIME ZONE '-7';

set TIME ZONE '-8';
alter database regression set TIME ZONE from current;
alter database regression set TIME ZONE to DEFAULT;
alter database regression set RESET TIME ZONE;

alter database regression set SET TIME ZONE INTERVAL '-08:00' HOUR TO MINUTE;

set TIME ZONE INTERVAL '-07:00' HOUR TO MINUTE;
alter database regression set TIME ZONE INTERVAL from current;
alter database regression set TIME ZONE INTERVAL to DEFAULT;
alter database regression set RESET TIME ZONE INTERVAL;













set citus.log_remote_commands = false;
