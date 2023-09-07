set citus.log_remote_commands = true;
set citus.grep_remote_commands = '%ALTER DATABASE%';

--since ALLOW_CONNECTIONS alter option should be executed in a different database
-- and since we don't have a multiple database support for now,
-- this statement will get error
alter database regression ALLOW_CONNECTIONS false;


DO $$
declare
    v_connlimit_initial numeric;
    v_connlimit_fetched int;
begin
    select datconnlimit into v_connlimit_initial from pg_database where datname = 'regression';
    alter database regression with CONNECTION LIMIT 100;
    select datconnlimit into v_connlimit_fetched from pg_database where datname = 'regression';
    raise notice 'v_connlimit_initial: %, v_connlimit_fetched: %', v_connlimit_initial, v_connlimit_fetched;
    execute 'alter database regression with CONNECTION LIMIT ' || v_connlimit_initial;
    select datconnlimit into v_connlimit_fetched from pg_database where datname = 'regression';
    raise notice 'v_connlimit_initial: %, v_connlimit_fetched: %', v_connlimit_initial, v_connlimit_fetched;

    alter database regression with IS_TEMPLATE true CONNECTION LIMIT 100;
    execute 'alter database regression with IS_TEMPLATE false  CONNECTION LIMIT' || v_connlimit_initial;

    alter database regression with IS_TEMPLATE true;
    select datistemplate from pg_database where datname = 'regression';

    alter database regression with IS_TEMPLATE false;
    select datistemplate from pg_database where datname = 'regression';
end;
$$
language plpgsql;


set citus.log_remote_commands = false;
