-- Test Scenario:
-- 1. Create 3 databases: test_group0, test_group1, test_group2 on all nodes.
-- 2. Set these as main databases for nodes 1, 2, 3 respectively by inserting
--    records into pg_dist_database.
-- 3. Create tables: test_table0, test_table1, test_table2 in test_group0,
--    test_group1, test_group2 respectively. Insert enough records to make
--    the size of each database distinct.
-- 4. Check the size of each database. Ensure each size is distinct and that
--    pg_dist_database_size returns the size from the correct node.
-- 5. Release the resources.

CREATE OR REPLACE FUNCTION round_to_nearest_ten(size_in_byte BIGINT)
RETURNS INT AS $$
DECLARE
    size_in_mb INT;
BEGIN
    size_in_mb := size_in_byte / 1024 / 1024;
    RETURN (size_in_mb + 5) / 10 * 10;
END;
$$ LANGUAGE plpgsql;

set citus.enable_create_database_propagation to true;

-- Step 1 creates the databases on all nodes.
create database test_group0;

create database test_group1;

create DATABASE test_group2;


-- Get the groupid for each node.
SELECT groupid AS worker_1_group FROM pg_dist_node WHERE nodeport = :worker_1_port \gset
SELECT groupid AS worker_2_group FROM pg_dist_node WHERE nodeport = :worker_2_port \gset


-- Step 2 sets the main database for each node by inserting records into pg_dist_database.
\c regression;

SELECT result FROM run_command_on_all_nodes('insert into pg_dist_database (databaseid, groupid) select oid, 0 from pg_database where datname = ''test_group0'';');
SELECT result FROM run_command_on_all_nodes('insert into pg_dist_database (databaseid, groupid) select oid,' || :worker_1_group || ' from pg_database where datname = ''test_group1'';');
SELECT result FROM run_command_on_all_nodes('insert into pg_dist_database (databaseid, groupid) select oid,' || :worker_2_group || ' from pg_database where datname = ''test_group2'';');


-- Step 3 creates tables in each database and inserts enough records to make the size of each database distinct.
\c test_group0;

create table test_table0 (a int);
insert into test_table0 select generate_series(1,1000000);

\c - - - :worker_1_port;
\c test_group1;

create table test_table1 (a int);
insert into test_table1 select generate_series(1,1000000);

\c - - - :worker_2_port;
\c test_group2;
create table test_table2 (a int);
insert into test_table2 select generate_series(1,1000000);

\c - - - :master_port;

\c regression;




-- Step 4 checks the size of each database. Ensure each size is distinct and that pg_dist_database_size returns the size from the correct node.

-- Below queries shows the size of each database on each node. Databases with lower sizes are shell databases.
SELECT result FROM run_command_on_all_nodes('SELECT round_to_nearest_ten(pg_database_size(''test_group0'')) FROM pg_dist_database WHERE databaseid IN (SELECT oid FROM pg_database WHERE datname = ''test_group0'') order by groupid;');
SELECT result FROM run_command_on_all_nodes('SELECT round_to_nearest_ten(pg_database_size(''test_group1'')) FROM pg_dist_database WHERE databaseid IN (SELECT oid FROM pg_database WHERE datname = ''test_group1'') order by groupid;');
SELECT result FROM run_command_on_all_nodes('SELECT round_to_nearest_ten(pg_database_size(''test_group2'')) FROM pg_dist_database WHERE databaseid IN (SELECT oid FROM pg_database WHERE datname = ''test_group2'') order by groupid;');
SET citus.log_remote_commands = true;

--Below queries shows the non-shell databases, which has greater size than others.
select CASE
        WHEN size > 35000001 THEN 'CORRECT DATABASE'
        ELSE 'SHELL DATABASE' end
from (select  pg_dist_database_size('test_group0') as size) as size;


select CASE
        WHEN size > 35000001 THEN 'CORRECT DATABASE'
        ELSE 'SHELL DATABASE' end
from (select  pg_dist_database_size('test_group1') as size) as size;

select CASE
        WHEN size > 35000001 THEN 'CORRECT DATABASE'
        ELSE 'SHELL DATABASE' end
from (select  pg_dist_database_size('test_group2') as size) as size;

--release the resources and reset the parameters.
RESET client_min_messages;
reset citus.log_remote_commands;

drop function round_to_nearest_ten(BIGINT);

SELECT result FROM run_command_on_all_nodes('delete from pg_dist_database where databaseid in (select oid from pg_database where datname in (''test_group0'', ''test_group1'', ''test_group2''))');

set citus.enable_create_database_propagation to true;

drop DATABASE test_group0;
drop DATABASE test_group1;
drop DATABASE test_group2;

reset citus.enable_create_database_propagation;




