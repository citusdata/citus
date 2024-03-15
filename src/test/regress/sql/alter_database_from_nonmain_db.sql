
--SET citus.superuser TO 'postgres';
set citus.enable_create_database_propagation=on;

create database test_alter_db_from_nonmain_db;

create database altered_database;
reset citus.enable_create_database_propagation;
