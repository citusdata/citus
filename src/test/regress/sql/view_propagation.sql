-- Tests to check propagation of all view commands
CREATE SCHEMA view_prop_schema;
SET search_path to view_prop_schema;

-- Check creating views depending on different types of tables
-- and from multiple schemas

-- Check the most basic one
CREATE VIEW prop_view_basic AS SELECT 1;

-- Try to create view depending local table, then try to recreate it after distributing the table
CREATE TABLE view_table_1(id int, val_1 text);
CREATE VIEW prop_view_1 AS
    SELECT * FROM view_table_1;

SELECT create_distributed_table('view_table_1', 'id');
CREATE OR REPLACE VIEW prop_view_1 AS
    SELECT * FROM view_table_1;

-- Try to create view depending local table, then try to recreate it after making the table reference table
CREATE TABLE view_table_2(id int PRIMARY KEY, val_1 text);
CREATE VIEW prop_view_2 AS
    SELECT view_table_1.id, view_table_2.val_1 FROM view_table_1 INNER JOIN view_table_2
    ON view_table_1.id = view_table_2.id;

SELECT create_reference_table('view_table_2');
CREATE OR REPLACE VIEW prop_view_2 AS
    SELECT view_table_1.id, view_table_2.val_1 FROM view_table_1 INNER JOIN view_table_2
    ON view_table_1.id = view_table_2.id;

-- Try to create view depending local table, then try to recreate it after making the table citus local table
CREATE TABLE view_table_3(id int, val_1 text);
CREATE VIEW prop_view_3 AS
    SELECT * FROM view_table_1 WHERE id IN
    (SELECT view_table_2.id FROM view_table_2 INNER JOIN view_table_3 ON view_table_2.id = view_table_3.id);

SET client_min_messages TO WARNING;
SELECT 1 FROM citus_add_node('localhost', :master_port, groupid=>0);
RESET client_min_messages;

ALTER TABLE view_table_3
ADD CONSTRAINT f_key_for_local_table
FOREIGN KEY(id)
REFERENCES view_table_2(id);

CREATE OR REPLACE VIEW prop_view_3 AS
    SELECT * FROM view_table_1 WHERE id IN
    (SELECT view_table_2.id FROM view_table_2 INNER JOIN view_table_3 ON view_table_2.id = view_table_3.id);

-- Try to create view depending on PG metadata table
CREATE VIEW prop_view_4 AS
    SELECT * FROM pg_stat_activity;

-- Try to create view depending on Citus metadata table
CREATE VIEW prop_view_5 AS
    SELECT * FROM citus_dist_stat_activity;

-- Try to create table depending on a local table from another schema, then try to create it again after distributing the table
CREATE SCHEMA view_prop_schema_inner;
SET search_path TO view_prop_schema_inner;

-- Create local table for tests below
CREATE TABLE view_table_4(id int, val_1 text);

-- Create a distributed table and view to test drop view below
CREATE TABLE inner_view_table(id int);
SELECT create_distributed_table('inner_view_table','id');
CREATE VIEW inner_view_prop AS SELECT * FROM inner_view_table;

SET search_path to view_prop_schema;

CREATE VIEW prop_view_6 AS
    SELECT vt1.id, vt4.val_1 FROM view_table_1 AS vt1
    INNER JOIN view_prop_schema_inner.view_table_4 AS vt4 ON vt1.id = vt4.id;

SELECT create_distributed_table('view_prop_schema_inner.view_table_4','id');
CREATE OR REPLACE VIEW prop_view_6 AS
    SELECT vt1.id, vt4.val_1 FROM view_table_1 AS vt1
    INNER JOIN view_prop_schema_inner.view_table_4 AS vt4 ON vt1.id = vt4.id;

-- Show that all views are propagated as distributed object
SELECT * FROM (SELECT pg_identify_object_as_address(classid, objid, objsubid) as obj_identifier from pg_catalog.pg_dist_object) as obj_identifiers where obj_identifier::text like '%prop_view_%' ORDER BY 1;

-- Check creating views depending various kind of objects
-- Tests will also check propagating dependent objects

-- Depending on function
SET citus.enable_ddl_propagation TO OFF;
CREATE OR REPLACE FUNCTION func_1_for_view(param_1 int)
RETURNS int
LANGUAGE plpgsql AS
$$
BEGIN
    return param_1;
END;
$$;
RESET citus.enable_ddl_propagation;

-- Show that function will be propagated together with the view
SELECT * FROM (SELECT pg_identify_object_as_address(classid, objid, objsubid) as obj_identifier from pg_catalog.pg_dist_object) as obj_identifiers where obj_identifier::text like '%func_1_for_view%';

CREATE VIEW prop_view_7 AS SELECT func_1_for_view(id) FROM view_table_1;

SELECT * FROM (SELECT pg_identify_object_as_address(classid, objid, objsubid) as obj_identifier from pg_catalog.pg_dist_object) as obj_identifiers where obj_identifier::text like '%func_1_for_view%';
SELECT * FROM (SELECT pg_identify_object_as_address(classid, objid, objsubid) as obj_identifier from pg_catalog.pg_dist_object) as obj_identifiers where obj_identifier::text like '%prop_view_7%';

-- Depending on type
SET citus.enable_ddl_propagation TO OFF;
CREATE TYPE type_for_view_prop AS ENUM ('a','b','c');
RESET citus.enable_ddl_propagation;

-- Show that type will be propagated together with the view
SELECT * FROM (SELECT pg_identify_object_as_address(classid, objid, objsubid) as obj_identifier from pg_catalog.pg_dist_object) as obj_identifiers where obj_identifier::text like '%type_for_view_prop%';

CREATE VIEW prop_view_8 AS SELECT val_1::type_for_view_prop FROM view_table_1;

SELECT * FROM (SELECT pg_identify_object_as_address(classid, objid, objsubid) as obj_identifier from pg_catalog.pg_dist_object) as obj_identifiers where obj_identifier::text like '%type_for_view_prop%';
SELECT * FROM (SELECT pg_identify_object_as_address(classid, objid, objsubid) as obj_identifier from pg_catalog.pg_dist_object) as obj_identifiers where obj_identifier::text like '%prop_view_8%';

-- Depending on another view
CREATE TABLE view_table_5(id int);
CREATE VIEW prop_view_9 AS SELECT * FROM view_table_5;
CREATE VIEW prop_view_10 AS SELECT * FROM prop_view_9;

SELECT * FROM (SELECT pg_identify_object_as_address(classid, objid, objsubid) as obj_identifier from pg_catalog.pg_dist_object) as obj_identifiers where obj_identifier::text like '%prop_view_9%';
SELECT * FROM (SELECT pg_identify_object_as_address(classid, objid, objsubid) as obj_identifier from pg_catalog.pg_dist_object) as obj_identifiers where obj_identifier::text like '%prop_view_10%';

SELECT create_distributed_table('view_table_5', 'id');
CREATE OR REPLACE VIEW prop_view_10 AS SELECT * FROM prop_view_9;

SELECT * FROM (SELECT pg_identify_object_as_address(classid, objid, objsubid) as obj_identifier from pg_catalog.pg_dist_object) as obj_identifiers where obj_identifier::text like '%prop_view_9%';
SELECT * FROM (SELECT pg_identify_object_as_address(classid, objid, objsubid) as obj_identifier from pg_catalog.pg_dist_object) as obj_identifiers where obj_identifier::text like '%prop_view_10%';

-- Check views owned by non-superuser
SET client_min_messages TO ERROR;
CREATE USER view_creation_user;
SELECT 1 FROM run_command_on_workers($$CREATE USER view_creation_user;$$);
GRANT ALL PRIVILEGES ON SCHEMA view_prop_schema to view_creation_user;

SET ROLE view_creation_user;

CREATE TABLE user_owned_table_for_view(id int);
SELECT create_distributed_table('user_owned_table_for_view','id');
CREATE VIEW view_owned_by_user AS SELECT * FROM user_owned_table_for_view;
SELECT * FROM (SELECT pg_identify_object_as_address(classid, objid, objsubid) as obj_identifier from pg_catalog.pg_dist_object) as obj_identifiers where obj_identifier::text like '%view_owned_by_user%';
DROP VIEW view_owned_by_user;
SELECT * FROM (SELECT pg_identify_object_as_address(classid, objid, objsubid) as obj_identifier from pg_catalog.pg_dist_object) as obj_identifiers where obj_identifier::text like '%view_owned_by_user%';
DROP TABLE user_owned_table_for_view;

RESET ROLE;
RESET client_min_messages;

-- Create view with different options

CREATE TABLE view_table_6(id int, val_1 text);
SELECT create_distributed_table('view_table_6','id');

-- TEMP VIEW is not supported. View will be created locally.
CREATE TEMP VIEW temp_prop_view AS SELECT * FROM view_table_6;

-- Recursive views are supported
CREATE RECURSIVE VIEW nums_1_100_prop_view (n) AS
    VALUES (1)
UNION ALL
    SELECT n+1 FROM nums_1_100_prop_view WHERE n < 100;

SELECT * FROM (SELECT pg_identify_object_as_address(classid, objid, objsubid) as obj_identifier from pg_catalog.pg_dist_object) as obj_identifiers where obj_identifier::text like '%nums_1_100_prop_view%';

-- Sequences are supported as dependency
CREATE SEQUENCE sequence_to_prop;
CREATE VIEW seq_view_prop AS SELECT sequence_to_prop.is_called FROM sequence_to_prop;

SELECT * FROM (SELECT pg_identify_object_as_address(classid, objid, objsubid) as obj_identifier from pg_catalog.pg_dist_object) as obj_identifiers where obj_identifier::text like '%sequence_to_prop%';
SELECT * FROM (SELECT pg_identify_object_as_address(classid, objid, objsubid) as obj_identifier from pg_catalog.pg_dist_object) as obj_identifiers where obj_identifier::text like '%seq_view_prop%';

-- Views depend on temp sequences will be created locally
CREATE TEMPORARY SEQUENCE temp_sequence_to_drop;
CREATE VIEW temp_seq_view_prop AS SELECT temp_sequence_to_drop.is_called FROM temp_sequence_to_drop;

-- Check circular dependencies are detected
CREATE VIEW circular_view_1 AS SELECT * FROM view_table_6;
CREATE VIEW circular_view_2 AS SELECT * FROM view_table_6;
CREATE OR REPLACE VIEW circular_view_1 AS SELECT view_table_6.* FROM view_table_6 JOIN circular_view_2 USING (id);
CREATE OR REPLACE VIEW circular_view_2 AS SELECT view_table_6.* FROM view_table_6 JOIN circular_view_1 USING (id);

-- Recursive views with distributed tables included
CREATE TABLE employees (employee_id int, manager_id int, full_name text);
SELECT create_distributed_table('employees', 'employee_id');

CREATE OR REPLACE RECURSIVE VIEW reporting_line (employee_id, subordinates) AS
SELECT
	employee_id,
	full_name AS subordinates
FROM
	employees
WHERE
	manager_id IS NULL
UNION ALL
	SELECT
		e.employee_id,
		(
			rl.subordinates || ' > ' || e.full_name
		) AS subordinates
	FROM
		employees e
	INNER JOIN reporting_line rl ON e.manager_id = rl.employee_id;

-- Aliases are supported
CREATE VIEW aliased_opt_prop_view(alias_1, alias_2) AS SELECT * FROM view_table_6;

-- View options are supported
CREATE VIEW opt_prop_view
    WITH(check_option=CASCADED, security_barrier=true)
    AS SELECT * FROM view_table_6;

CREATE VIEW sep_opt_prop_view
    AS SELECT * FROM view_table_6
    WITH LOCAL CHECK OPTION;

SELECT * FROM (SELECT pg_identify_object_as_address(classid, objid, objsubid) as obj_identifier from pg_catalog.pg_dist_object) as obj_identifiers where obj_identifier::text like '%opt_prop_view%' ORDER BY 1;

-- Check definitions and reltoptions of views are correct on workers
\c - - - :worker_1_port

SELECT definition FROM pg_views WHERE viewname = 'aliased_opt_prop_view';
SELECT definition FROM pg_views WHERE viewname = 'opt_prop_view';
SELECT definition FROM pg_views WHERE viewname = 'sep_opt_prop_view';

SELECT relname, reloptions
FROM pg_class
WHERE
    oid = 'view_prop_schema.aliased_opt_prop_view'::regclass::oid OR
    oid = 'view_prop_schema.opt_prop_view'::regclass::oid OR
    oid = 'view_prop_schema.sep_opt_prop_view'::regclass::oid
ORDER BY 1;

\c - - - :master_port
SET search_path to view_prop_schema;

-- Sync metadata to check it works properly after adding a view
SELECT start_metadata_sync_to_node('localhost', :worker_1_port);

-- Drop views and check metadata afterwards
DROP VIEW prop_view_9 CASCADE;
DROP VIEW opt_prop_view, aliased_opt_prop_view, view_prop_schema_inner.inner_view_prop, sep_opt_prop_view;

SELECT * FROM (SELECT pg_identify_object_as_address(classid, objid, objsubid) as obj_identifier from pg_catalog.pg_dist_object) as obj_identifiers where obj_identifier::text like '%inner_view_prop%';
SELECT * FROM (SELECT pg_identify_object_as_address(classid, objid, objsubid) as obj_identifier from pg_catalog.pg_dist_object) as obj_identifiers where obj_identifier::text like '%opt_prop_view%';

-- Drop a column that view depends on
ALTER TABLE view_table_1 DROP COLUMN val_1 CASCADE;

-- Since prop_view_3 depends on the view_table_1's val_1 column, it should be dropped
SELECT * FROM (SELECT pg_identify_object_as_address(classid, objid, objsubid) as obj_identifier from pg_catalog.pg_dist_object) as obj_identifiers where obj_identifier::text like '%prop_view_3%';

-- Drop a table that view depends on
DROP TABLE view_table_2 CASCADE;

-- Since prop_view_2 depends on the view_table_2, it should be dropped
SELECT * FROM (SELECT pg_identify_object_as_address(classid, objid, objsubid) as obj_identifier from pg_catalog.pg_dist_object) as obj_identifiers where obj_identifier::text like '%prop_view_2%';

-- Show that unsupported CREATE OR REPLACE VIEW commands are catched by PG on the coordinator
CREATE TABLE table_to_test_unsup_view(id int, val1 text);
SELECT create_distributed_table('table_to_test_unsup_view', 'id');

CREATE VIEW view_for_unsup_commands AS SELECT * FROM table_to_test_unsup_view;

CREATE OR REPLACE VIEW view_for_unsup_commands(a,b) AS SELECT * FROM table_to_test_unsup_view;
CREATE OR REPLACE VIEW view_for_unsup_commands AS SELECT id FROM table_to_test_unsup_view;

-- ALTER VIEW PROPAGATION
CREATE TABLE alter_view_table(id int, val1 text);
SELECT create_distributed_table('alter_view_table','id');

CREATE VIEW alter_view_1 AS SELECT * FROM alter_view_table;

-- Set/drop default value is not supported by Citus
ALTER VIEW alter_view_1 ALTER COLUMN val1 SET DEFAULT random()::text;
ALTER TABLE alter_view_1 ALTER COLUMN val1 SET DEFAULT random()::text;

ALTER VIEW alter_view_1 ALTER COLUMN val1 DROP DEFAULT;
ALTER TABLE alter_view_1 ALTER COLUMN val1 DROP DEFAULT;

-- Set/reset options view alter view/alter table commands
ALTER VIEW alter_view_1 SET (check_option=cascaded);
ALTER VIEW alter_view_1 SET (security_barrier);
ALTER VIEW alter_view_1 SET (check_option=cascaded, security_barrier);
ALTER VIEW alter_view_1 SET (check_option=cascaded, security_barrier = true);

ALTER TABLE alter_view_1 SET (check_option=cascaded);
ALTER TABLE alter_view_1 SET (security_barrier);
ALTER TABLE alter_view_1 SET (check_option=cascaded, security_barrier);
ALTER TABLE alter_view_1 SET (check_option=cascaded, security_barrier = true);

-- Check the definition on both coordinator and worker node
SELECT definition FROM pg_views WHERE viewname = 'alter_view_1';

SELECT relname, reloptions
FROM pg_class
WHERE oid = 'view_prop_schema.alter_view_1'::regclass::oid;

\c - - - :worker_1_port
SELECT definition FROM pg_views WHERE viewname = 'alter_view_1';

SELECT relname, reloptions
FROM pg_class
WHERE oid = 'view_prop_schema.alter_view_1'::regclass::oid;

\c - - - :master_port
SET search_path to view_prop_schema;

ALTER TABLE alter_view_1 RESET (check_option, security_barrier);
ALTER VIEW alter_view_1 RESET (check_option, security_barrier);

-- Change the schema of the view
ALTER TABLE alter_view_1 SET SCHEMA view_prop_schema_inner;
ALTER VIEW view_prop_schema_inner.alter_view_1 SET SCHEMA view_prop_schema;

-- Rename view and view's column name
ALTER VIEW alter_view_1 RENAME COLUMN val1 TO val2;
ALTER VIEW alter_view_1 RENAME val2 TO val1;
ALTER VIEW alter_view_1 RENAME TO alter_view_2;

ALTER TABLE alter_view_2 RENAME COLUMN val1 TO val2;
ALTER TABLE alter_view_2 RENAME val2 TO val1;
ALTER TABLE alter_view_2 RENAME TO alter_view_1;

-- Alter owner vith alter view/alter table
SET client_min_messages TO ERROR;
CREATE USER alter_view_user;
SELECT 1 FROM run_command_on_workers($$CREATE USER alter_view_user;$$);
RESET client_min_messages;
ALTER VIEW alter_view_1 OWNER TO alter_view_user;
ALTER TABLE alter_view_1 OWNER TO alter_view_user;

-- Alter view owned by extension
CREATE TABLE table_for_ext_owned_view(id int);
CREATE VIEW extension_owned_view AS SELECT * FROM table_for_ext_owned_view;

CREATE EXTENSION seg;
ALTER EXTENSION seg ADD VIEW extension_owned_view;

SELECT create_distributed_table('table_for_ext_owned_view','id');
CREATE OR REPLACE VIEW extension_owned_view AS SELECT * FROM table_for_ext_owned_view;

-- Since the view is owned by extension Citus shouldn't propagate it
SELECT * FROM (SELECT pg_identify_object_as_address(classid, objid, objsubid) as obj_identifier from pg_catalog.pg_dist_object) as obj_identifiers where obj_identifier::text like '%extension_owned_view%';

-- Try syncing metadata after running ALTER VIEW commands
SELECT start_metadata_sync_to_node('localhost', :worker_1_port);

-- Alter non-existing view
ALTER VIEW IF EXISTS non_existing_view ALTER COLUMN val1 SET DEFAULT random()::text;
ALTER VIEW IF EXISTS non_existing_view SET (check_option=cascaded);
ALTER VIEW IF EXISTS non_existing_view RENAME COLUMN val1 TO val2;
ALTER VIEW IF EXISTS non_existing_view RENAME val2 TO val1;
ALTER VIEW IF EXISTS non_existing_view SET SCHEMA view_prop_schema;

SET client_min_messages TO ERROR;
DROP SCHEMA view_prop_schema_inner CASCADE;
DROP SCHEMA view_prop_schema CASCADE;
