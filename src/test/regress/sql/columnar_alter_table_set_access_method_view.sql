CREATE SCHEMA view_access_method_test;
SET search_path TO view_access_method_test;

--create the view to test alter table set access method on it
CREATE TABLE view_test_table (id int, val int, flag bool, kind int);
SELECT create_distributed_table('view_access_method_test.view_test_table','id');
INSERT INTO view_test_table VALUES (1, 1, true, 99), (2, 2, false, 99), (2, 3, true, 88);
CREATE VIEW view_test_view AS SELECT * FROM view_test_table;

--alter the view to columnar
select alter_table_set_access_method('view_test_view','columnar');

--clean environment
DROP SCHEMA view_access_method_test CASCADE;
