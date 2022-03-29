CREATE SCHEMA views_create;
SET search_path TO views_create;

CREATE TABLE view_test_table(a INT NOT NULL PRIMARY KEY, b BIGINT, c text);
CREATE OR REPLACE VIEW select_filtered_view AS
    SELECT * FROM view_test_table WHERE c = 'testing'
    WITH CASCADED CHECK OPTION;
CREATE OR REPLACE VIEW select_all_view AS
    SELECT * FROM view_test_table
    WITH LOCAL CHECK OPTION;
CREATE OR REPLACE VIEW count_view AS
    SELECT COUNT(*) FROM view_test_table;
SELECT create_distributed_table('view_test_table', 'a');

INSERT INTO view_test_table VALUES (1,1,'testing'), (2,1,'views');
SELECT * FROM count_view;
SELECT COUNT(*) FROM count_view;
SELECT COUNT(*) FROM select_all_view;

SELECT * FROM select_filtered_view;

-- dummy temp recursive view
CREATE TEMP RECURSIVE VIEW recursive_defined_non_recursive_view(c) AS (SELECT 1);
