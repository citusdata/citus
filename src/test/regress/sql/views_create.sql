CREATE SCHEMA views_create;
SET search_path TO views_create;

CREATE TABLE view_test_table(a INT NOT NULL PRIMARY KEY, b BIGINT, c text);
SELECT create_distributed_table('view_test_table', 'a');

SHOW citus.enable_metadata_sync;
SELECT * FROM pg_dist_partition;
SELECT pg_identify_object_as_address(classid, objid, objsubid) from pg_catalog.pg_dist_object;

CREATE OR REPLACE VIEW select_filtered_view AS
    SELECT * FROM view_test_table WHERE c = 'testing'
    WITH CASCADED CHECK OPTION;
CREATE OR REPLACE VIEW select_all_view AS
    SELECT * FROM view_test_table
    WITH LOCAL CHECK OPTION;
CREATE OR REPLACE VIEW count_view AS
    SELECT COUNT(*) FROM view_test_table;

INSERT INTO view_test_table VALUES (1,1,'testing'), (2,1,'views');
SELECT * FROM count_view;
SELECT COUNT(*) FROM count_view;
SELECT COUNT(*) FROM select_all_view;

SELECT * FROM select_filtered_view;

-- dummy temp recursive view
CREATE TEMP RECURSIVE VIEW recursive_defined_non_recursive_view(c) AS (SELECT 1);

CREATE MATERIALIZED VIEW select_all_matview AS
    SELECT * FROM view_test_table
    WITH DATA;

CREATE MATERIALIZED VIEW IF NOT EXISTS select_filtered_matview AS
    SELECT * FROM view_test_table WHERE c = 'views'
    WITH NO DATA;

REFRESH MATERIALIZED VIEW select_filtered_matview;

SELECT COUNT(*) FROM select_all_matview;
SELECT * FROM select_filtered_matview;

SELECT COUNT(*) FROM select_all_view a JOIN select_filtered_matview b ON a.c=b.c;
SELECT COUNT(*) FROM select_all_view a JOIN view_test_table b ON a.c=b.c;
