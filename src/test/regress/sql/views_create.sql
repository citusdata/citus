CREATE SCHEMA views_create;
SET search_path TO views_create;
GRANT ALL ON SCHEMA views_create TO regularuser;

CREATE TABLE view_test_table(a INT NOT NULL PRIMARY KEY, b BIGINT, c text);
SELECT create_distributed_table('view_test_table', 'a');
-- Since creating view distributed or locally depends on the arbitrary config
-- set client_min_messages to ERROR to get consistent result.
SET client_min_messages TO ERROR;
CREATE OR REPLACE VIEW select_filtered_view AS
    SELECT * FROM view_test_table WHERE c = 'testing'
    WITH CASCADED CHECK OPTION;
CREATE OR REPLACE VIEW select_all_view AS
    SELECT * FROM view_test_table
    WITH LOCAL CHECK OPTION;
CREATE OR REPLACE VIEW count_view AS
    SELECT COUNT(*) FROM view_test_table;
RESET client_min_messages;

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

CREATE TABLE distributed (id bigserial PRIMARY KEY,
                    	  name text,
                    	  created_at timestamptz DEFAULT now());
CREATE TABLE reference (id bigserial PRIMARY KEY,
                    	title text);
CREATE TABLE local (id bigserial PRIMARY KEY,
                    title text);
SET client_min_messages TO ERROR;
CREATE VIEW "local regular view" AS SELECT * FROM local;
GRANT ALL ON TABLE "local regular view" TO regularuser;
CREATE VIEW dist_regular_view AS SELECT * FROM distributed;
GRANT ALL ON TABLE dist_regular_view TO regularuser;

CREATE VIEW local_regular_view2 as SELECT count(*) FROM distributed JOIN "local regular view" USING (id);
GRANT ALL ON TABLE local_regular_view2 TO regularuser;
CREATE VIEW local_regular_view3 as SELECT count(*) FROM local JOIN dist_regular_view USING (id);
GRANT ALL ON TABLE local_regular_view3 TO regularuser;
CREATE VIEW "local regular view4" as SELECT count(*) as "my cny" FROM dist_regular_view JOIN "local regular view" USING (id);
GRANT ALL ON TABLE "local regular view4" TO regularuser;
RESET client_min_messages;

-- these above restrictions brought us to the following schema
SELECT create_reference_table('reference');
SELECT create_distributed_table('distributed', 'id');
SELECT create_reference_table('local');
