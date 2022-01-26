CREATE SCHEMA mx_app_name;

SET application_name TO mx_app_name;
CREATE TABLE dist_1(a int);
SELECT create_distributed_table('dist_1', 'a');

DROP TABLE dist_1;

DROP SCHEMA mx_app_name CASCADE;

