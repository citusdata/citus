CREATE TABLE dist(a int, b int);
SELECT create_distributed_table('dist', 'a');

CREATE table ref(a int, b int);
SELECT create_reference_table('ref');

