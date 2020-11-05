--
-- Test utility functions for cstore_fdw tables.
--

CREATE TABLE empty_table (a int) USING cstore_tableam;
CREATE TABLE table_with_data (a int) USING cstore_tableam;
CREATE TABLE non_cstore_table (a int);

COPY table_with_data FROM STDIN;
1
2
3
\.

SELECT pg_relation_size('empty_table') < pg_relation_size('table_with_data');
SELECT cstore_table_size('non_cstore_table');

DROP TABLE empty_table;
DROP TABLE table_with_data;
DROP TABLE non_cstore_table;
