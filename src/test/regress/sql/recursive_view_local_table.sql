CREATE SCHEMA postgres_local_table;
SET search_path TO postgres_local_table;

CREATE TABLE local_table(a INT);
INSERT INTO local_table VALUES (1),(2),(3);

CREATE RECURSIVE VIEW recursive_view(val_1, val_2) AS
(
		VALUES(0,1)
	UNION ALL
		SELECT GREATEST(val_1,val_2),val_1 + val_2 AS local_table
	FROM
		recursive_view
	WHERE val_2 < 50
);

CREATE RECURSIVE VIEW recursive_defined_non_recursive_view(c) AS (SELECT 1 FROM local_table);

CREATE TABLE ref_table(a int, b INT);
SELECT create_reference_table('ref_table');
INSERT INTO ref_table VALUES (1,1);

SELECT ref_table.* FROM ref_table LEFT OUTER JOIN (SELECT * FROM recursive_view WHERE FALSE) AS sub ON FALSE;
SELECT ref_table.* FROM ref_table LEFT OUTER JOIN (SELECT * FROM (SELECT 1, random() FROM local_table) as s WHERE FALSE) AS sub ON FALSE ORDER BY 1,2;
SELECT ref_table.* FROM ref_table LEFT OUTER JOIN (SELECT * FROM recursive_view WHERE FALSE) AS sub ON TRUE ORDER BY 1,2;
SELECT ref_table.* FROM ref_table LEFT OUTER JOIN (SELECT * FROM (SELECT 1, random() FROM local_table) as s WHERE FALSE) AS sub ON TRUE ORDER BY 1,2;
SELECT ref_table.* FROM ref_table LEFT OUTER JOIN (SELECT * FROM recursive_view WHERE TRUE) AS sub ON TRUE ORDER BY 1,2;
SELECT ref_table.* FROM ref_table LEFT OUTER JOIN (SELECT * FROM (SELECT 1, random() FROM local_table) as s WHERE TRUE) AS sub ON TRUE ORDER BY 1,2;
SELECT ref_table.* FROM ref_table JOIN (SELECT * FROM recursive_view WHERE FALSE) AS sub ON FALSE ORDER BY 1,2;
SELECT ref_table.* FROM ref_table JOIN (SELECT * FROM (SELECT 1, random() FROM local_table) as s WHERE FALSE) AS sub ON FALSE ORDER BY 1,2;
SELECT ref_table.* FROM ref_table JOIN (SELECT * FROM recursive_view WHERE FALSE) AS sub ON TRUE ORDER BY 1,2;
SELECT ref_table.* FROM ref_table JOIN (SELECT * FROM (SELECT 1, random() FROM local_table) as s WHERE FALSE) AS sub ON TRUE ORDER BY 1,2;
SELECT ref_table.* FROM ref_table JOIN (SELECT * FROM recursive_view WHERE TRUE) AS sub ON TRUE ORDER BY 1,2;
SELECT ref_table.* FROM ref_table JOIN (SELECT * FROM (SELECT 1, random() FROM local_table) as s WHERE TRUE) AS sub ON TRUE ORDER BY 1,2;
SELECT ref_table.* FROM ref_table LEFT OUTER JOIN (SELECT * FROM recursive_defined_non_recursive_view WHERE FALSE) AS sub ON FALSE ORDER BY 1,2;
SELECT ref_table.* FROM ref_table LEFT OUTER JOIN (SELECT * FROM recursive_defined_non_recursive_view WHERE FALSE) AS sub ON TRUE ORDER BY 1,2;
SELECT ref_table.* FROM ref_table LEFT OUTER JOIN (SELECT * FROM recursive_defined_non_recursive_view WHERE TRUE) AS sub ON TRUE ORDER BY 1,2;
SELECT ref_table.* FROM ref_table JOIN (SELECT * FROM recursive_defined_non_recursive_view WHERE FALSE) AS sub ON FALSE ORDER BY 1,2;
SELECT ref_table.* FROM ref_table JOIN (SELECT * FROM recursive_defined_non_recursive_view WHERE FALSE) AS sub ON TRUE ORDER BY 1,2;
SELECT ref_table.* FROM ref_table JOIN (SELECT * FROM recursive_defined_non_recursive_view WHERE TRUE) AS sub ON TRUE ORDER BY 1,2;

SELECT ref_table.* FROM ref_table WHERE EXISTS (SELECT * FROM local_table l WHERE l.a = ref_table.a);
SELECT ref_table.* FROM ref_table WHERE EXISTS (SELECT * FROM local_table l WHERE l.a = ref_table.a) AND false;
SELECT ref_table.* FROM ref_table WHERE EXISTS (SELECT * FROM local_table l WHERE l.a = ref_table.a AND false);

SELECT ref_table.* FROM ref_table WHERE EXISTS (SELECT * FROM recursive_view l WHERE l.val_1 = ref_table.a);
SELECT ref_table.* FROM ref_table WHERE EXISTS (SELECT * FROM recursive_view l WHERE l.val_1 = ref_table.a) AND false;
SELECT ref_table.* FROM ref_table WHERE EXISTS (SELECT * FROM recursive_view l WHERE l.val_1 = ref_table.a AND false);

SELECT ref_table.* FROM ref_table WHERE EXISTS (SELECT * FROM recursive_defined_non_recursive_view l WHERE l.c = ref_table.a);
SELECT ref_table.* FROM ref_table WHERE EXISTS (SELECT * FROM recursive_defined_non_recursive_view l WHERE l.c = ref_table.a) AND false;
SELECT ref_table.* FROM ref_table WHERE EXISTS (SELECT * FROM recursive_defined_non_recursive_view l WHERE l.c = ref_table.a AND false);

SET client_min_messages TO WARNING;
DROP SCHEMA postgres_local_table CASCADE;
