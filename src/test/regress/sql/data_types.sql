CREATE SCHEMA data_types;
SET search_path TO data_types;

Create or replace function test_jsonb() returns jsonb as
$$
begin
	return '{"test_json": "test"}';
end;
$$ language plpgsql;


CREATE TABLE data_types_table

(
	dist_key bigint PRIMARY KEY,
	col1 int[], col2 int[][], col3 int [][][],
	col4 varchar[], col5 varchar[][], col6 varchar [][][],
	col70 bit, col7 bit[], col8 bit[][], col9 bit [][][],
	col10 bit varying(10),
	col11 bit varying(10)[], col12 bit varying(10)[][], col13 bit varying(10)[][][],
	col14 bytea, col15 bytea[], col16 bytea[][], col17 bytea[][][],
	col18 boolean, col19 boolean[], col20 boolean[][], col21 boolean[][][],
	col22 inet, col23 inet[], col24 inet[][], col25 inet[][][],
	col26 macaddr, col27 macaddr[], col28 macaddr[][], col29 macaddr[][][],
	col30 numeric, col32 numeric[], col33 numeric[][], col34 numeric[][][],
	col35 jsonb, col36 jsonb[], col37 jsonb[][], col38 jsonb[][][]
);

CREATE TABLE  data_types_table_local AS SELECT * FROM data_types_table;

SELECT create_distributed_table('data_types_table', 'dist_key');

INSERT INTO data_types_table (dist_key,col1, col2, col3, col4, col5, col6, col70, col7, col8, col9, col10, col11, col12, col13, col14, col15, col16, col17, col18, col19, col20, col21, col22, col23, col24, col25, col26, col27, col28, col29, col30, col32, col33, col34, col35, col36, col37, col38)
VALUES (1,ARRAY[1], ARRAY[ARRAY[0,0,0]], ARRAY[ARRAY[ARRAY[0,0,0]]], ARRAY['1'], ARRAY[ARRAY['0','0','0']], ARRAY[ARRAY[ARRAY['0','0','0']]], '1', ARRAY[b'1'], ARRAY[ARRAY[b'0',b'0',b'0']], ARRAY[ARRAY[ARRAY[b'0',b'0',b'0']]], '11101',ARRAY[b'1'], ARRAY[ARRAY[b'01',b'01',b'01']], ARRAY[ARRAY[ARRAY[b'011',b'110',b'0000']]], '\xb4a8e04c0b', ARRAY['\xb4a8e04c0b'::BYTEA], ARRAY[ARRAY['\xb4a8e04c0b'::BYTEA, '\xb4a8e04c0b'::BYTEA, '\xb4a8e04c0b'::BYTEA]], ARRAY[ARRAY[ARRAY['\xb4a8e04c0b'::BYTEA,'\x18a232a678'::BYTEA,'\x38b2697632'::BYTEA]]], '1', ARRAY[TRUE], ARRAY[ARRAY[1::boolean,TRUE,FALSE]], ARRAY[ARRAY[ARRAY[1::boolean,TRUE,FALSE]]], INET '192.168.1/24', ARRAY[INET '192.168.1.1'], ARRAY[ARRAY[INET '0.0.0.0', '0.0.0.0/32', '::ffff:fff0:1', '192.168.1/24']], ARRAY[ARRAY[ARRAY[INET '0.0.0.0', '0.0.0.0/32', '::ffff:fff0:1', '192.168.1/24']]],MACADDR '08:00:2b:01:02:03', ARRAY[MACADDR '08:00:2b:01:02:03'], ARRAY[ARRAY[MACADDR '08002b-010203', MACADDR '08002b-010203', '08002b010203']], ARRAY[ARRAY[ARRAY[MACADDR '08002b-010203', MACADDR '08002b-010203', '08002b010203']]], 690, ARRAY[1.1], ARRAY[ARRAY[0,0.111,0.15]], ARRAY[ARRAY[ARRAY[0,0,0]]], test_jsonb(), ARRAY[test_jsonb()], ARRAY[ARRAY[test_jsonb(),test_jsonb(),test_jsonb(),test_jsonb()]], ARRAY[ARRAY[ARRAY[test_jsonb(),test_jsonb(),test_jsonb(),test_jsonb()]]]),
       (2,ARRAY[1,2,3], ARRAY[ARRAY[1,2,3], ARRAY[5,6,7]], ARRAY[ARRAY[ARRAY[1,2,3]], ARRAY[ARRAY[5,6,7]], ARRAY[ARRAY[1,2,3]], ARRAY[ARRAY[5,6,7]]], ARRAY['1','2','3'], ARRAY[ARRAY['1','2','3'], ARRAY['5','6','7']], ARRAY[ARRAY[ARRAY['1','2','3']], ARRAY[ARRAY['5','6','7']], ARRAY[ARRAY['1','2','3']], ARRAY[ARRAY['5','6','7']]], '0', ARRAY[b'1',b'0',b'0'], ARRAY[ARRAY[b'1',b'1',b'0'], ARRAY[b'0',b'0',b'1']], ARRAY[ARRAY[ARRAY[b'1',b'1',b'1']], ARRAY[ARRAY[b'1','0','0']], ARRAY[ARRAY[b'1','1','1']], ARRAY[ARRAY[b'0','0','0']]], '00010', ARRAY[b'11',b'10',b'01'], ARRAY[ARRAY[b'11',b'010',b'101'], ARRAY[b'101',b'01111',b'1000001']], ARRAY[ARRAY[ARRAY[b'10000',b'111111',b'1101010101']], ARRAY[ARRAY[b'1101010','0','1']], ARRAY[ARRAY[b'1','1','11111111']], ARRAY[ARRAY[b'0000000','0','0']]], '\xb4a8e04c0b', ARRAY['\xb4a8e04c0b'::BYTEA,'\x18a232a678'::BYTEA,'\x38b2697632'::BYTEA], ARRAY[ARRAY['\xb4a8e04c0b'::BYTEA,'\x18a232a678'::BYTEA,'\x38b2697632'::BYTEA], ARRAY['\xb4a8e04c0b'::BYTEA,'\x18a232a678'::BYTEA,'\x38b2697632'::BYTEA]], ARRAY[ARRAY[ARRAY['\xb4a8e04c0b'::BYTEA,'\x18a232a678'::BYTEA,'\x38b2697632'::BYTEA]], ARRAY[ARRAY['\xb4a8e04c0b'::BYTEA,'\x18a232a678'::BYTEA,'\x38b2697632'::BYTEA]], ARRAY[ARRAY['\xb4a8e04c0b'::BYTEA,'\x18a232a678'::BYTEA,'\x38b2697632'::BYTEA]], ARRAY[ARRAY['\xb4a8e04c0b'::BYTEA,'\x18a232a678'::BYTEA,'\x38b2697632'::BYTEA]]], 'true', ARRAY[1::boolean,TRUE,FALSE], ARRAY[ARRAY[1::boolean,TRUE,FALSE], ARRAY[1::boolean,TRUE,FALSE]], ARRAY[ARRAY[ARRAY[1::boolean,TRUE,FALSE]], ARRAY[ARRAY[1::boolean,TRUE,FALSE]], ARRAY[ARRAY[1::boolean,TRUE,FALSE]], ARRAY[ARRAY[1::boolean,TRUE,FALSE]]],'0.0.0.0/32', ARRAY[INET '0.0.0.0', '0.0.0.0/32', '::ffff:fff0:1', '192.168.1/24'], ARRAY[ARRAY[INET '0.0.0.0', '0.0.0.0/32', '::ffff:fff0:1', '192.168.1/24']], ARRAY[ARRAY[ARRAY[INET '0.0.0.0', '0.0.0.0/32', '::ffff:fff0:1', '192.168.1/24']], ARRAY[ARRAY[INET '0.0.0.0', '0.0.0.0/32', '::ffff:fff0:1', '192.168.1/24']], ARRAY[ARRAY[INET '0.0.0.0', '0.0.0.0/32', '::ffff:fff0:1', '192.168.1/24']], ARRAY[ARRAY[INET '0.0.0.0', '0.0.0.0/32', '::ffff:fff0:1', '192.168.1/24']]], '0800.2b01.0203', ARRAY[MACADDR '08002b-010203', MACADDR '08002b-010203', '08002b010203'], ARRAY[ARRAY[MACADDR '08002b-010203', MACADDR '08002b-010203', '08002b010203']], ARRAY[ARRAY[ARRAY[MACADDR '08002b-010203', MACADDR '08002b-010203', '08002b010203']], ARRAY[ARRAY[MACADDR '08002b-010203', MACADDR '08002b-010203', '08002b010203']], ARRAY[ARRAY[MACADDR '08002b-010203', MACADDR '08002b-010203', '08002b010203']], ARRAY[ARRAY[MACADDR '08002b-010203', MACADDR '08002b-010203', '08002b010203']]], 0.99, ARRAY[1.1,2.22,3.33], ARRAY[ARRAY[1.55,2.66,3.88], ARRAY[11.5,10101.6,7111.1]], ARRAY[ARRAY[ARRAY[1,2,3]], ARRAY[ARRAY[5,6,7]], ARRAY[ARRAY[1.1,2.1,3]], ARRAY[ARRAY[5.0,6.0,7.0]]],test_jsonb(), ARRAY[test_jsonb(),test_jsonb(),test_jsonb(),test_jsonb()], ARRAY[ARRAY[test_jsonb(),test_jsonb(),test_jsonb(),test_jsonb()], ARRAY[test_jsonb(),test_jsonb(),test_jsonb(),test_jsonb()]], ARRAY[ARRAY[ARRAY[test_jsonb(),test_jsonb(),test_jsonb(),test_jsonb()]], ARRAY[ARRAY[test_jsonb(),test_jsonb(),test_jsonb(),test_jsonb()]], ARRAY[ARRAY[test_jsonb(),test_jsonb(),test_jsonb(),test_jsonb()]], ARRAY[ARRAY[test_jsonb(),test_jsonb(),test_jsonb(),test_jsonb()]]]);

-- insert the same data to the local node as well
INSERT INTO data_types_table_local (dist_key,col1, col2, col3, col4, col5, col6, col70, col7, col8, col9, col10, col11, col12, col13, col14, col15, col16, col17, col18, col19, col20, col21, col22, col23, col24, col25, col26, col27, col28, col29, col30, col32, col33, col34, col35, col36, col37, col38)
VALUES (1,ARRAY[1], ARRAY[ARRAY[0,0,0]], ARRAY[ARRAY[ARRAY[0,0,0]]], ARRAY['1'], ARRAY[ARRAY['0','0','0']], ARRAY[ARRAY[ARRAY['0','0','0']]], '1', ARRAY[b'1'], ARRAY[ARRAY[b'0',b'0',b'0']], ARRAY[ARRAY[ARRAY[b'0',b'0',b'0']]], '11101',ARRAY[b'1'], ARRAY[ARRAY[b'01',b'01',b'01']], ARRAY[ARRAY[ARRAY[b'011',b'110',b'0000']]], '\xb4a8e04c0b', ARRAY['\xb4a8e04c0b'::BYTEA], ARRAY[ARRAY['\xb4a8e04c0b'::BYTEA, '\xb4a8e04c0b'::BYTEA, '\xb4a8e04c0b'::BYTEA]], ARRAY[ARRAY[ARRAY['\xb4a8e04c0b'::BYTEA,'\x18a232a678'::BYTEA,'\x38b2697632'::BYTEA]]], '1', ARRAY[TRUE], ARRAY[ARRAY[1::boolean,TRUE,FALSE]], ARRAY[ARRAY[ARRAY[1::boolean,TRUE,FALSE]]], INET '192.168.1/24', ARRAY[INET '192.168.1.1'], ARRAY[ARRAY[INET '0.0.0.0', '0.0.0.0/32', '::ffff:fff0:1', '192.168.1/24']], ARRAY[ARRAY[ARRAY[INET '0.0.0.0', '0.0.0.0/32', '::ffff:fff0:1', '192.168.1/24']]],MACADDR '08:00:2b:01:02:03', ARRAY[MACADDR '08:00:2b:01:02:03'], ARRAY[ARRAY[MACADDR '08002b-010203', MACADDR '08002b-010203', '08002b010203']], ARRAY[ARRAY[ARRAY[MACADDR '08002b-010203', MACADDR '08002b-010203', '08002b010203']]], 690, ARRAY[1.1], ARRAY[ARRAY[0,0.111,0.15]], ARRAY[ARRAY[ARRAY[0,0,0]]], test_jsonb(), ARRAY[test_jsonb()], ARRAY[ARRAY[test_jsonb(),test_jsonb(),test_jsonb(),test_jsonb()]], ARRAY[ARRAY[ARRAY[test_jsonb(),test_jsonb(),test_jsonb(),test_jsonb()]]]),
       (2,ARRAY[1,2,3], ARRAY[ARRAY[1,2,3], ARRAY[5,6,7]], ARRAY[ARRAY[ARRAY[1,2,3]], ARRAY[ARRAY[5,6,7]], ARRAY[ARRAY[1,2,3]], ARRAY[ARRAY[5,6,7]]], ARRAY['1','2','3'], ARRAY[ARRAY['1','2','3'], ARRAY['5','6','7']], ARRAY[ARRAY[ARRAY['1','2','3']], ARRAY[ARRAY['5','6','7']], ARRAY[ARRAY['1','2','3']], ARRAY[ARRAY['5','6','7']]], '0', ARRAY[b'1',b'0',b'0'], ARRAY[ARRAY[b'1',b'1',b'0'], ARRAY[b'0',b'0',b'1']], ARRAY[ARRAY[ARRAY[b'1',b'1',b'1']], ARRAY[ARRAY[b'1','0','0']], ARRAY[ARRAY[b'1','1','1']], ARRAY[ARRAY[b'0','0','0']]], '00010', ARRAY[b'11',b'10',b'01'], ARRAY[ARRAY[b'11',b'010',b'101'], ARRAY[b'101',b'01111',b'1000001']], ARRAY[ARRAY[ARRAY[b'10000',b'111111',b'1101010101']], ARRAY[ARRAY[b'1101010','0','1']], ARRAY[ARRAY[b'1','1','11111111']], ARRAY[ARRAY[b'0000000','0','0']]], '\xb4a8e04c0b', ARRAY['\xb4a8e04c0b'::BYTEA,'\x18a232a678'::BYTEA,'\x38b2697632'::BYTEA], ARRAY[ARRAY['\xb4a8e04c0b'::BYTEA,'\x18a232a678'::BYTEA,'\x38b2697632'::BYTEA], ARRAY['\xb4a8e04c0b'::BYTEA,'\x18a232a678'::BYTEA,'\x38b2697632'::BYTEA]], ARRAY[ARRAY[ARRAY['\xb4a8e04c0b'::BYTEA,'\x18a232a678'::BYTEA,'\x38b2697632'::BYTEA]], ARRAY[ARRAY['\xb4a8e04c0b'::BYTEA,'\x18a232a678'::BYTEA,'\x38b2697632'::BYTEA]], ARRAY[ARRAY['\xb4a8e04c0b'::BYTEA,'\x18a232a678'::BYTEA,'\x38b2697632'::BYTEA]], ARRAY[ARRAY['\xb4a8e04c0b'::BYTEA,'\x18a232a678'::BYTEA,'\x38b2697632'::BYTEA]]], 'true', ARRAY[1::boolean,TRUE,FALSE], ARRAY[ARRAY[1::boolean,TRUE,FALSE], ARRAY[1::boolean,TRUE,FALSE]], ARRAY[ARRAY[ARRAY[1::boolean,TRUE,FALSE]], ARRAY[ARRAY[1::boolean,TRUE,FALSE]], ARRAY[ARRAY[1::boolean,TRUE,FALSE]], ARRAY[ARRAY[1::boolean,TRUE,FALSE]]],'0.0.0.0/32', ARRAY[INET '0.0.0.0', '0.0.0.0/32', '::ffff:fff0:1', '192.168.1/24'], ARRAY[ARRAY[INET '0.0.0.0', '0.0.0.0/32', '::ffff:fff0:1', '192.168.1/24']], ARRAY[ARRAY[ARRAY[INET '0.0.0.0', '0.0.0.0/32', '::ffff:fff0:1', '192.168.1/24']], ARRAY[ARRAY[INET '0.0.0.0', '0.0.0.0/32', '::ffff:fff0:1', '192.168.1/24']], ARRAY[ARRAY[INET '0.0.0.0', '0.0.0.0/32', '::ffff:fff0:1', '192.168.1/24']], ARRAY[ARRAY[INET '0.0.0.0', '0.0.0.0/32', '::ffff:fff0:1', '192.168.1/24']]], '0800.2b01.0203', ARRAY[MACADDR '08002b-010203', MACADDR '08002b-010203', '08002b010203'], ARRAY[ARRAY[MACADDR '08002b-010203', MACADDR '08002b-010203', '08002b010203']], ARRAY[ARRAY[ARRAY[MACADDR '08002b-010203', MACADDR '08002b-010203', '08002b010203']], ARRAY[ARRAY[MACADDR '08002b-010203', MACADDR '08002b-010203', '08002b010203']], ARRAY[ARRAY[MACADDR '08002b-010203', MACADDR '08002b-010203', '08002b010203']], ARRAY[ARRAY[MACADDR '08002b-010203', MACADDR '08002b-010203', '08002b010203']]], 0.99, ARRAY[1.1,2.22,3.33], ARRAY[ARRAY[1.55,2.66,3.88], ARRAY[11.5,10101.6,7111.1]], ARRAY[ARRAY[ARRAY[1,2,3]], ARRAY[ARRAY[5,6,7]], ARRAY[ARRAY[1.1,2.1,3]], ARRAY[ARRAY[5.0,6.0,7.0]]],test_jsonb(), ARRAY[test_jsonb(),test_jsonb(),test_jsonb(),test_jsonb()], ARRAY[ARRAY[test_jsonb(),test_jsonb(),test_jsonb(),test_jsonb()], ARRAY[test_jsonb(),test_jsonb(),test_jsonb(),test_jsonb()]], ARRAY[ARRAY[ARRAY[test_jsonb(),test_jsonb(),test_jsonb(),test_jsonb()]], ARRAY[ARRAY[test_jsonb(),test_jsonb(),test_jsonb(),test_jsonb()]], ARRAY[ARRAY[test_jsonb(),test_jsonb(),test_jsonb(),test_jsonb()]], ARRAY[ARRAY[test_jsonb(),test_jsonb(),test_jsonb(),test_jsonb()]]]);


-- different query/planning executiom types
-- compare results with Postgres
SELECT * FROM data_types_table
	EXCEPT
SELECT * FROM data_types_table_local;

SELECT * FROM data_types_table_local
	EXCEPT
SELECT * FROM data_types_table;

WITH cte_1 AS (SELECT * FROM data_types_table LIMIT 100000),
cte_2 AS (SELECT * FROM data_types_table_local LIMIT 100000)
 SELECT * FROM cte_1
	EXCEPT
 SELECT * FROM cte_2;

WITH cte_1 AS (SELECT * FROM data_types_table LIMIT 100000),
cte_2 AS (SELECT * FROM data_types_table_local LIMIT 100000)
 SELECT * FROM cte_2
	EXCEPT
 SELECT * FROM cte_1;

SELECT * FROM (SELECT *, random() > 100 FROM data_types_table) as foo
EXCEPT
SELECT * FROM (SELECT *, random() > 100 FROM data_types_table_local) as bar;

SELECT * FROM (SELECT *, random() > 100 FROM data_types_table_local) as bar
EXCEPT
SELECT * FROM (SELECT *, random() > 100 FROM data_types_table) as foo;


-- GROUP BY w/wout the dist key
SELECT
	count(*)
FROM
	data_types_table
GROUP BY
	col1, col2, col3, col4, col5, col6, col70, col7, col8, col9, col10, col11, col12, col13, col14, col15, col16, col17, col18, col19, col20, col21, col22, col23, col24, col25, col26, col27, col28, col29, col32, col33, col34, col35, col36, col37, col38;

SELECT
	count(*)
FROM
	data_types_table
GROUP BY
	dist_key, col1, col2, col3, col4, col5, col6, col70, col7, col8, col9, col10, col11, col12, col13, col14, col15, col16, col17, col18, col19, col20, col21, col22, col23, col24, col25, col26, col27, col28, col29, col32, col33, col34, col35, col36, col37, col38;


-- window function w/wout distribution key
SELECT
	count(*) OVER (PARTITION BY col1, col2, col3, col4, col5, col6, col70, col7, col8, col9, col10, col11, col12, col13, col14, col15, col16, col17, col18, col19, col20, col21, col22, col23, col24, col25, col26, col27, col28, col29, col32, col33, col34, col35, col36, col37, col38)
FROM
	data_types_table;

SELECT
	count(*) OVER (PARTITION BY dist_key, col1, col2, col3, col4, col5, col6, col70, col7, col8, col9, col10, col11, col12, col13, col14, col15, col16, col17, col18, col19, col20, col21, col22, col23, col24, col25, col26, col27, col28, col29, col32, col33, col34, col35, col36, col37, col38)
FROM
	data_types_table;

-- DISTINCT w/wout distribution key
SELECT DISTINCT(col1, col2, col3, col4, col5, col6, col70, col7, col8, col9, col10, col11, col12, col13, col14, col15, col16, col17, col18, col19, col20, col21, col22, col23, col24, col25, col26, col27, col28, col29, col32, col33, col34, col35, col36, col37, col38)
FROM
	data_types_table
ORDER BY 1 DESC;

SELECT DISTINCT(dist_key, col1, col2, col3, col4, col5, col6, col70, col7, col8, col9, col10, col11, col12, col13, col14, col15, col16, col17, col18, col19, col20, col21, col22, col23, col24, col25, col26, col27, col28, col29, col32, col33, col34, col35, col36, col37, col38)
FROM
	data_types_table
ORDER BY 1 DESC;

-- count DISTINCT w/wout dist key
SELECT count(DISTINCT(col1, col2, col3, col4, col5, col6, col70, col7, col8, col9, col10, col11, col12, col13, col14, col15, col16, col17, col18, col19, col20, col21, col22, col23, col24, col25, col26, col27, col28, col29, col32, col33, col34, col35, col36, col37, col38))
FROM
	data_types_table
ORDER BY 1 DESC;

SELECT count(DISTINCT(dist_key, col1, col2, col3, col4, col5, col6, col70, col7, col8, col9, col10, col11, col12, col13, col14, col15, col16, col17, col18, col19, col20, col21, col22, col23, col24, col25, col26, col27, col28, col29, col32, col33, col34, col35, col36, col37, col38))
FROM
	data_types_table
ORDER BY 1 DESC;


-- also test with RETURNING
ALTER TABLE data_types_table ADD COLUMN useless_column INT;
UPDATE data_types_table SET useless_column = 1 RETURNING *;

-- three methods of INSERT .. SELECT
INSERT INTO data_types_table SELECT * FROM data_types_table ON CONFLICT (dist_key) DO UPDATE SET useless_column = 10;
INSERT INTO data_types_table SELECT * FROM data_types_table LIMIT 100000 ON CONFLICT (dist_key) DO UPDATE SET useless_column = 10;
INSERT INTO data_types_table (dist_key, col1, col2, col3, col4, col5, col6, col70, col7, col8, col9, col10, col11, col12, col13, col14, col15, col16, col17, col18, col19, col20, col21, col22, col23, col24, col25, col26, col27, col28, col29, col32, col33, col34, col35, col36, col37, col38)
				SELECT dist_key+1, col1, col2, col3, col4, col5, col6, col70, col7, col8, col9, col10, col11, col12, col13, col14, col15, col16, col17, col18, col19, col20, col21, col22, col23, col24, col25, col26, col27, col28, col29, col32, col33, col34, col35, col36, col37, col38 FROM data_types_table ON CONFLICT (dist_key) DO UPDATE SET useless_column = 10;

SET client_min_messages TO ERROR;
DROP SCHEMA data_types CASCADE;
