--
-- Test loading data into columnar tables.
--

-- COPY with incorrect delimiter
\set contestants_1_csv_file :abs_srcdir '/data/contestants.1.csv'
\set client_side_copy_command '\\copy contestant FROM ' :'contestants_1_csv_file' ' WITH DELIMITER '''|''';'
:client_side_copy_command -- ERROR

-- COPY with invalid program
COPY contestant FROM PROGRAM 'invalid_program' WITH CSV; -- ERROR

-- COPY into uncompressed table from file
\set client_side_copy_command '\\copy contestant FROM ' :'contestants_1_csv_file' ' WITH CSV;'
:client_side_copy_command

-- COPY into uncompressed table from program
\set cat_contestants_2_csv_file 'cat ' :abs_srcdir '/data/contestants.2.csv'
COPY contestant FROM PROGRAM :'cat_contestants_2_csv_file' WITH CSV;

select
  version_major, version_minor, reserved_stripe_id, reserved_row_number
  from columnar_test_helpers.columnar_storage_info('contestant');

-- COPY into compressed table
\set client_side_copy_command '\\copy contestant_compressed FROM ' :'contestants_1_csv_file' ' WITH CSV;'
:client_side_copy_command

-- COPY into uncompressed table from program
COPY contestant_compressed FROM PROGRAM :'cat_contestants_2_csv_file'
	WITH CSV;

select
  version_major, version_minor, reserved_stripe_id, reserved_row_number
  from columnar_test_helpers.columnar_storage_info('contestant_compressed');

-- Test column list
CREATE TABLE famous_constants (id int, name text, value real)
    USING columnar;
COPY famous_constants (value, name, id) FROM STDIN WITH CSV;
3.141,pi,1
2.718,e,2
0.577,gamma,3
5.291e-11,bohr radius,4
\.

COPY famous_constants (name, value) FROM STDIN WITH CSV;
avagadro,6.022e23
electron mass,9.109e-31
proton mass,1.672e-27
speed of light,2.997e8
\.

SELECT * FROM famous_constants ORDER BY id, name;

SELECT * FROM columnar_test_helpers.chunk_group_consistency;

DROP TABLE famous_constants;
