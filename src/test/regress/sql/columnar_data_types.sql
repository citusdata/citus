--
-- Test loading and reading different data types to/from columnar foreign tables.
--


-- Settings to make the result deterministic
SET datestyle = "ISO, YMD";
SET timezone to 'GMT';
SET intervalstyle TO 'POSTGRES_VERBOSE';


-- Test array types
CREATE TABLE test_array_types (int_array int[], bigint_array bigint[],
	text_array text[]) USING columnar;

\set array_types_csv_file :abs_srcdir '/data/array_types.csv'
\set client_side_copy_command '\\copy test_array_types FROM ' :'array_types_csv_file' ' WITH CSV;'
:client_side_copy_command

SELECT * FROM test_array_types;


-- Test date/time types
CREATE TABLE test_datetime_types (timestamp timestamp,
	timestamp_with_timezone timestamp with time zone, date date, time time,
	interval interval) USING columnar;

\set datetime_types_csv_file :abs_srcdir '/data/datetime_types.csv'
\set client_side_copy_command '\\copy test_datetime_types FROM ' :'datetime_types_csv_file' ' WITH CSV;'
:client_side_copy_command

SELECT * FROM test_datetime_types;


-- Test enum and composite types
CREATE TYPE enum_type AS ENUM ('a', 'b', 'c');
CREATE TYPE composite_type AS (a int, b text);

CREATE TABLE test_enum_and_composite_types (enum enum_type,
	composite composite_type) USING columnar;

\set enum_and_composite_types_csv_file :abs_srcdir '/data/enum_and_composite_types.csv'
COPY test_enum_and_composite_types FROM
	:'enum_and_composite_types_csv_file' WITH CSV;

SELECT * FROM test_enum_and_composite_types;


-- Test range types
CREATE TABLE test_range_types (int4range int4range, int8range int8range,
	numrange numrange, tsrange tsrange) USING columnar;

\set range_types_csv_file :abs_srcdir '/data/range_types.csv'
\set client_side_copy_command '\\copy test_range_types FROM ' :'range_types_csv_file' ' WITH CSV;'
:client_side_copy_command

SELECT * FROM test_range_types;


-- Test other types
CREATE TABLE test_other_types (bool boolean, bytea bytea, money money,
	inet inet, bitstring bit varying(5), uuid uuid, json json) USING columnar;

\set other_types_csv_file :abs_srcdir '/data/other_types.csv'
\set client_side_copy_command '\\copy test_other_types FROM ' :'other_types_csv_file' ' WITH CSV;'
:client_side_copy_command

SELECT * FROM test_other_types;


-- Test null values
CREATE TABLE test_null_values (a int, b int[], c composite_type)
	USING columnar;

\set null_values_csv_file :abs_srcdir '/data/null_values.csv'
\set client_side_copy_command '\\copy test_null_values FROM ' :'null_values_csv_file' ' WITH CSV;'
:client_side_copy_command

SELECT * FROM test_null_values;

CREATE TABLE test_json(j json) USING columnar;
INSERT INTO test_json SELECT ('{"att": ' || g::text || '}')::json from generate_series(1,1000000) g;
SELECT * FROM test_json WHERE (j->'att')::text::int8 > 999990;
DROP TABLE test_json;
