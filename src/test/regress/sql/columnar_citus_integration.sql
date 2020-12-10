SET columnar.compression TO 'none';

CREATE SCHEMA columnar_citus_integration;
SET search_path TO columnar_citus_integration;
SET citus.next_shard_id TO 20090000;

SET citus.shard_replication_factor TO 1;
SET citus.shard_count TO 4;

-- table options integration testing
-- distributed table 1 placement
CREATE TABLE table_option (a int, b text) USING columnar;
SELECT create_distributed_table('table_option', 'a');

-- setting: compression
-- get baseline for setting
SELECT run_command_on_placements('table_option',$cmd$
  SELECT compression FROM columnar.options WHERE regclass = '%s'::regclass;
$cmd$);
-- change setting
SELECT alter_columnar_table_set('table_option', compression => 'pglz');
-- verify setting
SELECT run_command_on_placements('table_option',$cmd$
  SELECT compression FROM columnar.options WHERE regclass = '%s'::regclass;
$cmd$);
-- reset setting
SELECT alter_columnar_table_reset('table_option', compression => true);
-- verify setting
SELECT run_command_on_placements('table_option',$cmd$
  SELECT compression FROM columnar.options WHERE regclass = '%s'::regclass;
$cmd$);

-- setting: compression_level
-- get baseline for setting
SELECT run_command_on_placements('table_option',$cmd$
  SELECT compression_level FROM columnar.options WHERE regclass = '%s'::regclass;
$cmd$);
-- change setting
SELECT alter_columnar_table_set('table_option', compression_level => 13);
-- verify setting
SELECT run_command_on_placements('table_option',$cmd$
  SELECT compression_level FROM columnar.options WHERE regclass = '%s'::regclass;
$cmd$);
-- reset setting
SELECT alter_columnar_table_reset('table_option', compression_level => true);
-- verify setting
SELECT run_command_on_placements('table_option',$cmd$
  SELECT compression_level FROM columnar.options WHERE regclass = '%s'::regclass;
$cmd$);

-- setting: chunk_row_count
-- get baseline for setting
SELECT run_command_on_placements('table_option',$cmd$
  SELECT chunk_row_count FROM columnar.options WHERE regclass = '%s'::regclass;
$cmd$);
-- change setting
SELECT alter_columnar_table_set('table_option', chunk_row_count => 100);
-- verify setting
SELECT run_command_on_placements('table_option',$cmd$
  SELECT chunk_row_count FROM columnar.options WHERE regclass = '%s'::regclass;
$cmd$);
-- reset setting
SELECT alter_columnar_table_reset('table_option', chunk_row_count => true);
-- verify setting
SELECT run_command_on_placements('table_option',$cmd$
  SELECT chunk_row_count FROM columnar.options WHERE regclass = '%s'::regclass;
$cmd$);

-- setting: stripe_row_count
-- get baseline for setting
SELECT run_command_on_placements('table_option',$cmd$
  SELECT stripe_row_count FROM columnar.options WHERE regclass = '%s'::regclass;
$cmd$);
-- change setting
SELECT alter_columnar_table_set('table_option', stripe_row_count => 100);
-- verify setting
SELECT run_command_on_placements('table_option',$cmd$
  SELECT stripe_row_count FROM columnar.options WHERE regclass = '%s'::regclass;
$cmd$);
-- reset setting
SELECT alter_columnar_table_reset('table_option', stripe_row_count => true);
-- verify setting
SELECT run_command_on_placements('table_option',$cmd$
  SELECT stripe_row_count FROM columnar.options WHERE regclass = '%s'::regclass;
$cmd$);

-- verify settings are propagated when creating a table
CREATE TABLE table_option_2 (a int, b text) USING columnar;
SELECT alter_columnar_table_set('table_option_2',
                                chunk_row_count => 100,
                                stripe_row_count => 1000,
                                compression => 'pglz',
                                compression_level => 15);
SELECT create_distributed_table('table_option_2', 'a');

-- verify settings on placements
SELECT run_command_on_placements('table_option_2',$cmd$
  SELECT ROW(chunk_row_count, stripe_row_count, compression, compression_level) FROM columnar.options WHERE regclass = '%s'::regclass;
$cmd$);

DROP TABLE table_option, table_option_2;

-- verify settings get to all placements when there are multiple replica's
SET citus.shard_replication_factor TO 2;

-- table options integration testing
CREATE TABLE table_option (a int, b text) USING columnar;
SELECT create_distributed_table('table_option', 'a');

-- setting: compression
-- get baseline for setting
SELECT run_command_on_placements('table_option',$cmd$
  SELECT compression FROM columnar.options WHERE regclass = '%s'::regclass;
$cmd$);
-- change setting
SELECT alter_columnar_table_set('table_option', compression => 'pglz');
-- verify setting
SELECT run_command_on_placements('table_option',$cmd$
  SELECT compression FROM columnar.options WHERE regclass = '%s'::regclass;
$cmd$);
-- reset setting
SELECT alter_columnar_table_reset('table_option', compression => true);
-- verify setting
SELECT run_command_on_placements('table_option',$cmd$
  SELECT compression FROM columnar.options WHERE regclass = '%s'::regclass;
$cmd$);

-- setting: compression_level
-- get baseline for setting
SELECT run_command_on_placements('table_option',$cmd$
  SELECT compression_level FROM columnar.options WHERE regclass = '%s'::regclass;
$cmd$);
-- change setting
SELECT alter_columnar_table_set('table_option', compression_level => 17);
-- verify setting
SELECT run_command_on_placements('table_option',$cmd$
  SELECT compression_level FROM columnar.options WHERE regclass = '%s'::regclass;
$cmd$);
-- reset setting
SELECT alter_columnar_table_reset('table_option', compression_level => true);
-- verify setting
SELECT run_command_on_placements('table_option',$cmd$
  SELECT compression_level FROM columnar.options WHERE regclass = '%s'::regclass;
$cmd$);

-- setting: chunk_row_count
-- get baseline for setting
SELECT run_command_on_placements('table_option',$cmd$
  SELECT chunk_row_count FROM columnar.options WHERE regclass = '%s'::regclass;
$cmd$);
-- change setting
SELECT alter_columnar_table_set('table_option', chunk_row_count => 100);
-- verify setting
SELECT run_command_on_placements('table_option',$cmd$
  SELECT chunk_row_count FROM columnar.options WHERE regclass = '%s'::regclass;
$cmd$);
-- reset setting
SELECT alter_columnar_table_reset('table_option', chunk_row_count => true);
-- verify setting
SELECT run_command_on_placements('table_option',$cmd$
  SELECT chunk_row_count FROM columnar.options WHERE regclass = '%s'::regclass;
$cmd$);

-- setting: stripe_row_count
-- get baseline for setting
SELECT run_command_on_placements('table_option',$cmd$
  SELECT stripe_row_count FROM columnar.options WHERE regclass = '%s'::regclass;
$cmd$);
-- change setting
SELECT alter_columnar_table_set('table_option', stripe_row_count => 100);
-- verify setting
SELECT run_command_on_placements('table_option',$cmd$
  SELECT stripe_row_count FROM columnar.options WHERE regclass = '%s'::regclass;
$cmd$);
-- reset setting
SELECT alter_columnar_table_reset('table_option', stripe_row_count => true);
-- verify setting
SELECT run_command_on_placements('table_option',$cmd$
  SELECT stripe_row_count FROM columnar.options WHERE regclass = '%s'::regclass;
$cmd$);

-- verify settings are propagated when creating a table
CREATE TABLE table_option_2 (a int, b text) USING columnar;
SELECT alter_columnar_table_set('table_option_2',
                                chunk_row_count => 100,
                                stripe_row_count => 1000,
                                compression => 'pglz',
                                compression_level => 19);
SELECT create_distributed_table('table_option_2', 'a');

-- verify settings on placements
SELECT run_command_on_placements('table_option_2',$cmd$
  SELECT ROW(chunk_row_count, stripe_row_count, compression, compression_level) FROM columnar.options WHERE regclass = '%s'::regclass;
$cmd$);

DROP TABLE table_option, table_option_2;

-- test options on a reference table
CREATE TABLE table_option_reference (a int, b text) USING columnar;
SELECT create_reference_table('table_option_reference');

-- setting: compression
-- get baseline for setting
SELECT run_command_on_placements('table_option_reference',$cmd$
  SELECT compression FROM columnar.options WHERE regclass = '%s'::regclass;
$cmd$);
-- change setting
SELECT alter_columnar_table_set('table_option_reference', compression => 'pglz');
-- verify setting
SELECT run_command_on_placements('table_option_reference',$cmd$
  SELECT compression FROM columnar.options WHERE regclass = '%s'::regclass;
$cmd$);
-- reset setting
SELECT alter_columnar_table_reset('table_option_reference', compression => true);
-- verify setting
SELECT run_command_on_placements('table_option_reference',$cmd$
  SELECT compression FROM columnar.options WHERE regclass = '%s'::regclass;
$cmd$);

-- setting: compression_level
-- get baseline for setting
SELECT run_command_on_placements('table_option_reference',$cmd$
  SELECT compression_level FROM columnar.options WHERE regclass = '%s'::regclass;
$cmd$);
-- change setting
SELECT alter_columnar_table_set('table_option_reference', compression_level => 11);
-- verify setting
SELECT run_command_on_placements('table_option_reference',$cmd$
  SELECT compression_level FROM columnar.options WHERE regclass = '%s'::regclass;
$cmd$);
-- reset setting
SELECT alter_columnar_table_reset('table_option_reference', compression_level => true);
-- verify setting
SELECT run_command_on_placements('table_option_reference',$cmd$
  SELECT compression_level FROM columnar.options WHERE regclass = '%s'::regclass;
$cmd$);

-- setting: chunk_row_count
-- get baseline for setting
SELECT run_command_on_placements('table_option_reference',$cmd$
  SELECT chunk_row_count FROM columnar.options WHERE regclass = '%s'::regclass;
$cmd$);
-- change setting
SELECT alter_columnar_table_set('table_option_reference', chunk_row_count => 100);
-- verify setting
SELECT run_command_on_placements('table_option_reference',$cmd$
  SELECT chunk_row_count FROM columnar.options WHERE regclass = '%s'::regclass;
$cmd$);
-- reset setting
SELECT alter_columnar_table_reset('table_option_reference', chunk_row_count => true);
-- verify setting
SELECT run_command_on_placements('table_option_reference',$cmd$
  SELECT chunk_row_count FROM columnar.options WHERE regclass = '%s'::regclass;
$cmd$);

-- setting: stripe_row_count
-- get baseline for setting
SELECT run_command_on_placements('table_option_reference',$cmd$
  SELECT stripe_row_count FROM columnar.options WHERE regclass = '%s'::regclass;
$cmd$);
-- change setting
SELECT alter_columnar_table_set('table_option_reference', stripe_row_count => 100);
-- verify setting
SELECT run_command_on_placements('table_option_reference',$cmd$
  SELECT stripe_row_count FROM columnar.options WHERE regclass = '%s'::regclass;
$cmd$);
-- reset setting
SELECT alter_columnar_table_reset('table_option_reference', stripe_row_count => true);
-- verify setting
SELECT run_command_on_placements('table_option_reference',$cmd$
  SELECT stripe_row_count FROM columnar.options WHERE regclass = '%s'::regclass;
$cmd$);

-- verify settings are propagated when creating a table
CREATE TABLE table_option_reference_2 (a int, b text) USING columnar;
SELECT alter_columnar_table_set('table_option_reference_2',
                                chunk_row_count => 100,
                                stripe_row_count => 1000,
                                compression => 'pglz',
                                compression_level => 9);
SELECT create_reference_table('table_option_reference_2');

-- verify settings on placements
SELECT run_command_on_placements('table_option_reference_2',$cmd$
  SELECT ROW(chunk_row_count, stripe_row_count, compression, compression_level) FROM columnar.options WHERE regclass = '%s'::regclass;
$cmd$);

DROP TABLE table_option_reference, table_option_reference_2;

SET client_min_messages TO WARNING;
DROP SCHEMA columnar_citus_integration CASCADE;
