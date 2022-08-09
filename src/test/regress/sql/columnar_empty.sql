--
-- Test different operations on empty columnar tables.
--

create table t_uncompressed(a int) using columnar;
create table t_compressed(a int) using columnar;

-- set options
ALTER TABLE t_compressed SET (columnar.compression = pglz);
ALTER TABLE t_compressed SET (columnar.stripe_row_limit = 2000);
ALTER TABLE t_compressed SET (columnar.chunk_group_row_limit = 1000);

SELECT * FROM columnar.options WHERE relation = 't_compressed'::regclass;

-- select
select * from t_uncompressed;
select count(*) from t_uncompressed;
select * from t_compressed;
select count(*) from t_compressed;

-- check storage
select
  version_major, version_minor, reserved_stripe_id, reserved_row_number
  from columnar_test_helpers.columnar_storage_info('t_compressed');
select
  version_major, version_minor, reserved_stripe_id, reserved_row_number
  from columnar_test_helpers.columnar_storage_info('t_uncompressed');

-- explain
explain (costs off, summary off, timing off) select * from t_uncompressed;
explain (costs off, summary off, timing off) select * from t_compressed;

-- vacuum
vacuum verbose t_compressed;
vacuum verbose t_uncompressed;

-- vacuum full
vacuum full t_compressed;
vacuum full t_uncompressed;

-- check storage
select
  version_major, version_minor, reserved_stripe_id, reserved_row_number
  from columnar_test_helpers.columnar_storage_info('t_compressed');
select
  version_major, version_minor, reserved_stripe_id, reserved_row_number
  from columnar_test_helpers.columnar_storage_info('t_uncompressed');

-- analyze
analyze t_uncompressed;
analyze t_compressed;

-- truncate
truncate t_uncompressed;
truncate t_compressed;

-- alter type
alter table t_uncompressed alter column a type text;
alter table t_compressed alter column a type text;

-- check storage
select
  version_major, version_minor, reserved_stripe_id, reserved_row_number
  from columnar_test_helpers.columnar_storage_info('t_compressed');
select
  version_major, version_minor, reserved_stripe_id, reserved_row_number
  from columnar_test_helpers.columnar_storage_info('t_uncompressed');

-- verify cost of scanning an empty table is zero, not NaN
explain table t_uncompressed;
explain table t_compressed;

-- drop
drop table t_compressed;
drop table t_uncompressed;
