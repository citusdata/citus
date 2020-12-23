--
-- Test different operations on empty columnar tables.
--

SET citus.compression to 'none';
create table t_uncompressed(a int) using columnar;
create table t_compressed(a int) using columnar;

-- set options
SELECT alter_columnar_table_set('t_compressed', compression => 'pglz');
SELECT alter_columnar_table_set('t_compressed', stripe_row_count => 100);
SELECT alter_columnar_table_set('t_compressed', chunk_row_count => 100);

SELECT * FROM columnar.options WHERE regclass = 't_compressed'::regclass;

-- select
select * from t_uncompressed;
select count(*) from t_uncompressed;
select * from t_compressed;
select count(*) from t_compressed;

-- explain
explain (costs off, summary off, timing off) select * from t_uncompressed;
explain (costs off, summary off, timing off) select * from t_compressed;

-- vacuum
vacuum verbose t_compressed;
vacuum verbose t_uncompressed;

-- vacuum full
vacuum full t_compressed;
vacuum full t_uncompressed;

-- analyze
analyze t_uncompressed;
analyze t_compressed;

-- truncate
truncate t_uncompressed;
truncate t_compressed;

-- alter type
alter table t_uncompressed alter column a type text;
alter table t_compressed alter column a type text;

-- drop
drop table t_compressed;
drop table t_uncompressed;
