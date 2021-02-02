--
-- columnar_fallback_scan.sql
--
-- Test columnar.enable_custom_scan = false, which will use an
-- ordinary sequential scan. It won't benefit from projection pushdown
-- or qual pushdown, but it should return the correct results.
--

set columnar.enable_custom_scan = false;

create table fallback_scan(i int) using columnar;
-- large enough to test parallel_workers > 1
select alter_columnar_table_set('fallback_scan', compression => 'none');
insert into fallback_scan select generate_series(1,150000);
vacuum analyze fallback_scan;

select count(*), min(i), max(i), avg(i) from fallback_scan;

--
-- Negative test: try to force a parallel plan with at least two
-- workers, but columnar should reject it and use a non-parallel scan.
--
set force_parallel_mode = regress;
set min_parallel_table_scan_size = 1;
set parallel_tuple_cost = 0;
set max_parallel_workers = 4;
set max_parallel_workers_per_gather = 4;
explain (costs off) select count(*), min(i), max(i), avg(i) from fallback_scan;
select count(*), min(i), max(i), avg(i) from fallback_scan;

set force_parallel_mode to default;
set min_parallel_table_scan_size to default;
set parallel_tuple_cost to default;
set max_parallel_workers to default;
set max_parallel_workers_per_gather to default;

set columnar.enable_custom_scan to default;

drop table fallback_scan;
