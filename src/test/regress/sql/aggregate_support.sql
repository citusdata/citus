--
-- AGGREGATE SUPPORT
--
-- Tests support for user defined aggregates

create schema aggregate_support;
set search_path to aggregate_support;
set citus.coordinator_aggregation_strategy to 'disabled';

-- We test with & without STRICT as our code is responsible for managing these NULL checks
create function sum2_sfunc_strict(state int, x int)
returns int immutable strict language plpgsql as $$
begin return state + x;
end;
$$;

create function sum2_finalfunc_strict(state int)
returns int immutable strict language plpgsql as $$
begin return state * 2;
end;
$$;

create function sum2_sfunc(state int, x int)
returns int immutable language plpgsql as $$
begin return state + x;
end;
$$;

create function sum2_finalfunc(state int)
returns int immutable language plpgsql as $$
begin return state * 2;
end;
$$;

create aggregate sum2 (int) (
    sfunc = sum2_sfunc,
    stype = int,
    finalfunc = sum2_finalfunc,
    combinefunc = sum2_sfunc,
    initcond = '0'
);

create aggregate sum2_strict (int) (
    sfunc = sum2_sfunc_strict,
    stype = int,
    finalfunc = sum2_finalfunc_strict,
    combinefunc = sum2_sfunc_strict
);

select create_distributed_function('sum2(int)');
select create_distributed_function('sum2_strict(int)');

-- user-defined aggregates with multiple-parameters
create function psum_sfunc(s int, x int, y int)
returns int immutable language plpgsql as $$
begin return coalesce(s,0) + coalesce(x*y+3,1);
end;
$$;

create  function psum_sfunc_strict(s int, x int, y int)
returns int immutable strict language plpgsql as $$
begin return coalesce(s,0) + coalesce(x*y+3,1);
end;
$$;

create function psum_combinefunc(s1 int, s2 int)
returns int immutable language plpgsql as $$
begin return coalesce(s1,0) + coalesce(s2,0);
end;
$$;

create function psum_combinefunc_strict(s1 int, s2 int)
returns int immutable strict language plpgsql as $$
begin return coalesce(s1,0) + coalesce(s2,0);
end;
$$;

create function psum_finalfunc(x int)
returns int immutable language plpgsql as $$
begin return x * 2;
end;
$$;

create function psum_finalfunc_strict(x int)
returns int immutable strict language plpgsql as $$
begin return x * 2;
end;
$$;

create aggregate psum(int, int)(
    sfunc=psum_sfunc,
    combinefunc=psum_combinefunc,
    finalfunc=psum_finalfunc,
    stype=int
);

create aggregate psum_strict(int, int)(
    sfunc=psum_sfunc_strict,
    combinefunc=psum_combinefunc_strict,
    finalfunc=psum_finalfunc_strict,
    stype=int,
    initcond=0
);

select create_distributed_function('psum(int,int)');
select create_distributed_function('psum_strict(int,int)');

-- generate test data
create table aggdata (id int, key int, val int, valf float8);
select create_distributed_table('aggdata', 'id');
insert into aggdata (id, key, val, valf) values (1, 1, 2, 11.2), (2, 1, NULL, 2.1), (3, 2, 2, 3.22), (4, 2, 3, 4.23), (5, 2, 5, 5.25), (6, 3, 4, 63.4), (7, 5, NULL, 75), (8, 6, NULL, NULL), (9, 6, NULL, 96), (10, 7, 8, 1078), (11, 9, 0, 1.19);

select key, sum2(val), sum2_strict(val), stddev(valf)::numeric(10,5), psum(val, valf::int), psum_strict(val, valf::int) from aggdata group by key order by key;
-- FILTER supported
select key, sum2(val) filter (where valf < 5), sum2_strict(val) filter (where valf < 5) from aggdata group by key order by key;
-- DISTINCT unsupported, unless grouped by partition key
select key, sum2(distinct val), sum2_strict(distinct val), psum(distinct val, valf::int), psum_strict(distinct val, valf::int) from aggdata group by key order by key;
select id, sum2(distinct val), sum2_strict(distinct val), psum(distinct val, valf::int), psum_strict(distinct val, valf::int) from aggdata group by id order by id;
-- ORDER BY unsupported
select key, sum2(val order by valf), sum2_strict(val order by valf), psum(val, valf::int order by valf), psum_strict(val, valf::int order by valf) from aggdata group by key order by key;
-- Test handling a lack of intermediate results
select sum2(val), sum2_strict(val), psum(val, valf::int), psum_strict(val, valf::int) from aggdata where valf = 0;
-- Test HAVING
select key, stddev(valf)::numeric(10,5) from aggdata group by key having stddev(valf) > 2 order by key;
select key, stddev(valf)::numeric(10,5) from aggdata group by key having stddev(val::float8) > 1 order by key;
select key, corr(valf,valf+val)::numeric(10,5) from aggdata group by key having corr(valf,valf+val) < 1 order by key;
select key, corr(valf,valf+val)::numeric(10,5) from aggdata group by key having corr(valf::float8,valf+val) < 1 order by key;
-- Previously aggregated on master, pushed down to workers with multi-parameter support
select floor(val/2), corr(valf, valf + val)::numeric(10,5) from aggdata group by floor(val/2) order by 1;
select floor(val/2), corr(valf, valf + val)::numeric(10,5) from aggdata group by floor(val/2) having corr(valf + val, val) < 1 order by 1;

-- built-in binary aggregates for statistics
select regr_count(valf,val)::numeric(10,5)from aggdata;
select regr_sxx(valf,val)::numeric(10,5) from aggdata;
select regr_syy(valf,val)::numeric(10,3) from aggdata;
select regr_sxy(valf,val)::numeric(10,5) from aggdata;
select regr_avgx(valf,val)::numeric(10,5), regr_avgy(valf,val)::numeric(10,5) from aggdata;
select regr_r2(valf,val)::numeric(10,5) from aggdata;
select regr_slope(valf,val)::numeric(10,5), regr_intercept(valf,val)::numeric(10,5) from aggdata;
select covar_pop(valf,val)::numeric(10,5), covar_samp(valf,val)::numeric(10,5) from aggdata;

-- binary string aggregation
create function binstragg_sfunc(s text, e1 text, e2 text)
returns text immutable language plpgsql as $$
begin case when coalesce(e1,'') > coalesce(s,'') and coalesce(e1,'') > coalesce(e2,'') then return e1;
           when coalesce(e2,'') > coalesce(s,'') and coalesce(e2,'') > coalesce(e1,'') then return e2;
           else return s;
      end case;
end;
$$;

create function binstragg_combinefunc(s1 text, s2 text)
returns text immutable language plpgsql as $$
begin if coalesce(s1,'') > coalesce(s2,'') then return s1; else return s2; end if;
end;
$$;

create aggregate binstragg(text, text)(
    sfunc=binstragg_sfunc,
    combinefunc=binstragg_combinefunc,
    stype=text
);
-- verify that the aggregate is added into pg_dist_object, on each worker
SELECT run_command_on_workers($$SELECT count(*) from pg_catalog.pg_dist_object where objid = 'aggregate_support.binstragg'::regproc;$$);

SELECT run_command_on_workers($$select count(*) from pg_aggregate where aggfnoid::text like '%binstragg%';$$);

select create_distributed_function('binstragg(text,text)');

create table txttbl(id int, col1 text, col2 text);

select create_distributed_table('txttbl', 'id');

insert into txttbl values (1, 'aaaa', 'bbbb'), (2, 'cccc', 'dddd'), (3, 'eeee', 'ffff'), (4, 'gggg', 'hhhh'), (5, 'iiii', 'jjjj'), (6, 'kkkk', 'llll'), (7, 'mmmm', 'nnnn'), (8, 'oooo', 'pppp'), (9, 'qqqq', 'rrrr'), (10, 'ssss', 'tttt'), (11, 'uuuu', 'vvvv'), (12, 'wwww', 'xxxx'), (13, 'yyyy', 'zzzz');

select binstragg(col1, col2) from txttbl;

create table users_table (user_id int, time timestamp, value_1 int, value_2 int, value_3 float, value_4 bigint);
select create_distributed_table('users_table', 'user_id');

create table events_table (user_id int, time timestamp, event_type int, value_2 int, value_3 float, value_4 bigint);
select create_distributed_table('events_table', 'user_id');


insert into users_table select i % 10000, timestamp '2014-01-10 20:00:00' +
       i * (timestamp '2014-01-20 20:00:00' -
                   timestamp '2014-01-10 10:00:00'),i, i % 100, i % 5 from generate_series(0, 1000) i;


insert into events_table select i % 10000, timestamp '2014-01-10 20:00:00' +
       i * (timestamp '2014-01-20 20:00:00' -
                   timestamp '2014-01-10 10:00:00'),i, i % 100, i % 5 from generate_series(0, 10000) i;

-- query with window functions, the agg. inside the window functions
select value_3, to_char(date_trunc('day', time), 'YYYYMMDD') as time, rank() over my_win as my_rank
from events_table
group by
    value_3, date_trunc('day', time)
    WINDOW my_win as (partition by regr_syy(event_type%10, value_2)::int order by count(*) desc)
order by 1,2,3
limit 5;

-- query with window functions, the agg. outside the window functions
select regr_syy(event_type%10, value_2)::int, value_3, to_char(date_trunc('day', time), 'YYYYMMDD') as time, rank() over my_win as my_rank
from events_table
group by
    value_3, date_trunc('day', time)
    WINDOW my_win as (partition by value_3 order by count(*) desc)
order by 1,2,3
limit 5;

-- query with only order by
select regr_syy(event_type%10, value_2)::int
from events_table
order by 1 desc;

-- query with group by + target list + order by
select count(*), regr_syy(event_type%10, value_2)::int
from events_table
group by value_3
order by 2 desc;

-- query with group by + order by
select count(*)
from events_table
group by value_3
order by regr_syy(event_type%10, value_2)::int desc;

-- query with basic join
select regr_syy(u1.user_id, u2.user_id)::int
from users_table  u1, events_table u2
where u1.user_id = u2.user_id;

-- agg. with filter with columns
select regr_syy(u1.user_id, u2.user_id) filter (where u1.value_1 < 5)::numeric(10,3)
from users_table  u1, events_table u2
where u1.user_id = u2.user_id;

-- agg with filter and group by
select regr_syy(u1.user_id, u2.user_id) filter (where (u1.value_1) < 5)::numeric(10,3)
from users_table  u1, events_table u2
where u1.user_id = u2.user_id
group by u1.value_3;

-- agg. with filter with consts
select regr_syy(u1.user_id, u2.user_id) filter (where '0300030' LIKE '%3%')::int
from users_table  u1, events_table u2
where u1.user_id = u2.user_id;

-- multiple aggs with filters
select regr_syy(u1.user_id, u2.user_id) filter (where u1.value_1 < 5)::numeric(10,3), regr_syy(u1.value_1, u2.value_2) filter (where u1.user_id < 5)::numeric(10,3)
from users_table  u1, events_table u2
where u1.user_id = u2.user_id;

-- query with where false
select regr_syy(u1.user_id, u2.user_id) filter (where u1.value_1 < 5)::numeric(10,3), regr_syy(u1.value_1, u2.value_2) filter (where u1.user_id < 5)::numeric(10,3)
from users_table  u1, events_table u2
where 1=0;

-- a CTE forced to be planned recursively (via OFFSET 0)
with cte_1 as
(
    select
        regr_syy(u1.user_id, u2.user_id) filter (where u1.value_1 < 5)::numeric(10,3), regr_syy(u1.value_1, u2.value_2) filter (where u1.user_id < 5)::numeric(10,3)
    from users_table  u1, events_table u2
    where u1.user_id = u2.user_id
    OFFSET 0
)
select
*
from
cte_1;

-- Test https://github.com/citusdata/citus/issues/3446
set citus.coordinator_aggregation_strategy to 'row-gather';
select id, stddev(val) from aggdata group by id order by 1;
set citus.coordinator_aggregation_strategy to 'disabled';

-- test polymorphic aggregates from https://github.com/citusdata/citus/issues/2397
-- we do not currently support pseudotypes for transition types, so this errors for now
CREATE OR REPLACE FUNCTION first_agg(anyelement, anyelement)
RETURNS anyelement AS $$
	SELECT CASE WHEN $1 IS NULL THEN $2 ELSE $1 END;
$$ LANGUAGE SQL STABLE;

CREATE AGGREGATE first (
	sfunc    = first_agg,
	basetype = anyelement,
	stype    = anyelement,
	combinefunc = first_agg
);

CREATE OR REPLACE FUNCTION last_agg(anyelement, anyelement)
RETURNS anyelement AS $$
	SELECT $2;
$$ LANGUAGE SQL STABLE;

CREATE AGGREGATE last (
	sfunc    = last_agg,
	basetype = anyelement,
	stype    = anyelement,
	combinefunc = last_agg
);

SELECT create_distributed_function('first(anyelement)');
SELECT create_distributed_function('last(anyelement)');

SELECT key, first(val ORDER BY id), last(val ORDER BY id)
FROM aggdata GROUP BY key ORDER BY key;

-- However, GROUP BY on distribution column gets pushed down
SELECT id, first(val ORDER BY key), last(val ORDER BY key)
FROM aggdata GROUP BY id ORDER BY id;

-- Test that expressions don't slip past. This fails
SELECT id%5, first(val ORDER BY key), last(val ORDER BY key)
FROM aggdata GROUP BY id%5 ORDER BY id%5;

-- test aggregate with stype which is not a by-value datum
create function sumstring_sfunc(state text, x text)
returns text immutable language plpgsql as $$
begin return (state::float8 + x::float8)::text;
end;
$$;

create aggregate sumstring(text) (
	sfunc = sumstring_sfunc,
	stype = text,
	combinefunc = sumstring_sfunc,
	initcond = '0'
);
-- verify that the aggregate is propagated
select aggfnoid from pg_aggregate where aggfnoid::text like '%sumstring%';
SELECT run_command_on_workers($$select aggfnoid from pg_aggregate where aggfnoid::text like '%sumstring%';$$);

select create_distributed_function('sumstring(text)');
select sumstring(valf::text) from aggdata where valf is not null;

-- test aggregate with stype that has an expanded read-write form
CREATE FUNCTION array_sort (int[])
RETURNS int[] LANGUAGE SQL AS $$
    SELECT ARRAY(SELECT unnest($1) ORDER BY 1)
$$;

create aggregate array_collect_sort(el int) (
	sfunc = array_append,
	stype = int[],
	combinefunc = array_cat,
	finalfunc = array_sort,
	initcond = '{}'
);
select create_distributed_function('array_collect_sort(int)');

select array_collect_sort(val) from aggdata;

-- Test multiuser scenario
create user notsuper;
grant all on schema aggregate_support to notsuper;
grant all on all tables in schema aggregate_support to notsuper;
select 1 from run_command_on_workers($$
grant all on schema aggregate_support to notsuper;
grant all on all tables in schema aggregate_support to notsuper;
$$);
set role notsuper;
select array_collect_sort(val) from aggdata;
reset role;
drop owned by notsuper;
drop user notsuper;

-- Test aggregation on coordinator
set citus.coordinator_aggregation_strategy to 'row-gather';

select key, first(val order by id), last(val order by id)
from aggdata group by key order by key;

select key, sum2(distinct val), sum2_strict(distinct val) from aggdata group by key order by key;
select key, sum2(val order by valf), sum2_strict(val order by valf) from aggdata group by key order by key;
select string_agg(distinct floor(val/2)::text, '|' order by floor(val/2)::text) from aggdata;
select string_agg(distinct floor(val/2)::text, '|' order by floor(val/2)::text) filter (where val < 5) from aggdata;
select mode() within group (order by floor(val/2)) from aggdata;
select percentile_cont(0.5) within group(order by valf) from aggdata;
select key, percentile_cont(key/10.0) within group(order by val) from aggdata group by key;
select array_agg(val order by valf) from aggdata;

-- test by using some other node types as arguments to agg
select key, percentile_cont((key - (key > 4)::int) / 10.0) within group(order by val) from aggdata group by key;

-- Test TransformSubqueryNode

select * FROM (
    SELECT key, mode() within group (order by floor(agg1.val/2)) m from aggdata agg1
    group by key
) subq ORDER BY 2, 1 LIMIT 5;

select * FROM (
    SELECT key k, avg(distinct floor(agg1.val/2)) m from aggdata agg1
    group by key
) subq
order by k,m;

-- Test TransformsSubqueryNode with group by not in FROM (failed in past)
select count(*) FROM (
    SELECT avg(distinct floor(agg1.val/2)) m from aggdata agg1
    group by key
) subq;


select key, count(distinct aggdata)
from aggdata group by key order by 1, 2;

-- GROUPING parses to GroupingFunc, distinct from Aggref
-- These three queries represent edge cases implementation would have to consider
-- For now we error out of all three
select grouping(id)
from aggdata group by id order by 1 limit 3;

select key, grouping(val)
from aggdata group by key, val order by 1, 2;

select key, grouping(val), sum(distinct valf)
from aggdata group by key, val order by 1, 2;


-- Test https://github.com/citusdata/citus/issues/3328
create table nulltable(id int);
insert into nulltable values (0);
-- These cases are not type correct
select pg_catalog.worker_partial_agg('string_agg(text,text)'::regprocedure, id) from nulltable;
select pg_catalog.worker_partial_agg('sum(int8)'::regprocedure, id) from nulltable;
select pg_catalog.coord_combine_agg('sum(float8)'::regprocedure, id::text::cstring, null::text) from nulltable;
select pg_catalog.coord_combine_agg('avg(float8)'::regprocedure, ARRAY[id,id,id]::text::cstring, null::text) from nulltable;
-- These cases are type correct
select pg_catalog.worker_partial_agg('sum(int)'::regprocedure, id) from nulltable;
select pg_catalog.coord_combine_agg('sum(float8)'::regprocedure, id::text::cstring, null::float8) from nulltable;
select pg_catalog.coord_combine_agg('avg(float8)'::regprocedure, ARRAY[id,id,id]::text::cstring, null::float8) from nulltable;


-- Test that we don't crash with empty resultset
-- See https://github.com/citusdata/citus/issues/3953
CREATE TABLE t1 (a int PRIMARY KEY, b int);
CREATE TABLE t2 (a int PRIMARY KEY, b int);
SELECT create_distributed_table('t1','a');
SELECT 'foo' as foo, count(distinct b) FROM t1;
SELECT 'foo' as foo, count(distinct b) FROM t2;
SELECT 'foo' as foo, string_agg(distinct a::character varying, ',') FROM t1;
SELECT 'foo' as foo, string_agg(distinct a::character varying, ',') FROM t2;


CREATE OR REPLACE FUNCTION const_function(int)
RETURNS int STABLE
LANGUAGE plpgsql
AS $function$
BEGIN
RAISE NOTICE 'stable_fn called';
RETURN 1;
END;
$function$;

CREATE OR REPLACE FUNCTION square_func_stable(int)
RETURNS int STABLE
LANGUAGE plpgsql
AS $function$
BEGIN
RETURN $1 * $1;
END;
$function$;

SET citus.enable_metadata_sync TO OFF;
CREATE OR REPLACE FUNCTION square_func(int)
RETURNS int
LANGUAGE plpgsql
AS $function$
BEGIN
RETURN $1 * $1;
END;
$function$;
RESET citus.enable_metadata_sync;

SELECT const_function(1), string_agg(a::character, ',') FROM t1;
SELECT const_function(1), count(b) FROM t1;
SELECT const_function(1), count(b), 10 FROM t1;
SELECT const_function(1), count(b), const_function(10) FROM t1;
SELECT square_func(5), string_agg(a::character, ','),const_function(1) FROM t1;
SELECT square_func_stable(5), string_agg(a::character, ','),const_function(1) FROM t1;

-- this will error since the expression will be
-- pushed down (group by) and the function doesn't exist on workers
SELECT square_func(5), a FROM t1 GROUP BY a;
-- this will error since it has group by even though there is an aggregation
-- the expression will be pushed down.
SELECT square_func(5), a, count(a) FROM t1 GROUP BY a;

-- Test the cases where the worker agg exec. returns no tuples.

CREATE TABLE dist_table (dist_col int, agg_col numeric);
SELECT create_distributed_table('dist_table', 'dist_col');

CREATE TABLE ref_table (int_col int);
SELECT create_reference_table('ref_table');

SELECT PERCENTILE_DISC(.25) WITHIN GROUP (ORDER BY agg_col)
FROM dist_table
LEFT JOIN ref_table ON TRUE;

SELECT PERCENTILE_DISC(.25) WITHIN GROUP (ORDER BY agg_col)
FROM (SELECT *, random() FROM dist_table) a;

SELECT PERCENTILE_DISC((2 > random())::int::numeric / 10) WITHIN GROUP (ORDER BY agg_col)
FROM dist_table
LEFT JOIN ref_table ON TRUE;

SELECT SUM(COALESCE(agg_col, 3))
FROM dist_table
LEFT JOIN ref_table ON TRUE;

SELECT AVG(COALESCE(agg_col, 10))
FROM dist_table
LEFT JOIN ref_table ON TRUE;

insert into dist_table values (2, 11.2), (3, NULL), (6, 3.22), (3, 4.23), (5, 5.25), (4, 63.4), (75, NULL), (80, NULL), (96, NULL), (8, 1078), (0, 1.19);

-- run the same queries after loading some data
SELECT PERCENTILE_DISC(.25) WITHIN GROUP (ORDER BY agg_col)
FROM dist_table
LEFT JOIN ref_table ON TRUE;

SELECT PERCENTILE_DISC(.25) WITHIN GROUP (ORDER BY agg_col)
FROM (SELECT *, random() FROM dist_table) a;

SELECT PERCENTILE_DISC((2 > random())::int::numeric / 10) WITHIN GROUP (ORDER BY agg_col)
FROM dist_table
LEFT JOIN ref_table ON TRUE;

SELECT floor(SUM(COALESCE(agg_col, 3)))
FROM dist_table
LEFT JOIN ref_table ON TRUE;

SELECT floor(AVG(COALESCE(agg_col, 10)))
FROM dist_table
LEFT JOIN ref_table ON TRUE;

-- try createing aggregate having non-distributable dependency type
create table dummy_tbl (a int);
create function dummy_fnc(a dummy_tbl, d double precision) RETURNS dummy_tbl
    AS $$SELECT 1;$$ LANGUAGE sql;
-- should give warning and create aggregate local only
create aggregate dependent_agg (float8) (stype=dummy_tbl, sfunc=dummy_fnc);

-- clear and try again with distributed table
DROP TABLE dummy_tbl CASCADE;

create table dummy_tbl (a int);
SELECT create_distributed_table('dummy_tbl','a');
create function dummy_fnc(a dummy_tbl, d double precision) RETURNS dummy_tbl
    AS $$SELECT 1;$$ LANGUAGE sql;

DROP TABLE dummy_tbl CASCADE;

-- Show that polymorphic aggregates with zero-argument works
CREATE FUNCTION stfnp_zero_arg(int[]) RETURNS int[] AS
'select $1' LANGUAGE SQL;

CREATE FUNCTION ffp_zero_arg(anyarray) RETURNS anyarray AS
'select $1' LANGUAGE SQL;

CREATE AGGREGATE zero_arg_agg(*) (SFUNC = stfnp_zero_arg, STYPE = int4[],
  FINALFUNC = ffp_zero_arg, INITCOND = '{}');

CREATE TABLE zero_arg_agg_table(f1 int, f2 int[]);
SELECT create_distributed_table('zero_arg_agg_table','f1');
INSERT INTO zero_arg_agg_table VALUES(1, array[1]);
INSERT INTO zero_arg_agg_table VALUES(1, array[11]);

SELECT zero_arg_agg(*) from zero_arg_agg_table;

-- Show that after dropping a table on which functions and aggregates depending on
-- pg_dist_object is consistent on coordinator and worker node.
SELECT pg_identify_object_as_address(classid, objid, objsubid)::text
FROM pg_catalog.pg_dist_object
    EXCEPT
SELECT unnest(result::text[]) AS unnested_result
FROM run_command_on_workers($$SELECT array_agg(pg_identify_object_as_address(classid, objid, objsubid)) from pg_catalog.pg_dist_object$$);

CREATE AGGREGATE newavg (
   sfunc = int4_avg_accum, basetype = int4, stype = _int8,
   finalfunc = int8_avg,
   initcond1 = '{0,0}'
);

SELECT run_command_on_workers($$select aggfnoid from pg_aggregate where aggfnoid::text like '%newavg%';$$);

CREATE TYPE coord AS (x int, y int);

CREATE FUNCTION coord_minx_sfunc(state coord, new coord)
returns coord immutable language plpgsql as $$
BEGIN
   IF (state IS NULL OR new.x < state.x) THEN
     RETURN new;
   ELSE
     RETURN state;
   END IF;
END
$$;

create function coord_minx_finalfunc(state coord)
returns coord immutable language plpgsql as $$
begin return state;
end;
$$;

-- custom aggregate that has the same name as a built-in function, but with a combinefunc
create aggregate min (coord) (
    sfunc = coord_minx_sfunc,
    stype = coord,
    finalfunc = coord_minx_finalfunc,
    combinefunc = coord_minx_sfunc
);

select min((id,val)::coord) from aggdata;

set client_min_messages to error;
drop schema aggregate_support cascade;
