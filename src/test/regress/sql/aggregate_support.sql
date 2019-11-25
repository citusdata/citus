--
-- AGGREGATE SUPPORT
--
-- Tests support for user defined aggregates

create schema aggregate_support;
set search_path to aggregate_support;

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

create table aggdata (id int, key int, val int, valf float8);
select create_distributed_table('aggdata', 'id');
insert into aggdata (id, key, val, valf) values (1, 1, 2, 11.2), (2, 1, NULL, 2.1), (3, 2, 2, 3.22), (4, 2, 3, 4.23), (5, 2, 5, 5.25), (6, 3, 4, 63.4), (7, 5, NULL, 75), (8, 6, NULL, NULL), (9, 6, NULL, 96), (10, 7, 8, 1078), (11, 9, 0, 1.19);
select key, sum2(val), sum2_strict(val), stddev(valf)
from aggdata group by key order by key;

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

-- test aggregate with stype which is not a by-value datum
-- also test our handling of the aggregate not existing on workers
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

select sumstring(valf::text order by id)
from aggdata where valf is not null;
select create_distributed_function('sumstring(text)');
select sumstring(valf::text order by id)
from aggdata where valf is not null;

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
select run_command_on_workers($$
create user notsuper;
grant all on schema aggregate_support to notsuper;
grant all on all tables in schema aggregate_support to notsuper;
$$);
set role notsuper;
select array_collect_sort(val) from aggdata;
reset role;

set client_min_messages to error;
drop schema aggregate_support cascade;
