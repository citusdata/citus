create schema functions_pushdown;
set search_path to functions_pushdown;
set citus.shard_replication_factor TO 1;

create table reference_table( id bigint primary key, t text);
select create_reference_table('reference_table');

create table distributed_table( id bigint primary key
  , fk_id bigint REFERENCES reference_table (id)
  , t text
);
select create_distributed_table('distributed_table', 'id');

CREATE FUNCTION reference_function_sql(i_id bigint)
RETURNS bigint LANGUAGE sql AS
$function$
SELECT count(*) FROM functions_pushdown.reference_table where id = i_id;
$function$;

CREATE FUNCTION reference_function_plpgsql(i_id bigint)
RETURNS bigint LANGUAGE plpgsql AS
$function$
DECLARE
    result bigint; -- Variable to hold the count
BEGIN
    -- Fetch the count into the variable
    SELECT count(*) INTO result
    FROM functions_pushdown.reference_table
    WHERE id = i_id;
    -- Return the result
    RETURN result;
END;
$function$;

CREATE FUNCTION distributed_function_sql(i_id bigint)
RETURNS bigint LANGUAGE sql AS
$function$
SELECT count(*) FROM functions_pushdown.distributed_table where id = i_id;
$function$;

CREATE FUNCTION distributed_function_plpgsql(i_id bigint)
RETURNS bigint LANGUAGE plpgsql AS
$function$
DECLARE
    result bigint; -- Variable to hold the count
BEGIN
    -- Fetch the count into the variable
    SELECT count(*) INTO result
    FROM functions_pushdown.distributed_table
    WHERE id = i_id;
    -- Return the result
    RETURN result;
END;
$function$;

select create_distributed_function('distributed_function_sql(bigint)', '$1'
            , colocate_with := 'distributed_table');
select create_distributed_function('distributed_function_plpgsql(bigint)', '$1'
            , colocate_with := 'distributed_table');

insert into reference_table values (1, 'a');
insert into reference_table values (2, 'b');
insert into reference_table values (3, 'c');

insert into distributed_table values (1, 1, 'aa');
insert into distributed_table values (2, 2, 'bb');
insert into distributed_table values (3, 3, 'cc');
insert into distributed_table values (4, 2, 'BB');

-- REFERENCE
select *,reference_function_sql(id) from reference_table order by id;
select *,reference_function_sql(id) from reference_table where id = 1;

select *,reference_function_plpgsql(id) from reference_table order by id;
select *,reference_function_plpgsql(id) from reference_table where id = 1;

select *,reference_function_sql(id) from distributed_table order by id;
select *,reference_function_sql(id) from distributed_table where id = 1;
select *,reference_function_sql(id) from distributed_table where id = 4;

select *,reference_function_plpgsql(id) from distributed_table order by id;
select *,reference_function_plpgsql(id) from distributed_table where id = 1;
select *,reference_function_plpgsql(id) from distributed_table where id = 4;

-- DISTRIBUTE (not supported yet)
select *,distributed_function_sql(id) from reference_table order by id;
select *,distributed_function_sql(id) from reference_table where id = 1;

select *,distributed_function_plpgsql(id) from reference_table order by id;
select *,distributed_function_plpgsql(id) from reference_table where id = 1;

select *,distributed_function_sql(id) from distributed_table order by id;
select *,distributed_function_sql(id) from distributed_table where id = 1;

select *,distributed_function_plpgsql(id) from distributed_table order by id;
select *,distributed_function_plpgsql(id) from distributed_table where id = 1;

-- some other checks (all should pass)
select reference_function_sql(1);
select reference_function_sql(2);
select reference_function_sql(3);
select reference_function_sql(4);

select reference_function_plpgsql(1);
select reference_function_plpgsql(2);
select reference_function_plpgsql(3);
select reference_function_plpgsql(4);

select distributed_function_sql(1);
select distributed_function_sql(2);
select distributed_function_sql(3);
select distributed_function_sql(4);

select distributed_function_plpgsql(1);
select distributed_function_plpgsql(2);
select distributed_function_plpgsql(3);
select distributed_function_plpgsql(4);

-- https://github.com/citusdata/citus/issues/5887
CREATE TABLE functions_pushdown.test(a int);
SELECT create_distributed_table('functions_pushdown.test', 'a');
INSERT INTO functions_pushdown.test SELECT i FROM generate_series(1,10)i;

CREATE OR REPLACE FUNCTION some_func() RETURNS integer
    AS 'select count(*) FROM functions_pushdown.test;'
    LANGUAGE SQL
    VOLATILE
    RETURNS NULL ON NULL INPUT;

SELECT some_func() FROM functions_pushdown.test;

drop table distributed_table;
drop table reference_table;
drop schema functions_pushdown cascade;
set citus.shard_replication_factor TO 2;
