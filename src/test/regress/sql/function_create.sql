\set VERBOSITY terse
CREATE SCHEMA function_create;
SET search_path TO function_create;
GRANT ALL ON SCHEMA function_create TO regularuser;

-- helper function to verify the function of a coordinator is the same on all workers
CREATE OR REPLACE FUNCTION verify_function_is_same_on_workers(funcname text)
    RETURNS bool
    LANGUAGE plpgsql
AS $func$
DECLARE
    coordinatorSql text;
    workerSql text;
BEGIN
    SELECT pg_get_functiondef(funcname::regprocedure) INTO coordinatorSql;
    FOR workerSql IN SELECT result FROM run_command_on_workers('SELECT pg_get_functiondef(' || quote_literal(funcname) || '::regprocedure)') LOOP
            IF workerSql != coordinatorSql THEN
                RAISE INFO 'functions are different, coordinator:% worker:%', coordinatorSql, workerSql;
                RETURN false;
            END IF;
        END LOOP;

    RETURN true;
END;
$func$;

-- test delegating function calls
CREATE TABLE warnings (
    id int primary key,
    message text
);

SELECT create_distributed_table('warnings', 'id');
INSERT INTO warnings VALUES (1, 'hello arbitrary config tests');

CREATE FUNCTION warning(int, text)
RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
    RAISE WARNING '%', $2;
END;
$$;

SELECT create_distributed_function('warning(int,text)','$1');

-- verify that the function definition is consistent in the cluster
SELECT verify_function_is_same_on_workers('function_create.warning(int,text)');

-- test a function that performs operation on the single shard of a reference table
CREATE TABLE monotonic_series(used_values int);
SELECT create_reference_table('monotonic_series');
INSERT INTO monotonic_series VALUES (1), (3), (5);

CREATE FUNCTION add_new_item_to_series()
RETURNS int
LANGUAGE SQL
AS $func$
INSERT INTO monotonic_series SELECT max(used_values)+1 FROM monotonic_series RETURNING used_values;
$func$;

-- Create and distribute a simple function
CREATE FUNCTION eq(macaddr, macaddr) RETURNS bool
    AS 'select $1 = $2;'
    LANGUAGE SQL
    IMMUTABLE
    RETURNS NULL ON NULL INPUT;

-- testing alter statements for a distributed function
-- ROWS 5, untested because;
-- ERROR:  ROWS is not applicable when function does not return a set
SELECT verify_function_is_same_on_workers('function_create.eq(macaddr,macaddr)');
ALTER FUNCTION eq(macaddr,macaddr) CALLED ON NULL INPUT IMMUTABLE SECURITY INVOKER PARALLEL UNSAFE COST 5;
SELECT verify_function_is_same_on_workers('function_create.eq(macaddr,macaddr)');
ALTER FUNCTION eq(macaddr,macaddr) RETURNS NULL ON NULL INPUT STABLE SECURITY DEFINER PARALLEL RESTRICTED;
SELECT verify_function_is_same_on_workers('function_create.eq(macaddr,macaddr)');
ALTER FUNCTION eq(macaddr,macaddr) STRICT VOLATILE PARALLEL SAFE;
SELECT verify_function_is_same_on_workers('function_create.eq(macaddr,macaddr)');

-- Test SET/RESET for alter function
ALTER ROUTINE eq(macaddr,macaddr) SET client_min_messages TO debug;
SELECT verify_function_is_same_on_workers('function_create.eq(macaddr,macaddr)');
ALTER FUNCTION eq(macaddr,macaddr) RESET client_min_messages;
SELECT verify_function_is_same_on_workers('function_create.eq(macaddr,macaddr)');
ALTER FUNCTION eq(macaddr,macaddr) SET search_path TO 'sch'';ma', public;
SELECT verify_function_is_same_on_workers('function_create.eq(macaddr,macaddr)');
ALTER FUNCTION eq(macaddr,macaddr) RESET search_path;

-- rename function and make sure the new name can be used on the workers
ALTER FUNCTION eq(macaddr,macaddr) RENAME TO eq2;
SELECT verify_function_is_same_on_workers('function_create.eq2(macaddr,macaddr)');

-- user-defined aggregates with & without strict
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

-- generate test data
create table aggdata (id int, key int, val int, valf float8);
select create_distributed_table('aggdata', 'id');
