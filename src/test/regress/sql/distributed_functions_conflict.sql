-- This is designed to test worker_create_or_replace_object in PG11 with aggregates
-- Note in PG12 we use CREATE OR REPLACE AGGREGATE, thus the renaming does not occur

CREATE SCHEMA proc_conflict;
SELECT run_command_on_workers($$CREATE SCHEMA proc_conflict;$$);

\c - - :public_worker_1_host :worker_1_port
SET search_path TO proc_conflict;
CREATE FUNCTION existing_func(state int, i int) RETURNS int AS $$
BEGIN
    RETURN state * 2 + i;
END;
$$ LANGUAGE plpgsql STRICT IMMUTABLE;
CREATE AGGREGATE existing_agg(int) (
    SFUNC = existing_func,
    STYPE = int
);

\c - - :master_host :master_port
SET search_path TO proc_conflict;

CREATE FUNCTION existing_func(state int, i int) RETURNS int AS $$
BEGIN
    RETURN state * i + i;
END;
$$ LANGUAGE plpgsql STRICT IMMUTABLE;
CREATE AGGREGATE existing_agg(int) (
    SFUNC = existing_func,
    STYPE = int
);

SELECT create_distributed_function('existing_agg(int)');

\c - - :public_worker_1_host :worker_1_port
SET search_path TO proc_conflict;

WITH data (val) AS (
    select 2
    union all select 4
    union all select 6
)
SELECT existing_agg(val) FROM data;

\c - - :master_host :master_port
SET search_path TO proc_conflict;

WITH data (val) AS (
    select 2
    union all select 4
    union all select 6
)
SELECT existing_agg(val) FROM data;

-- Now drop & recreate in order to make sure rename detects the existing renamed objects
-- hide cascades
SET client_min_messages TO error;
DROP AGGREGATE existing_agg(int) CASCADE;
DROP FUNCTION existing_func(int, int) CASCADE;

\c - - :public_worker_1_host :worker_1_port
SET search_path TO proc_conflict;

CREATE FUNCTION existing_func(state int, i int) RETURNS int AS $$
BEGIN
    RETURN state * 3 + i;
END;
$$ LANGUAGE plpgsql STRICT IMMUTABLE;
CREATE AGGREGATE existing_agg(int) (
    SFUNC = existing_func,
    STYPE = int
);

\c - - :master_host :master_port
SET search_path TO proc_conflict;

CREATE FUNCTION existing_func(state int, i int) RETURNS int AS $$
BEGIN
    RETURN state * 5 + i;
END;
$$ LANGUAGE plpgsql STRICT IMMUTABLE;
CREATE AGGREGATE existing_agg(int) (
    SFUNC = existing_func,
    STYPE = int
);

SELECT create_distributed_function('existing_agg(int)');

\c - - :public_worker_1_host :worker_1_port
SET search_path TO proc_conflict;

WITH data (val) AS (
    select 2
    union all select 4
    union all select 6
)
SELECT existing_agg(val) FROM data;

\c - - :master_host :master_port
SET search_path TO proc_conflict;

WITH data (val) AS (
    select 2
    union all select 4
    union all select 6
)
SELECT existing_agg(val) FROM data;

-- now test worker_create_or_replace_object directly
CREATE FUNCTION existing_func2(state int, i int) RETURNS int AS $$
BEGIN
    RETURN state + i;
END;
$$ LANGUAGE plpgsql STRICT IMMUTABLE;

SELECT worker_create_or_replace_object('CREATE AGGREGATE proc_conflict.existing_agg(integer) (STYPE = integer,SFUNC = proc_conflict.existing_func2)');
SELECT worker_create_or_replace_object('CREATE AGGREGATE proc_conflict.existing_agg(integer) (STYPE = integer,SFUNC = proc_conflict.existing_func2)');

-- hide cascades
SET client_min_messages TO error;
DROP SCHEMA proc_conflict CASCADE;

