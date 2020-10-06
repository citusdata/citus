-- citus--9.5-1--9.4-1

SET search_path = 'pg_catalog';

#include "../udfs/citus_drop_trigger/9.0-1.sql"

-- Check if user has any citus local tables.
-- If not, DROP create_citus_local_table UDF and continue safely.
-- Otherwise, raise an exception to stop the downgrade process.
DO $$
DECLARE
    citus_local_table_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO citus_local_table_count
    FROM pg_dist_partition WHERE repmodel != 't' AND partmethod = 'n';

    IF citus_local_table_count = 0 THEN
        -- no citus local tables exist, can safely downgrade
        DROP FUNCTION create_citus_local_table(table_name regclass);
    ELSE
        RAISE EXCEPTION 'citus local tables are introduced in Citus 9.5'
        USING HINT = 'To downgrade Citus to an older version, you should '
                     'first convert each citus local table to a postgres '
                     'table by executing SELECT undistribute_table("%s")';
    END IF;
END;
$$ LANGUAGE plpgsql;

DROP FUNCTION worker_record_sequence_dependency(regclass, regclass, name);

--  task_tracker_* functions

CREATE FUNCTION task_tracker_assign_task(bigint, integer, text)
    RETURNS void
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$task_tracker_assign_task$$;
COMMENT ON FUNCTION task_tracker_assign_task(bigint, integer, text)
    IS 'assign a task to execute';

CREATE FUNCTION task_tracker_task_status(bigint, integer)
    RETURNS integer
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$task_tracker_task_status$$;
COMMENT ON FUNCTION task_tracker_task_status(bigint, integer)
    IS 'check an assigned task''s execution status';

CREATE FUNCTION task_tracker_cleanup_job(bigint)
    RETURNS void
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$task_tracker_cleanup_job$$;
COMMENT ON FUNCTION task_tracker_cleanup_job(bigint)
    IS 'clean up all tasks associated with a job';

CREATE FUNCTION worker_merge_files_and_run_query(bigint, integer, text, text)
    RETURNS void
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$worker_merge_files_and_run_query$$;
COMMENT ON FUNCTION worker_merge_files_and_run_query(bigint, integer, text, text)
    IS 'merge files and run a reduce query on merged files';

CREATE FUNCTION worker_execute_sql_task(jobid bigint, taskid integer, query text, binary bool)
    RETURNS bigint
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$worker_execute_sql_task$$;
COMMENT ON FUNCTION worker_execute_sql_task(bigint, integer, text, bool)
    IS 'execute a query and write the results to a task file';

CREATE FUNCTION task_tracker_conninfo_cache_invalidate()
    RETURNS trigger
    LANGUAGE C
    AS 'citus', $$task_tracker_conninfo_cache_invalidate$$;
COMMENT ON FUNCTION task_tracker_conninfo_cache_invalidate()
    IS 'invalidate task-tracker conninfo cache';

CREATE TRIGGER dist_poolinfo_task_tracker_cache_invalidate
    AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE
    ON pg_catalog.pg_dist_poolinfo
    FOR EACH STATEMENT EXECUTE PROCEDURE task_tracker_conninfo_cache_invalidate();

CREATE TRIGGER dist_authinfo_task_tracker_cache_invalidate
    AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE
    ON pg_catalog.pg_dist_authinfo
    FOR EACH STATEMENT EXECUTE PROCEDURE task_tracker_conninfo_cache_invalidate();

CREATE FUNCTION master_drop_sequences(sequence_names text[])
    RETURNS void
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$master_drop_sequences$$;
COMMENT ON FUNCTION master_drop_sequences(text[])
    IS 'drop specified sequences from the cluster';

RESET search_path;

DROP FUNCTION pg_catalog.undistribute_table(table_name regclass);
