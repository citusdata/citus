-- citus_locks combines the pg_locks views from all nodes and adds global_pid, nodeid, and
-- relation_name. The columns of citus_locks don't change based on the Postgres version,
-- however the pg_locks's columns do. Postgres 14 added one more column to pg_locks
-- (waitstart timestamptz). citus_locks has the most expansive column set, including the
-- newly added column. If citus_locks is queried in a Postgres version where pg_locks
-- doesn't have some columns, the values for those columns in citus_locks will be NULL
CREATE OR REPLACE FUNCTION pg_catalog.citus_locks (
    OUT global_pid bigint,
    OUT nodeid int,
    OUT locktype text,
    OUT database oid,
    OUT relation oid,
    OUT relation_name text,
    OUT page integer,
    OUT tuple smallint,
    OUT virtualxid text,
    OUT transactionid xid,
    OUT classid oid,
    OUT objid oid,
    OUT objsubid smallint,
    OUT virtualtransaction text,
    OUT pid integer,
    OUT mode text,
    OUT granted boolean,
    OUT fastpath boolean,
    OUT waitstart timestamp with time zone
)
    RETURNS SETOF record
    LANGUAGE plpgsql
    AS $function$
BEGIN
    RETURN QUERY
    SELECT *
    FROM jsonb_to_recordset((
        SELECT
            jsonb_agg(all_citus_locks_rows_as_jsonb.citus_locks_row_as_jsonb)::jsonb
        FROM (
            SELECT
                jsonb_array_elements(run_command_on_all_nodes.result::jsonb)::jsonb ||
                    ('{"nodeid":' || run_command_on_all_nodes.nodeid || '}')::jsonb AS citus_locks_row_as_jsonb
            FROM
                run_command_on_all_nodes (
                    $$
                        SELECT
                            coalesce(to_jsonb (array_agg(citus_locks_from_one_node.*)), '[{}]'::jsonb)
                        FROM (
                            SELECT
                                global_pid, pg_locks.relation::regclass::text AS relation_name, pg_locks.*
                            FROM pg_locks
                        LEFT JOIN get_all_active_transactions () ON process_id = pid) AS citus_locks_from_one_node;
                    $$,
                    parallel:= TRUE,
                    give_warning_for_connection_errors:= TRUE)
            WHERE
                success = 't')
        AS all_citus_locks_rows_as_jsonb))
AS (
    global_pid bigint,
    nodeid int,
    locktype text,
    database oid,
    relation oid,
    relation_name text,
    page integer,
    tuple smallint,
    virtualxid text,
    transactionid xid,
    classid oid,
    objid oid,
    objsubid smallint,
    virtualtransaction text,
    pid integer,
    mode text,
    granted boolean,
    fastpath boolean,
    waitstart timestamp with time zone
);
END;
$function$;

CREATE OR REPLACE VIEW citus.citus_locks AS
SELECT * FROM pg_catalog.citus_locks();

ALTER VIEW citus.citus_locks SET SCHEMA pg_catalog;

GRANT SELECT ON pg_catalog.citus_locks TO PUBLIC;
