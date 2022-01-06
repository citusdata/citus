DROP FUNCTION citus_worker_stat_activity CASCADE;

CREATE OR REPLACE FUNCTION citus_worker_stat_activity(OUT query_hostname text, OUT query_hostport int, OUT distributed_query_host_name text, OUT distributed_query_host_port int,
                                                      OUT transaction_number int8, OUT transaction_stamp timestamptz, OUT datid oid, OUT datname name,
                                                      OUT pid int, OUT usesysid oid, OUT usename name, OUT application_name text, OUT client_addr INET,
                                                      OUT client_hostname TEXT, OUT client_port int, OUT backend_start timestamptz, OUT xact_start timestamptz,
                                                      OUT query_start timestamptz, OUT state_change timestamptz, OUT wait_event_type text, OUT wait_event text,
                                                      OUT state text, OUT backend_xid xid, OUT backend_xmin xid, OUT query text, OUT backend_type text, OUT global_pid int8)
RETURNS SETOF RECORD
LANGUAGE C STRICT AS 'MODULE_PATHNAME',
$$citus_worker_stat_activity$$;

COMMENT ON FUNCTION citus_worker_stat_activity(OUT query_hostname text, OUT query_hostport int, OUT distributed_query_host_name text, OUT distributed_query_host_port int,
                                               OUT transaction_number int8, OUT transaction_stamp timestamptz, OUT datid oid, OUT datname name,
                                               OUT pid int, OUT usesysid oid, OUT usename name, OUT application_name text, OUT client_addr INET,
                                               OUT client_hostname TEXT, OUT client_port int, OUT backend_start timestamptz, OUT xact_start timestamptz,
                                               OUT query_start timestamptz, OUT state_change timestamptz, OUT wait_event_type text, OUT wait_event text,
                                               OUT state text, OUT backend_xid xid, OUT backend_xmin xid, OUT query text, OUT backend_type text, OUT global_pid int8)
IS 'returns distributed transaction activity on shards of distributed tables';

CREATE VIEW citus.citus_worker_stat_activity AS
SELECT * FROM pg_catalog.citus_worker_stat_activity();
ALTER VIEW citus.citus_worker_stat_activity SET SCHEMA pg_catalog;
GRANT SELECT ON pg_catalog.citus_worker_stat_activity TO PUBLIC;
