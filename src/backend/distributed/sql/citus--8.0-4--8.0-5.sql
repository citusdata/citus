/* citus--8.0-4--8.0-5.sql */
SET search_path = 'pg_catalog';


DROP FUNCTION IF EXISTS get_all_active_transactions();


CREATE OR REPLACE FUNCTION get_all_active_transactions(OUT datid oid, OUT process_id int, OUT initiator_node_identifier int4, OUT worker_query BOOL, 
                                                       OUT transaction_number int8, OUT transaction_stamp timestamptz) 
RETURNS SETOF RECORD 
LANGUAGE C STRICT AS 'MODULE_PATHNAME', 
$$get_all_active_transactions$$;

COMMENT ON FUNCTION get_all_active_transactions(OUT datid oid, OUT datname text, OUT process_id int, OUT initiator_node_identifier int4, OUT worker_query BOOL, 
                                                OUT transaction_number int8, OUT transaction_stamp timestamptz) 
IS 'returns distributed transaction ids of active distributed transactions';


CREATE OR REPLACE FUNCTION citus_dist_stat_activity(OUT query_hostname text, OUT query_hostport int, OUT master_query_host_name text, OUT master_query_host_port int, 
                                                    OUT transaction_number int8, OUT transaction_stamp timestamptz, OUT datid oid, OUT datname name, 
                                                    OUT pid int, OUT usesysid oid, OUT usename name, OUT application_name text, OUT client_addr INET, 
                                                    OUT client_hostname TEXT, OUT client_port int, OUT backend_start timestamptz, OUT xact_start timestamptz, 
                                                    OUT query_start timestamptz, OUT state_change timestamptz, OUT wait_event_type text, OUT wait_event text, 
                                                    OUT state text, OUT backend_xid xid, OUT backend_xmin xid, OUT query text, OUT backend_type text) 
RETURNS SETOF RECORD 
LANGUAGE C STRICT AS 'MODULE_PATHNAME', 
$$citus_dist_stat_activity$$;

COMMENT ON FUNCTION citus_dist_stat_activity(OUT query_hostname text, OUT query_hostport int, OUT master_query_host_name text, OUT master_query_host_port int, 
                                             OUT transaction_number int8, OUT transaction_stamp timestamptz, OUT datid oid, OUT datname name, 
                                             OUT pid int, OUT usesysid oid, OUT usename name, OUT application_name text, OUT client_addr INET, 
                                             OUT client_hostname TEXT, OUT client_port int, OUT backend_start timestamptz, OUT xact_start timestamptz, 
                                             OUT query_start timestamptz, OUT state_change timestamptz, OUT wait_event_type text, OUT wait_event text, 
                                             OUT state text, OUT backend_xid xid, OUT backend_xmin xid, OUT query text, OUT backend_type text) 
IS 'returns distributed transaction activity on distributed tables';

CREATE VIEW citus.citus_dist_stat_activity AS
SELECT * FROM pg_catalog.citus_dist_stat_activity();
ALTER VIEW citus.citus_dist_stat_activity SET SCHEMA pg_catalog;
GRANT SELECT ON pg_catalog.citus_dist_stat_activity TO PUBLIC;



CREATE OR REPLACE FUNCTION citus_worker_stat_activity(OUT query_hostname text, OUT query_hostport int, OUT master_query_host_name text, OUT master_query_host_port int, 
                                                      OUT transaction_number int8, OUT transaction_stamp timestamptz, OUT datid oid, OUT datname name, 
                                                      OUT pid int, OUT usesysid oid, OUT usename name, OUT application_name text, OUT client_addr INET, 
                                                      OUT client_hostname TEXT, OUT client_port int, OUT backend_start timestamptz, OUT xact_start timestamptz, 
                                                      OUT query_start timestamptz, OUT state_change timestamptz, OUT wait_event_type text, OUT wait_event text, 
                                                      OUT state text, OUT backend_xid xid, OUT backend_xmin xid, OUT query text, OUT backend_type text) 
RETURNS SETOF RECORD 
LANGUAGE C STRICT AS 'MODULE_PATHNAME', 
$$citus_worker_stat_activity$$;

COMMENT ON FUNCTION citus_worker_stat_activity(OUT query_hostname text, OUT query_hostport int, OUT master_query_host_name text, OUT master_query_host_port int, 
                                               OUT transaction_number int8, OUT transaction_stamp timestamptz, OUT datid oid, OUT datname name, 
                                               OUT pid int, OUT usesysid oid, OUT usename name, OUT application_name text, OUT client_addr INET, 
                                               OUT client_hostname TEXT, OUT client_port int, OUT backend_start timestamptz, OUT xact_start timestamptz, 
                                               OUT query_start timestamptz, OUT state_change timestamptz, OUT wait_event_type text, OUT wait_event text, 
                                               OUT state text, OUT backend_xid xid, OUT backend_xmin xid, OUT query text, OUT backend_type text) 
IS 'returns distributed transaction activity on shards of distributed tables';

CREATE VIEW citus.citus_worker_stat_activity AS
SELECT * FROM pg_catalog.citus_worker_stat_activity();
ALTER VIEW citus.citus_worker_stat_activity SET SCHEMA pg_catalog;
GRANT SELECT ON pg_catalog.citus_worker_stat_activity TO PUBLIC;

RESET search_path;

