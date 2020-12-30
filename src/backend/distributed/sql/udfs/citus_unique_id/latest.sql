CREATE SEQUENCE citus.citus_unique_id
    MINVALUE 1
    NO CYCLE;
ALTER SEQUENCE citus.citus_unique_id SET SCHEMA pg_catalog;