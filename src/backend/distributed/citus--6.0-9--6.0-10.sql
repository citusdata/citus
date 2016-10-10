/* citus--6.0-9--6.0-10.sql */

CREATE TABLE citus.pg_dist_transaction (
    groupid int NOT NULL,
    gid text NOT NULL
);

CREATE INDEX pg_dist_transaction_group_index
ON citus.pg_dist_transaction using btree(groupid);

ALTER TABLE citus.pg_dist_transaction SET SCHEMA pg_catalog;
ALTER TABLE pg_catalog.pg_dist_transaction
ADD CONSTRAINT pg_dist_transaction_unique_constraint UNIQUE (groupid, gid);

GRANT SELECT ON pg_catalog.pg_dist_transaction TO public;

CREATE FUNCTION recover_prepared_transactions()
    RETURNS int
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$recover_prepared_transactions$$;

COMMENT ON FUNCTION recover_prepared_transactions()
    IS 'recover prepared transactions started by this node';

