/* citus--7.1-2--7.1-3 */

CREATE TABLE citus.pg_dist_metadata(
    tag text NOT NULL,
    value text NOT NULL
);
ALTER TABLE citus.pg_dist_metadata SET SCHEMA pg_catalog;
GRANT SELECT ON pg_catalog.pg_dist_metadata TO public;

INSERT INTO pg_dist_metadata VALUES ('server_id', (random() * 1e18)::bigint::text);
