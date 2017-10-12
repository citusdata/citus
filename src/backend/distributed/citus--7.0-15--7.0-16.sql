/* citus--7.0-15--7.0-16 */

CREATE TABLE citus.pg_dist_metadata(
    tag text,
    value text
);
ALTER TABLE citus.pg_dist_metadata SET SCHEMA pg_catalog;

INSERT INTO pg_dist_metadata VALUES ('server_id', (random() * 1e18)::bigint::text);
