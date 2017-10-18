/* citus--7.1-2--7.1-3 */

CREATE TABLE citus.pg_dist_node_metadata(
    metadata jsonb
);
ALTER TABLE citus.pg_dist_node_metadata SET SCHEMA pg_catalog;
GRANT SELECT ON pg_catalog.pg_dist_node_metadata TO public;

INSERT INTO pg_dist_node_metadata
    VALUES (jsonb_build_object('server_id',
            ((random() * 2^16)::bigint |
             ((random() * 2^16)::bigint << 16) |
             ((random() * 2^16)::bigint << 32) |
             ((random() * 2^16)::bigint << 48))::text));
