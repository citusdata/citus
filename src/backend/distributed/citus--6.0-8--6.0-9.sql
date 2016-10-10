/* citus--6.0-8--6.0-9.sql */

CREATE TABLE citus.pg_dist_local_group(
    groupid int NOT NULL PRIMARY KEY)
;

/* insert the default value for being the coordinator node */
INSERT INTO citus.pg_dist_local_group VALUES (0);

ALTER TABLE citus.pg_dist_local_group SET SCHEMA pg_catalog;
GRANT SELECT ON pg_catalog.pg_dist_local_group TO public;

ALTER TABLE pg_catalog.pg_dist_node ADD COLUMN hasmetadata bool NOT NULL DEFAULT false;
