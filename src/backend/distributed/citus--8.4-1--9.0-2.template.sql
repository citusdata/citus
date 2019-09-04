/* citus--8.4-1--9.0-2 */

SET search_path = 'pg_catalog';

ALTER TABLE pg_dist_node
    ADD shouldhavedata bool NOT NULL DEFAULT true;

@include udfs/master_add_node/9.0-2.sql
@include udfs/master_activate_node/9.0-2.sql
@include udfs/master_add_secondary_node/9.0-2.sql
@include udfs/master_add_inactive_node/9.0-2.sql

RESET search_path;
