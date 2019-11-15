--  citus--8.0-8--8.0-9
SET search_path = 'pg_catalog';

REVOKE ALL ON FUNCTION master_activate_node(text,int) FROM PUBLIC;
REVOKE ALL ON FUNCTION master_add_inactive_node(text,int,int,noderole,name) FROM PUBLIC;
REVOKE ALL ON FUNCTION master_add_node(text,int,int,noderole,name) FROM PUBLIC;
REVOKE ALL ON FUNCTION master_add_secondary_node(text,int,text,int,name) FROM PUBLIC;
REVOKE ALL ON FUNCTION master_disable_node(text,int) FROM PUBLIC;
REVOKE ALL ON FUNCTION master_remove_node(text,int) FROM PUBLIC;
REVOKE ALL ON FUNCTION master_update_node(int,text,int) FROM PUBLIC;

RESET search_path;
