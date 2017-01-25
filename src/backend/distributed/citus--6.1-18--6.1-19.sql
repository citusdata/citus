/* citus--6.1-18--6.1-19.sql */

SET search_path = 'pg_catalog';

CREATE FUNCTION master_activate_node(nodename text,                                                
                                     nodeport integer)                                      
    RETURNS void                                                                            
    LANGUAGE C STRICT                                                                         
    AS 'MODULE_PATHNAME', $$master_activate_node$$;
COMMENT ON FUNCTION master_activate_node(nodename text, nodeport integer)                                         
    IS 'add node to the cluster';
    
RESET search_path;
