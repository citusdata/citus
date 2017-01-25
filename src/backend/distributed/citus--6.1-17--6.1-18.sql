/* citus--6.1-17--6.1-18.sql */

SET search_path = 'pg_catalog';

DROP FUNCTION IF EXISTS master_add_node(text, integer);

CREATE FUNCTION master_add_node(nodename text,                                                
                                nodeport integer,
                                activate_node boolean DEFAULT TRUE,                                             
                                OUT nodeid integer,                                           
                                OUT groupid integer,                                          
                                OUT nodename text,                                            
                                OUT nodeport integer,                                         
                                OUT noderack text,                                            
                                OUT hasmetadata boolean)                                      
    RETURNS record                                                                            
    LANGUAGE C STRICT                                                                         
    AS 'MODULE_PATHNAME', $$master_add_node$$;
COMMENT ON FUNCTION master_add_node(nodename text, nodeport integer, activate_node boolean)                                         
    IS 'add node to the cluster';
    
RESET search_path;
