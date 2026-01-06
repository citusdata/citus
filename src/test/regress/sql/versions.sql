\set ECHO none
SELECT 
    node_type,
    node_port,
    so_version,
    sql_version
FROM (
    -- Coordinator citus extension
    SELECT 
        'coordinator' AS node_type,
        NULL::integer AS node_port,
        split_part(citus_version(), ' ', 2) AS so_version,
        extversion AS sql_version
    FROM pg_extension 
    WHERE extname = 'citus'
    
    UNION ALL
    
    -- Worker citus extension
    SELECT 
        'worker' AS node_type,
        w_so.nodeport AS node_port,
        w_so.result AS so_version,
        w_sql.result AS sql_version
    FROM 
        run_command_on_workers($$SELECT split_part(citus_version(), ' ', 2)$$) w_so
        JOIN run_command_on_workers($$SELECT extversion FROM pg_extension WHERE extname = 'citus'$$) w_sql
            ON w_so.nodeport = w_sql.nodeport
    
) versions
ORDER BY node_type DESC, node_port;
