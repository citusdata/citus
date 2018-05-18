/* citus--7.4-2--7.4-3 */
SET search_path = 'pg_catalog';

-- note that we're not dropping the older version of the function
CREATE FUNCTION worker_hash_partition_table(bigint, integer, text, text, oid, anyarray)
    RETURNS void
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$worker_hash_partition_table$$;
COMMENT ON FUNCTION worker_hash_partition_table(bigint, integer, text, text, oid,
                                                anyarray)
    IS 'hash partition query results';

RESET search_path;

CREATE FUNCTION citus_connections_hash()
RETURNS TABLE (
    hostname TEXT,
    port INT,
    "user" TEXT,
    database TEXT,
    socket INT,
    sessionLifespan BOOLEAN,
    claimedExclusively BOOLEAN,
    connectionStart Timestamp WITH TIME ZONE,
    transactionState TEXT,
    transactionCritical BOOLEAN,
    transactionFailed BOOLEAN,
    preparedName TEXT,
    dontKill BOOLEAN
)
AS 'MODULE_PATHNAME', 'citus_connections_hash'
LANGUAGE C STRICT;

CREATE FUNCTION citus_zombie_connections()
RETURNS TABLE (
    hostname TEXT,
    port INT,
    "user" TEXT,
    database TEXT,
    socket INT,
    sessionLifespan BOOLEAN,
    claimedExclusively BOOLEAN,
    connectionStart Timestamp WITH TIME ZONE,
    transactionState TEXT,
    transactionCritical BOOLEAN,
    transactionFailed BOOLEAN,
    preparedName TEXT,
    dontKill BOOLEAN
)
AS 'MODULE_PATHNAME', 'citus_zombie_connections'
LANGUAGE C STRICT;

CREATE FUNCTION dont_kill_multiconnection(INTEGER) RETURNS VOID
AS 'MODULE_PATHNAME', 'dont_kill_multiconnection'
LANGUAGE C STRICT;

CREATE FUNCTION cleanup_zombie_connections() RETURNS VOID
AS 'MODULE_PATHNAME', 'cleanup_zombie_connections'
LANGUAGE C STRICT;


CREATE FUNCTION citus_connection_placement_hash()
RETURNS TABLE (
    placementId BIGINT,
    failed BOOLEAN,
    hasSecondary BOOLEAN,
    username TEXT,
    connection_socket INT,
    hadDML BOOLEAN,
    hadDDL BOOLEAN,
    colocationGroupId INT,
    representativeValue INT
)
AS 'MODULE_PATHNAME', 'citus_connection_placement_hash'
LANGUAGE C STRICT;
