-- Citus uses ssl by default now. It does so by turning on ssl and if needed will generate
-- self-signed certificates.

-- To test this we will verify that SSL is set to ON for all machines, and we will make
-- sure connections to workers use SSL by having it required in citus.conn_nodeinfo and
-- lastly we will inspect the ssl state for connections to the workers

-- ssl can only be enabled by default on installations that are OpenSSL-enabled.
SHOW ssl_ciphers \gset
SELECT :'ssl_ciphers' != 'none' AS hasssl;

SHOW ssl;
SELECT run_command_on_workers($$
    SHOW ssl;
$$);

SHOW citus.node_conninfo;
SELECT run_command_on_workers($$
    SHOW citus.node_conninfo;
$$);

SELECT run_command_on_workers($$
    SELECT ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid();
$$);

SHOW ssl_ciphers;
SELECT run_command_on_workers($$
    SHOW ssl_ciphers;
$$);
