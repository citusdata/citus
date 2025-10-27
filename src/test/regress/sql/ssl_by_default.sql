-- Citus uses ssl by default now. It does so by turning on ssl and if needed will generate
-- self-signed certificates.
--
-- This test verifies:
-- 1) ssl=on on coordinator and workers
-- 2) coordinator->workers connections use SSL (pg_stat_ssl true)
-- 3) ssl_ciphers is non-empty and has a colon-separated rule/list on both coordinator and workers
--    (PG18/OpenSSL may report a rule string like HIGH:MEDIUM:+3DES:!aNULL instead of an expanded list)

-- 0) Is this an OpenSSL-enabled build? (if not, ssl_ciphers is 'none')
--    Keep the “hasssl” signal but don’t rely on the literal cipher list value.
SHOW ssl_ciphers \gset
SELECT :'ssl_ciphers' <> 'none' AS hasssl;

-- 1) ssl must be on (coordinator + workers)
SHOW ssl;
SELECT run_command_on_workers($$
    SHOW ssl;
$$);

-- 2) connections to workers carry sslmode=require
SHOW citus.node_conninfo;
SELECT run_command_on_workers($$
    SHOW citus.node_conninfo;
$$);

-- 3) pg_stat_ssl says SSL is active on each worker connection
SELECT run_command_on_workers($$
    SELECT ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid();
$$);

-- 4) ssl_ciphers checks (coordinator): non-empty and contains at least one ':'
SELECT current_setting('ssl_ciphers') <> '' AS has_ssl_ciphers;
SELECT position(':' in current_setting('ssl_ciphers')) > 0 AS has_colon;

-- 5) ssl_ciphers checks (workers)
SELECT run_command_on_workers($$
  SELECT current_setting('ssl_ciphers') <> '' AS has_ssl_ciphers
$$);

SELECT run_command_on_workers($$
  SELECT position(':' in current_setting('ssl_ciphers')) > 0 AS has_colon
$$);
