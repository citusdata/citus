--
-- PG15
--
SHOW server_version \gset
SELECT substring(:'server_version', '\d+')::int >= 15 AS server_version_ge_15
\gset
\if :server_version_ge_15
\else
\q
\endif

-- create/drop database for pg >= 15

set citus.enable_create_database_propagation=on;

CREATE DATABASE mydatabase
    WITH OID = 966345;

CREATE DATABASE mydatabase
    WITH strategy file_copy;

CREATE DATABASE st_wal_log
    WITH strategy WaL_LoG;

SELECT * FROM public.check_database_on_all_nodes('st_wal_log') ORDER BY node_type;

drop database st_wal_log;

select 1 from citus_remove_node('localhost', :worker_2_port);

-- test COLLATION_VERSION

CREATE DATABASE test_collation_version
    WITH ENCODING = 'UTF8'
            COLLATION_VERSION = '1.0'
            ALLOW_CONNECTIONS = false;

select 1 from citus_add_node('localhost', :worker_2_port);

SELECT * FROM public.check_database_on_all_nodes('test_collation_version') ORDER BY node_type;

drop database test_collation_version;

SET client_min_messages TO WARNING;
-- test LOCALE_PROVIDER & ICU_LOCALE
CREATE DATABASE test_locale_provider
    WITH ENCODING = 'UTF8'
         LOCALE_PROVIDER = 'icu'
         ICU_LOCALE = 'en_US';
RESET client_min_messages;

CREATE DATABASE test_locale_provider
    WITH ENCODING = 'UTF8'
         LOCALE_PROVIDER = 'libc'
         ICU_LOCALE = 'en_US';

CREATE DATABASE test_locale_provider
    WITH ENCODING = 'UTF8'
         LOCALE_PROVIDER = 'libc';

SELECT * FROM public.check_database_on_all_nodes('test_locale_provider') ORDER BY node_type;

drop database test_locale_provider;

\c - - - :master_port
