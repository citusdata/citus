set citus.enable_create_database_propagation=on;
create user create_drop_db_test_user;


CREATE DATABASE mydatabase
    WITH TEMPLATE = 'template0'
            OWNER = create_drop_db_test_user
            CONNECTION LIMIT = 10
            ENCODING = 'UTF8'
            STRATEGY = 'wal_log'
            LOCALE = 'C'
            LC_COLLATE = 'C'
            LC_CTYPE = 'C'
            ICU_LOCALE = 'C'
            LOCALE_PROVIDER = 'icu'
            COLLATION_VERSION = '1.0'
            TABLESPACE = test_tablespace
            ALLOW_CONNECTIONS = true
            IS_TEMPLATE = false
            OID = 966345;

SELECT pd.datname  , pd.datdba, pd.encoding, pd.datlocprovider,
pd.datistemplate, pd.datallowconn, pd.datconnlimit,
pd.datcollate , pd. datctype  , pd.daticulocale, pd.datcollversion,
pd.datacl, rolname AS database_owner, pa.rolname AS database_owner, pt.spcname AS tablespace
FROM pg_database pd
JOIN pg_authid pa ON pd.datdba = pa.oid
join pg_tablespace pt on pd.dattablespace = pt.oid
WHERE datname = 'mydatabase';

drop database mydatabase;

SELECT pd.datname  , pd.datdba, pd.encoding, pd.datlocprovider,
pd.datistemplate, pd.datallowconn, pd.datconnlimit,
pd.datcollate , pd. datctype  , pd.daticulocale, pd.datcollversion,
pd.datacl, rolname AS database_owner, pa.rolname AS database_owner, pt.spcname AS tablespace
FROM pg_database pd
JOIN pg_authid pa ON pd.datdba = pa.oid
join pg_tablespace pt on pd.dattablespace = pt.oid
WHERE datname = 'mydatabase';

drop user create_drop_db_test_user;
set citus.enable_create_database_propagation=off;


