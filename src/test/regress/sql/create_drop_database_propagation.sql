\c - - - :master_port
create user create_drop_db_test_user;

set citus.enable_create_database_propagation=on;

CREATE DATABASE mydatabase
    WITH TEMPLATE = 'template0'
            OWNER = create_drop_db_test_user
            CONNECTION LIMIT = 10
            ENCODING = 'UTF8'
            LC_COLLATE = 'C'
            LC_CTYPE = 'C'
            ALLOW_CONNECTIONS = true
            IS_TEMPLATE = false;

SELECT pd.oid,pd.datname, pd.encoding,
pd.datistemplate, pd.datallowconn, pd.datconnlimit,
pd.datcollate , pd. datctype  ,  pd.datacl, rolname AS database_owner,
pa.rolname AS database_owner, pt.spcname AS tablespace
FROM pg_database pd
JOIN pg_authid pa ON pd.datdba = pa.oid
join pg_tablespace pt on pd.dattablespace = pt.oid
WHERE datname = 'mydatabase';

\c - - - :worker_1_port

SELECT pd.datname, pd.encoding,
pd.datistemplate, pd.datallowconn, pd.datconnlimit,
pd.datcollate , pd. datctype  ,  pd.datacl, rolname AS database_owner,
pa.rolname AS database_owner, pt.spcname AS tablespace
FROM pg_database pd
JOIN pg_authid pa ON pd.datdba = pa.oid
join pg_tablespace pt on pd.dattablespace = pt.oid
WHERE datname = 'mydatabase';
\c - - - :worker_2_port

SELECT pd.datname, pd.encoding,
pd.datistemplate, pd.datallowconn, pd.datconnlimit,
pd.datcollate , pd. datctype  ,  pd.datacl, rolname AS database_owner,
pa.rolname AS database_owner, pt.spcname AS tablespace
FROM pg_database pd
JOIN pg_authid pa ON pd.datdba = pa.oid
join pg_tablespace pt on pd.dattablespace = pt.oid
WHERE datname = 'mydatabase';

\c - - - :master_port
set citus.enable_create_database_propagation=on;
drop database mydatabase;

SELECT pd.datname, pd.encoding,
pd.datistemplate, pd.datallowconn, pd.datconnlimit,
pd.datcollate , pd. datctype  ,  pd.datacl, rolname AS database_owner,
pa.rolname AS database_owner, pt.spcname AS tablespace
FROM pg_database pd
JOIN pg_authid pa ON pd.datdba = pa.oid
join pg_tablespace pt on pd.dattablespace = pt.oid
WHERE datname = 'mydatabase';

\c - - - :worker_1_port

SELECT pd.datname, pd.encoding,
pd.datistemplate, pd.datallowconn, pd.datconnlimit,
pd.datcollate , pd. datctype  ,  pd.datacl, rolname AS database_owner,
pa.rolname AS database_owner, pt.spcname AS tablespace
FROM pg_database pd
JOIN pg_authid pa ON pd.datdba = pa.oid
join pg_tablespace pt on pd.dattablespace = pt.oid
WHERE datname = 'mydatabase';

\c - - - :worker_2_port

SELECT pd.datname, pd.encoding,
pd.datistemplate, pd.datallowconn, pd.datconnlimit,
pd.datcollate , pd. datctype  ,  pd.datacl, rolname AS database_owner,
pa.rolname AS database_owner, pt.spcname AS tablespace
FROM pg_database pd
JOIN pg_authid pa ON pd.datdba = pa.oid
join pg_tablespace pt on pd.dattablespace = pt.oid
WHERE datname = 'mydatabase';


\c - - - :master_port

drop user create_drop_db_test_user;


