

\set create_drop_db_tablespace :abs_srcdir '/tmp_check/ts3'
CREATE TABLESPACE create_drop_db_tablespace LOCATION :'create_drop_db_tablespace';

\c - - - :worker_1_port
\set create_drop_db_tablespace :abs_srcdir '/tmp_check/ts4'
CREATE TABLESPACE create_drop_db_tablespace LOCATION :'create_drop_db_tablespace';

\c - - - :worker_2_port
\set create_drop_db_tablespace :abs_srcdir '/tmp_check/ts5'
CREATE TABLESPACE create_drop_db_tablespace LOCATION :'create_drop_db_tablespace';

\c - - - :master_port
create user create_drop_db_test_user;

set citus.enable_create_database_propagation=on;


CREATE DATABASE mydatabase
    WITH OWNER = create_drop_db_test_user
    TEMPLATE = 'template0'
            ENCODING = 'UTF8'
            CONNECTION LIMIT = 10
            LC_COLLATE = 'C'
            LC_CTYPE = 'C'
            TABLESPACE = create_drop_db_tablespace
            ALLOW_CONNECTIONS = true
            IS_TEMPLATE = false;



SELECT result from run_command_on_all_nodes(
  $$
  SELECT jsonb_agg(to_jsonb(q2.*)) FROM (
    SELECT pd.datname, pg_encoding_to_char(pd.encoding) as encoding,
    pd.datistemplate, pd.datallowconn, pd.datconnlimit,
    pd.datcollate , pd. datctype  ,  pd.datacl,
    pa.rolname AS database_owner, pt.spcname AS tablespace
    FROM pg_database pd
    JOIN pg_authid pa ON pd.datdba = pa.oid
    join pg_tablespace pt on pd.dattablespace = pt.oid
    WHERE datname = 'mydatabase'
  ) q2
  $$
) ORDER BY result;


drop database mydatabase;



SELECT result from run_command_on_all_nodes(
  $$
  SELECT jsonb_agg(to_jsonb(q2.*)) FROM (
    SELECT pd.datname, pg_encoding_to_char(pd.encoding) as encoding,
    pd.datistemplate, pd.datallowconn, pd.datconnlimit,
    pd.datcollate , pd. datctype  ,  pd.datacl,
    pa.rolname AS database_owner, pt.spcname AS tablespace
    FROM pg_database pd
    JOIN pg_authid pa ON pd.datdba = pa.oid
    join pg_tablespace pt on pd.dattablespace = pt.oid
    WHERE datname = 'mydatabase'
  ) q2
  $$
) ORDER BY result;

-- test database syncing after node addition

select citus_remove_node('localhost', :worker_2_port);

--test with is_template true and allow connections false
CREATE DATABASE mydatabase
    WITH TEMPLATE = 'template0'
            OWNER = create_drop_db_test_user
            CONNECTION LIMIT = 10
            ENCODING = 'UTF8'
            LC_COLLATE = 'C'
            LC_CTYPE = 'C'
            TABLESPACE = create_drop_db_tablespace
            ALLOW_CONNECTIONS = false
            IS_TEMPLATE = false;


SELECT result from run_command_on_all_nodes(
  $$
  SELECT jsonb_agg(to_jsonb(q2.*)) FROM (
    SELECT pd.datname, pg_encoding_to_char(pd.encoding) as encoding,
    pd.datistemplate, pd.datallowconn, pd.datconnlimit,
    pd.datcollate , pd. datctype  ,  pd.datacl,
    pa.rolname AS database_owner, pt.spcname AS tablespace
    FROM pg_database pd
    JOIN pg_authid pa ON pd.datdba = pa.oid
    join pg_tablespace pt on pd.dattablespace = pt.oid
    WHERE datname = 'mydatabase'
  ) q2
  $$
) ORDER BY result;

select citus_add_node('localhost', :worker_2_port);

SELECT result from run_command_on_all_nodes(
  $$
  SELECT jsonb_agg(to_jsonb(q2.*)) FROM (
    SELECT pd.datname, pg_encoding_to_char(pd.encoding) as encoding,
    pd.datistemplate, pd.datallowconn, pd.datconnlimit,
    pd.datcollate , pd. datctype  ,  pd.datacl,
    pa.rolname AS database_owner, pt.spcname AS tablespace
    FROM pg_database pd
    JOIN pg_authid pa ON pd.datdba = pa.oid
    join pg_tablespace pt on pd.dattablespace = pt.oid
    WHERE datname = 'mydatabase'
  ) q2
  $$
) ORDER BY result;

SET citus.log_remote_commands = true;
set citus.grep_remote_commands = '%DROP DATABASE%';
drop database mydatabase;

SET citus.log_remote_commands = false;

SELECT result from run_command_on_all_nodes(
  $$
  SELECT jsonb_agg(to_jsonb(q2.*)) FROM (
    SELECT pd.datname, pg_encoding_to_char(pd.encoding) as encoding,
    pd.datistemplate, pd.datallowconn, pd.datconnlimit,
    pd.datcollate , pd. datctype  ,  pd.datacl,
    pa.rolname AS database_owner, pt.spcname AS tablespace
    FROM pg_database pd
    JOIN pg_authid pa ON pd.datdba = pa.oid
    join pg_tablespace pt on pd.dattablespace = pt.oid
    WHERE datname = 'mydatabase'
  ) q2
  $$
) ORDER BY result;

--tests for special characters in database name
set citus.enable_create_database_propagation=on;
SET citus.log_remote_commands = true;
set citus.grep_remote_commands = '%CREATE DATABASE%';

create database "mydatabase#1'2";

set citus.grep_remote_commands = '%DROP DATABASE%';
drop database if exists "mydatabase#1'2";

--clean up resources created by this test

drop tablespace create_drop_db_tablespace;

\c - - - :worker_1_port

drop tablespace create_drop_db_tablespace;

\c - - - :worker_2_port

drop tablespace create_drop_db_tablespace;

\c - - - :master_port

drop user create_drop_db_test_user;
