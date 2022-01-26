CREATE SCHEMA mx_app_name;

CREATE TABLE output (line text);

-- a hack to run command with another user
\COPY output FROM PROGRAM 'PGAPPNAME=mx_app_name psql -c "CREATE TABLE dist_1(a int)" -h localhost -p 57636 -U postgres -d regression'
\COPY output FROM PROGRAM 'PGAPPNAME=mx_app_name psql -c "SELECT create_distributed_table(''dist_1'', ''a'');" -h localhost -p 57636 -U postgres -d regression'

-- ensure the command executed fine
SELECT count(*) FROM pg_dist_partition WHERE logicalrelid = 'dist_1'::regclass;

\COPY output FROM PROGRAM 'PGAPPNAME=mx_app_name psql -c "DROP TABLE dist_1;" -h localhost -p 57636 -U postgres -d regression'

DROP SCHEMA mx_app_name CASCADE;
