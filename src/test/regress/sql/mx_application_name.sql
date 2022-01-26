CREATE SCHEMA mx_app_name;

CREATE TABLE output (line text);

-- a hack to run command with another user
\COPY output FROM PROGRAM 'export PGAPPNAME=mx_app_name psql -c "CREATE TABLE dist_1(a int)" -h localhost -p 57636 -U postgres -d regression'
\COPY output FROM PROGRAM 'export PGAPPNAME=mx_app_name psql -c "show application_name;" -h localhost -p 57636 -U postgres -d regression'
SELECT * FROM output;
-- ensure the command executed fine
SELECT count(*) FROM pg_dist_partition WHERE logicalrelid = 'dist_1'::regclass;

\COPY output FROM PROGRAM 'export PGAPPNAME=mx_app_name psql -c "DROP TABLE dist_1;" -h localhost -p 57636 -U postgres -d regression'

DROP SCHEMA mx_app_name CASCADE;
