-- columnar--10.0-1--10.0-2.sql

-- grant read access for columnar metadata tables to unprivileged user
GRANT USAGE ON SCHEMA columnar TO PUBLIC;
GRANT SELECT ON ALL tables IN SCHEMA columnar TO PUBLIC ;
