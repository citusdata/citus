-- columnar--10.0-2--10.0-1.sql

-- revoke read access for columnar metadata tables from unprivileged user
REVOKE USAGE ON SCHEMA columnar FROM PUBLIC;
REVOKE SELECT ON ALL tables IN SCHEMA columnar FROM PUBLIC;
