-- columnar--10.2-1--10.2-2.sql

-- revoke read access for columnar.chunk from unprivileged
-- user as it contains chunk min/max values
REVOKE SELECT ON columnar.chunk FROM PUBLIC;
