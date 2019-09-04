SET search_path TO upgrade_before, public;

SELECT * FROM t;
SELECT * FROM t WHERE a = 1;

DROP SCHEMA upgrade_before CASCADE;