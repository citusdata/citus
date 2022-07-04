SELECT column_name FROM information_schema.columns WHERE table_name = 'citus_locks' AND column_name NOT IN ('waitstart')
EXCEPT SELECT column_name FROM information_schema.columns WHERE table_name = 'pg_locks'
ORDER BY 1;

SELECT column_name FROM information_schema.columns WHERE table_name = 'pg_locks'
EXCEPT SELECT column_name FROM information_schema.columns WHERE table_name = 'citus_locks'
ORDER BY 1;
