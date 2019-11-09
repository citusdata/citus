CREATE SCHEMA adaptive_executor;
SET search_path TO adaptive_executor;

CREATE TABLE ab(a int, b int);
SELECT create_distributed_table('ab', 'a');
INSERT INTO ab SELECT *,* FROM generate_series(1,10);

SELECT COUNT(*) FROM ab k, ab l 
WHERE k.a = l.b; 

SELECT COUNT(*) FROM ab k, ab l, ab m, ab t 
WHERE k.a = l.b AND k.a = m.b AND t.b = l.a; 

DROP SCHEMA adaptive_executor CASCADE;
