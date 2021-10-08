SET search_path to "te;'st", public;

INSERT INTO dist SELECT *,* FROM generate_series(1,100);
INSERT INTO dist2 SELECT *,* FROM generate_series(1,100);
INSERT INTO dist2 SELECT *,* FROM generate_series(1,100);
INSERT INTO ref SELECT *,* FROM generate_series(1,100);

SELECT COUNT(*) FROM dist join ref USING (a);
SELECT COUNT(*) FROM dist join ref USING (a) WHERE dist.a =5;

SELECT COUNT(*) FROM dist as d1 join dist as d2 USING (a);
SELECT COUNT(*) FROM dist as d1 join dist as d2 USING (a) WHERE d1.a =5;
SELECT COUNT(*) FROM dist as d1 join dist2 as d2 USING (a);
SELECT COUNT(*) FROM dist as d1 join dist2 as d2 USING (a) WHERE d1.a =5;

SET citus.enable_repartition_joins TO TRUE;
SELECT COUNT(*) FROM dist as d1 join dist as d2 on d1.a = d2.b;
SELECT COUNT(*) FROM dist as d1 join dist as d2 on d1.a = d2.b WHERE d1.a =5;
SELECT COUNT(*) FROM dist as d1 join dist2 as d2 on d1.a = d2.b;
SELECT COUNT(*) FROM dist as d1 join dist2 as d2 on d1.a = d2.b WHERE d1.a =5;
