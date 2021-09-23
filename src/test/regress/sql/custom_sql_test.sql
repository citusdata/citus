INSERT INTO dist SELECT *,* FROM generate_series(1,100);
INSERT INTO ref SELECT *,* FROM generate_series(1,100);

SELECT COUNT(*) FROM dist join ref USING (a);
SELECT COUNT(*) FROM dist join ref USING (a) WHERE dist.a =5;
