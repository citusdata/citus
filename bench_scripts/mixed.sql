\set kid random(1, 1000000)
\set op random(1, 10)
\if :op <= 6
SELECT val FROM bench_kv WHERE kid = :kid;
\elif :op <= 8
UPDATE bench_kv SET val = val + 1 WHERE kid = :kid;
\else
\set newkid random(1000001, 9999999)
INSERT INTO bench_kv (kid, val) VALUES (:newkid, 1)
  ON CONFLICT (kid) DO UPDATE SET val = bench_kv.val + 1;
\endif
