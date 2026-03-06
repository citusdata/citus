\set kid random(1000001, 9999999)
INSERT INTO bench_kv (kid, val) VALUES (:kid, 1)
  ON CONFLICT (kid) DO UPDATE SET val = bench_kv.val + 1;
