\set kid random(1, 1000000)
UPDATE bench_kv SET val = val + 1 WHERE kid = :kid;
