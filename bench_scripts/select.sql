\set kid random(1, 1000000)
SELECT val FROM bench_kv WHERE kid = :kid;
