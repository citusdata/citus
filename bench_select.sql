\set id random(1, 10000)
SELECT id, val, payload FROM bench_dist WHERE id = :id;
