\set id random(1, 10000)
SELECT id, val, payload FROM bench_dist WHERE id = :id;
\set uid random(1, 10000)
\set newval random(1, 1000000)
UPDATE bench_dist SET val = :newval WHERE id = :uid;
