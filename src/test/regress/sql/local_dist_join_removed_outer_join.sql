CREATE SCHEMA local_dist_join_removed_outer_join;
SET search_path TO local_dist_join_removed_outer_join;

CREATE TABLE distributed (key int, id bigserial PRIMARY KEY,
                         name text, created_at timestamptz DEFAULT now());
CREATE TABLE local (key int, id bigserial PRIMARY KEY, key2 int,
                    title text, key3 int);

ALTER TABLE local DROP COLUMN key;
ALTER TABLE local DROP COLUMN key2;
ALTER TABLE local DROP COLUMN key3;
ALTER TABLE distributed DROP COLUMN key;

SELECT create_distributed_table('distributed', 'id');

INSERT INTO distributed SELECT i, i::text, now() FROM generate_series(0,100) i;
INSERT INTO local SELECT i, i::text FROM generate_series(0,100) i;

-- Preserve dropped-column handling when PostgreSQL removes the distributed side.
SELECT count(*) FROM local LEFT JOIN distributed USING (id);

-- PostgreSQL can pull a view or subquery up before removing the join.
CREATE VIEW distributed_view AS SELECT * FROM distributed;
SELECT count(*) FROM local LEFT JOIN distributed_view USING (id);
SELECT count(*) FROM local LEFT JOIN (SELECT id, name FROM distributed) d USING (id);
SELECT count(*) FROM (SELECT * FROM (SELECT * FROM distributed) d1) d2 RIGHT JOIN local USING (id);
-- A relation whose columns are used above the join is kept.
SELECT count(d.name) FROM local LEFT JOIN distributed_view d USING (id);

-- Replanning after recursive planning must not mistake self join elimination
-- for outer join removal.
SELECT count(q.id) FROM distributed d0
LEFT JOIN (distributed d1 JOIN distributed d2 USING (id)) q ON d0.id = q.id
WHERE d0.name IN (SELECT name FROM distributed ORDER BY name LIMIT 30);

DROP VIEW distributed_view;
DROP TABLE local, distributed;
RESET search_path;
DROP SCHEMA local_dist_join_removed_outer_join;
