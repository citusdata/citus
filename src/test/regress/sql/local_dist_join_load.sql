CREATE SCHEMA local_dist_join_mixed;
SET search_path TO local_dist_join_mixed;



CREATE TABLE distributed (key int, id bigserial PRIMARY KEY,
                    	  name text,
                    	  created_at timestamptz DEFAULT now(), b int);
CREATE TABLE reference (a int, id bigserial PRIMARY KEY,
                    	title text, b int);

CREATE TABLE local (key int, id bigserial PRIMARY KEY, key2 int,
                    title text, key3 int);

-- drop columns so that we test the correctness in different scenarios.
ALTER TABLE local DROP column key;
ALTER TABLE local DROP column key2;
ALTER TABLE local DROP column key3;

ALTER TABLE distributed DROP column key;
ALTER TABLE reference DROP column b;

-- these above restrictions brought us to the following schema
SELECT create_reference_table('reference');
SELECT create_distributed_table('distributed', 'id');
SELECT citus_add_local_table_to_metadata('local');

ALTER TABLE distributed DROP column b;
ALTER TABLE reference DROP column a;

INSERT INTO distributed SELECT i,  i::text, now() FROM generate_series(0,100)i;
INSERT INTO reference SELECT i,  i::text FROM generate_series(0,100)i;
INSERT INTO local SELECT i,  i::text FROM generate_series(0,100)i;

-- A planned subquery is not an eliminated nullable relation.
CREATE TABLE outer_join_ref (id int);
SELECT create_reference_table('outer_join_ref');
CREATE TABLE outer_join_dist (id int);
SELECT create_distributed_table('outer_join_dist', 'id');
INSERT INTO outer_join_ref SELECT generate_series(0, 10);
INSERT INTO outer_join_ref SELECT generate_series(50, 70);
INSERT INTO outer_join_ref VALUES (NULL), (NULL), (0);
INSERT INTO outer_join_dist SELECT generate_series(0, 10);
INSERT INTO outer_join_dist SELECT generate_series(70, 90);
INSERT INTO outer_join_dist VALUES (0);
WITH joined AS (
    SELECT r.id FROM outer_join_ref r
    WHERE r.id NOT IN (
        SELECT r2.id FROM outer_join_ref r2 LEFT JOIN outer_join_dist d USING (id)
        WHERE d.id IN (SELECT id FROM outer_join_dist)
        ORDER BY id
    )
    ORDER BY id
)
SELECT count(*), avg(result.id)
FROM (SELECT a.id FROM joined a FULL JOIN joined b USING (id) WHERE b.id > 3) result;
DROP TABLE outer_join_ref, outer_join_dist;
