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
