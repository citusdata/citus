CREATE TABLE dist(a int, b int);
SELECT create_distributed_table('dist', 'a');

CREATE table local(a int, b int);

INSERT INTO dist SELECT *,* FROM generate_series(1,100);
INSERT INTO local SELECT *,* FROM generate_series(1,100);

SELECT COUNT(*) FROM dist join local USING (a);
SELECT COUNT(*) FROM dist join local USING (a) WHERE dist.a =5;
