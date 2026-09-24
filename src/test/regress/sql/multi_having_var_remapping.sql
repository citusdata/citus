-- HAVING aggregate arguments should preserve their original variable references
CREATE TABLE having_dist_1 (key boolean);
CREATE TABLE having_dist_2 (key boolean);
CREATE TABLE having_ref (value boolean);

SELECT create_distributed_table('having_dist_1', 'key');
SELECT create_distributed_table('having_dist_2', 'key');
SELECT create_reference_table('having_ref');

INSERT INTO having_dist_1 VALUES (true);
INSERT INTO having_dist_2 VALUES (true);
INSERT INTO having_ref VALUES (false), (NULL);

SELECT having_dist_1.key, having_dist_2.key, having_ref.value
FROM having_dist_1
JOIN having_dist_2 ON having_dist_1.key = having_dist_2.key,
having_ref
GROUP BY having_dist_1.key, having_dist_2.key, having_ref.value
HAVING EVERY(having_ref.value);

DROP TABLE having_dist_1;
DROP TABLE having_dist_2;
DROP TABLE having_ref;

-- HAVING FILTER expressions should preserve their original variable references
CREATE TABLE having_int_dist_1 (key int);
CREATE TABLE having_int_dist_2 (key int);
CREATE TABLE having_filter_ref (value boolean);

SELECT create_distributed_table('having_int_dist_1', 'key');
SELECT create_distributed_table('having_int_dist_2', 'key');
SELECT create_reference_table('having_filter_ref');

INSERT INTO having_int_dist_1 VALUES (1);
INSERT INTO having_int_dist_2 VALUES (1);
INSERT INTO having_filter_ref VALUES (true), (false), (NULL);

SELECT having_int_dist_1.key, having_int_dist_2.key, having_filter_ref.value
FROM having_int_dist_1
JOIN having_int_dist_2 ON having_int_dist_1.key = having_int_dist_2.key,
having_filter_ref
GROUP BY having_int_dist_1.key, having_int_dist_2.key, having_filter_ref.value
HAVING count(*) FILTER (WHERE having_filter_ref.value) > 0;

DROP TABLE having_int_dist_1;
DROP TABLE having_int_dist_2;
DROP TABLE having_filter_ref;
