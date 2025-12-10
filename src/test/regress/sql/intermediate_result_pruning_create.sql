CREATE SCHEMA "intermediate result pruning";
SET search_path TO "intermediate result pruning";
GRANT ALL ON SCHEMA "intermediate result pruning" TO regularuser;

CREATE TABLE table_1 (key int, value text);
SELECT create_distributed_table('table_1', 'key');

CREATE TABLE table_2 (key int, value text);
SELECT create_distributed_table('table_2', 'key');


CREATE TABLE table_3 (key int, value text);
SELECT create_distributed_table('table_3', 'key');

CREATE TABLE ref_table (key int, value text);
SELECT create_reference_table('ref_table');


-- load some data
INSERT INTO table_1    VALUES (1, '1'), (2, '2'), (3, '3'), (4, '4');
INSERT INTO table_2    VALUES                     (3, '3'), (4, '4'), (5, '5'), (6, '6');
INSERT INTO table_3    VALUES                     (3, '3'), (4, '4'), (5, '5'), (6, '6');
INSERT INTO ref_table  VALUES (1, '1'), (2, '2'), (3, '3'), (4, '4'), (5, '5'), (6, '6');


CREATE TABLE accounts (id text PRIMARY KEY);
CREATE TABLE stats (account_id text PRIMARY KEY, spent int);

SELECT create_distributed_table('accounts', 'id', colocate_with => 'none');
SELECT create_distributed_table('stats', 'account_id', colocate_with => 'accounts');

INSERT INTO accounts (id) VALUES ('foo');
INSERT INTO stats (account_id, spent) VALUES ('foo', 100);
