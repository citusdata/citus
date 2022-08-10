
-- This test relies on metadata being synced
-- that's why is should be executed on MX schedule
CREATE SCHEMA coordinator_evaluation_combinations_modify;
SET search_path TO coordinator_evaluation_combinations_modify;

-- in this test, we are considering combinations of
-- several Citus features, and there is one prepared
-- statement for the combinations of following:
--     (a) Router Modify vs Fast Path Router Modify
--     (b) Local Execution vs Remote Execution
--     (c) Parameters on distribution key vs Parameters on non-dist key
--         vs Non-parametrized queries
--     (d) Coordinator Function Evaluation Required vs
--         Coordinator Function Evaluation Not Required

SET citus.next_shard_id TO 1180000;

-- create a volatile function that returns the local node id
CREATE OR REPLACE FUNCTION get_local_node_id_stable()
RETURNS INT AS $$
DECLARE localGroupId int;
BEGIN
	SELECT groupid INTO localGroupId FROM pg_dist_local_group;
  RETURN localGroupId;
END; $$ language plpgsql STABLE;
SELECT create_distributed_function('get_local_node_id_stable()');

-- returns 1 on coordinator
CREATE OR REPLACE FUNCTION get_constant_stable()
RETURNS INT AS $$
BEGIN
  RETURN 1;
END; $$ language plpgsql STABLE;

CREATE TYPE user_data AS (name text, age int);

SET citus.shard_replication_factor TO 1;

CREATE TABLE user_info_data (user_id int, u_data user_data, user_index int);
SELECT create_distributed_table('user_info_data', 'user_id');

-- show that local id is 0, we'll use this information
SELECT get_local_node_id_stable();



INSERT INTO user_info_data SELECT i, ('name' || i, i % 20 + 20)::user_data, i FROM generate_series(0,7)i;

-- make sure that it is also true for  fast-path router queries with paramaters
PREPARE fast_path_router_with_param(int) AS DELETE FROM user_info_data WHERE user_id  = $1 RETURNING user_id, u_data;

execute fast_path_router_with_param(0);
execute fast_path_router_with_param(1);
execute fast_path_router_with_param(2);
execute fast_path_router_with_param(3);
execute fast_path_router_with_param(4);
execute fast_path_router_with_param(5);
execute fast_path_router_with_param(6);
execute fast_path_router_with_param(7);


-- make sure that it is also true for  fast-path router queries with paramaters
PREPARE fast_path_router_with_param_and_func(int) AS DELETE FROM user_info_data WHERE u_data = ('test', get_local_node_id_stable())::user_data AND user_id  = $1 RETURNING user_id, u_data;

INSERT INTO user_info_data SELECT i, ('test', 0)::user_data FROM generate_series(0,7)i;

-- should evaluate the function on the coordinator, hence get_local_node_id_stable() returns zero
execute fast_path_router_with_param_and_func(0);
execute fast_path_router_with_param_and_func(1);
execute fast_path_router_with_param_and_func(2);
execute fast_path_router_with_param_and_func(3);
execute fast_path_router_with_param_and_func(4);
execute fast_path_router_with_param_and_func(5);
execute fast_path_router_with_param_and_func(6);
execute fast_path_router_with_param_and_func(7);

INSERT INTO user_info_data SELECT 1, ('test', 0)::user_data, i FROM generate_series(0,7)i;

PREPARE fast_path_router_with_param_and_func_on_non_dist_key(int) AS
	DELETE FROM user_info_data WHERE user_id = 1 AND user_index = $1 AND u_data = ('test', get_local_node_id_stable())::user_data RETURNING *;

EXECUTE fast_path_router_with_param_and_func_on_non_dist_key(0);
EXECUTE fast_path_router_with_param_and_func_on_non_dist_key(1);
EXECUTE fast_path_router_with_param_and_func_on_non_dist_key(2);
EXECUTE fast_path_router_with_param_and_func_on_non_dist_key(3);
EXECUTE fast_path_router_with_param_and_func_on_non_dist_key(4);
EXECUTE fast_path_router_with_param_and_func_on_non_dist_key(5);
EXECUTE fast_path_router_with_param_and_func_on_non_dist_key(6);
EXECUTE fast_path_router_with_param_and_func_on_non_dist_key(7);


INSERT INTO user_info_data SELECT 1, ('test' || i, 0)::user_data FROM generate_series(0,7)i;

PREPARE fast_path_router_with_param_on_non_dist_key_and_func(user_data) AS DELETE FROM user_info_data WHERE u_data = $1 AND user_id  = 1 RETURNING user_id, u_data;
EXECUTE fast_path_router_with_param_on_non_dist_key_and_func(('test0', get_local_node_id_stable())::user_data);
EXECUTE fast_path_router_with_param_on_non_dist_key_and_func(('test1', get_local_node_id_stable())::user_data);
EXECUTE fast_path_router_with_param_on_non_dist_key_and_func(('test2', get_local_node_id_stable())::user_data);
EXECUTE fast_path_router_with_param_on_non_dist_key_and_func(('test3', get_local_node_id_stable())::user_data);
EXECUTE fast_path_router_with_param_on_non_dist_key_and_func(('test4', get_local_node_id_stable())::user_data);
EXECUTE fast_path_router_with_param_on_non_dist_key_and_func(('test5', get_local_node_id_stable())::user_data);
EXECUTE fast_path_router_with_param_on_non_dist_key_and_func(('test6', get_local_node_id_stable())::user_data);
EXECUTE fast_path_router_with_param_on_non_dist_key_and_func(('test7', get_local_node_id_stable())::user_data);


INSERT INTO user_info_data SELECT 1, ('test', i)::user_data FROM generate_series(0,7)i;

PREPARE fast_path_router_with_param_on_non_dist_key(user_data) AS DELETE FROM user_info_data WHERE u_data = $1 AND user_id  = 1 RETURNING
 user_id, u_data;
EXECUTE fast_path_router_with_param_on_non_dist_key(('test', 0)::user_data);
EXECUTE fast_path_router_with_param_on_non_dist_key(('test', 1)::user_data);
EXECUTE fast_path_router_with_param_on_non_dist_key(('test', 2)::user_data);
EXECUTE fast_path_router_with_param_on_non_dist_key(('test', 3)::user_data);
EXECUTE fast_path_router_with_param_on_non_dist_key(('test', 4)::user_data);
EXECUTE fast_path_router_with_param_on_non_dist_key(('test', 5)::user_data);
EXECUTE fast_path_router_with_param_on_non_dist_key(('test', 6)::user_data);
EXECUTE fast_path_router_with_param_on_non_dist_key(('test', 7)::user_data);


INSERT INTO user_info_data SELECT i, ('test', i)::user_data FROM generate_series(0,7)i;

PREPARE fast_path_router_with_two_params(user_data, int) AS DELETE FROM user_info_data WHERE u_data = $1 AND user_id  = $2 RETURNING
 user_id, u_data;

EXECUTE fast_path_router_with_two_params(('test', 0)::user_data, 0);
EXECUTE fast_path_router_with_two_params(('test', 1)::user_data, 1);
EXECUTE fast_path_router_with_two_params(('test', 2)::user_data, 2);
EXECUTE fast_path_router_with_two_params(('test', 3)::user_data, 3);
EXECUTE fast_path_router_with_two_params(('test', 4)::user_data, 4);
EXECUTE fast_path_router_with_two_params(('test', 5)::user_data, 5);
EXECUTE fast_path_router_with_two_params(('test', 6)::user_data, 6);
EXECUTE fast_path_router_with_two_params(('test', 7)::user_data, 7);


INSERT INTO user_info_data VALUES(1, ('test', 1)::user_data);

PREPARE fast_path_router_with_only_function AS DELETE FROM user_info_data WHERE get_local_node_id_stable() = 0 AND user_id = 1 RETURNING user_id, u_data;
EXECUTE fast_path_router_with_only_function;
INSERT INTO user_info_data VALUES(1, ('test', 1)::user_data);
EXECUTE fast_path_router_with_only_function;
INSERT INTO user_info_data VALUES(1, ('test', 1)::user_data);
EXECUTE fast_path_router_with_only_function;
INSERT INTO user_info_data VALUES(1, ('test', 1)::user_data);
EXECUTE fast_path_router_with_only_function;
INSERT INTO user_info_data VALUES(1, ('test', 1)::user_data);
EXECUTE fast_path_router_with_only_function;
INSERT INTO user_info_data VALUES(1, ('test', 1)::user_data);
EXECUTE fast_path_router_with_only_function;
INSERT INTO user_info_data VALUES(1, ('test', 1)::user_data);
EXECUTE fast_path_router_with_only_function;
INSERT INTO user_info_data VALUES(1, ('test', 1)::user_data);
EXECUTE fast_path_router_with_only_function;

PREPARE insert_with_function_and_param(user_data) AS INSERT INTO user_info_data VALUES (1, $1, get_local_node_id_stable()) RETURNING user_id;
EXECUTE insert_with_function_and_param(('test', 1)::user_data);
EXECUTE insert_with_function_and_param(('test', 1)::user_data);
EXECUTE insert_with_function_and_param(('test', 1)::user_data);
EXECUTE insert_with_function_and_param(('test', 1)::user_data);
EXECUTE insert_with_function_and_param(('test', 1)::user_data);
EXECUTE insert_with_function_and_param(('test', 1)::user_data);
EXECUTE insert_with_function_and_param(('test', 1)::user_data);
EXECUTE insert_with_function_and_param(('test', 1)::user_data);

TRUNCATE user_info_data;

INSERT INTO user_info_data SELECT i, ('test', i)::user_data FROM generate_series(0,7)i;

-- make sure that it is also true for non fast-path router queries with paramaters
PREPARE router_with_param(int) AS DELETE FROM user_info_data WHERE user_id  = $1 AND user_id = $1 RETURNING user_id, u_data;

execute router_with_param(0);
execute router_with_param(1);
execute router_with_param(2);
execute router_with_param(3);
execute router_with_param(4);
execute router_with_param(5);
execute router_with_param(6);
execute router_with_param(7);


-- make sure that it is also true for non fast-path router queries with paramaters
PREPARE router_with_param_and_func(int) AS DELETE FROM user_info_data WHERE u_data = ('test', get_local_node_id_stable())::user_data AND user_id  = $1 AND user_id  = $1 RETURNING user_id, u_data;

INSERT INTO user_info_data SELECT i, ('test', 0)::user_data FROM generate_series(0,7)i;

execute router_with_param_and_func(0);
execute router_with_param_and_func(1);
execute router_with_param_and_func(2);
execute router_with_param_and_func(3);
execute router_with_param_and_func(4);
execute router_with_param_and_func(5);
execute router_with_param_and_func(6);
execute router_with_param_and_func(7);

INSERT INTO user_info_data SELECT 1, ('test', 0)::user_data, i FROM generate_series(0,7)i;

PREPARE router_with_param_and_func_on_non_dist_key(int) AS
	DELETE FROM user_info_data WHERE user_id = 1 AND user_id = 1 AND user_index = $1 AND u_data = ('test', get_local_node_id_stable())::user_data RETURNING *;

EXECUTE router_with_param_and_func_on_non_dist_key(0);
EXECUTE router_with_param_and_func_on_non_dist_key(1);
EXECUTE router_with_param_and_func_on_non_dist_key(2);
EXECUTE router_with_param_and_func_on_non_dist_key(3);
EXECUTE router_with_param_and_func_on_non_dist_key(4);
EXECUTE router_with_param_and_func_on_non_dist_key(5);
EXECUTE router_with_param_and_func_on_non_dist_key(6);
EXECUTE router_with_param_and_func_on_non_dist_key(7);


INSERT INTO user_info_data SELECT 1, ('test' || i, 0)::user_data FROM generate_series(0,7)i;

PREPARE router_with_param_on_non_dist_key_and_func(user_data) AS DELETE FROM user_info_data WHERE u_data = $1 AND user_id = 1 AND user_id  = 1 RETURNING user_id, u_data;
EXECUTE router_with_param_on_non_dist_key_and_func(('test0', get_local_node_id_stable())::user_data);
EXECUTE router_with_param_on_non_dist_key_and_func(('test1', get_local_node_id_stable())::user_data);
EXECUTE router_with_param_on_non_dist_key_and_func(('test2', get_local_node_id_stable())::user_data);
EXECUTE router_with_param_on_non_dist_key_and_func(('test3', get_local_node_id_stable())::user_data);
EXECUTE router_with_param_on_non_dist_key_and_func(('test4', get_local_node_id_stable())::user_data);
EXECUTE router_with_param_on_non_dist_key_and_func(('test5', get_local_node_id_stable())::user_data);
EXECUTE router_with_param_on_non_dist_key_and_func(('test6', get_local_node_id_stable())::user_data);
EXECUTE router_with_param_on_non_dist_key_and_func(('test7', get_local_node_id_stable())::user_data);


INSERT INTO user_info_data SELECT 1, ('test', i)::user_data FROM generate_series(0,7)i;

PREPARE router_with_param_on_non_dist_key(user_data) AS DELETE FROM user_info_data WHERE u_data = $1 AND user_id  = 1 AND user_id  = 1 RETURNING user_id, u_data;
EXECUTE router_with_param_on_non_dist_key(('test', 0)::user_data);
EXECUTE router_with_param_on_non_dist_key(('test', 1)::user_data);
EXECUTE router_with_param_on_non_dist_key(('test', 2)::user_data);
EXECUTE router_with_param_on_non_dist_key(('test', 3)::user_data);
EXECUTE router_with_param_on_non_dist_key(('test', 4)::user_data);
EXECUTE router_with_param_on_non_dist_key(('test', 5)::user_data);
EXECUTE router_with_param_on_non_dist_key(('test', 6)::user_data);
EXECUTE router_with_param_on_non_dist_key(('test', 7)::user_data);



INSERT INTO user_info_data SELECT i, ('test', i)::user_data FROM generate_series(0,7)i;

PREPARE router_with_two_params(user_data, int) AS DELETE FROM user_info_data WHERE u_data = $1 AND user_id  = $2 AND user_id  = $2 RETURNING
 user_id, u_data;

EXECUTE router_with_two_params(('test', 0)::user_data, 0);
EXECUTE router_with_two_params(('test', 1)::user_data, 1);
EXECUTE router_with_two_params(('test', 2)::user_data, 2);
EXECUTE router_with_two_params(('test', 3)::user_data, 3);
EXECUTE router_with_two_params(('test', 4)::user_data, 4);
EXECUTE router_with_two_params(('test', 5)::user_data, 5);
EXECUTE router_with_two_params(('test', 6)::user_data, 6);
EXECUTE router_with_two_params(('test', 7)::user_data, 7);

INSERT INTO user_info_data VALUES(1, ('test', 1)::user_data);

PREPARE router_with_only_function AS DELETE FROM user_info_data WHERE get_local_node_id_stable() = 0 AND user_id = 1 AND user_id = 1 RETURNING user_id, u_data;
EXECUTE router_with_only_function;
INSERT INTO user_info_data VALUES(1, ('test', 1)::user_data);
EXECUTE router_with_only_function;
INSERT INTO user_info_data VALUES(1, ('test', 1)::user_data);
EXECUTE router_with_only_function;
INSERT INTO user_info_data VALUES(1, ('test', 1)::user_data);
EXECUTE router_with_only_function;
INSERT INTO user_info_data VALUES(1, ('test', 1)::user_data);
EXECUTE router_with_only_function;
INSERT INTO user_info_data VALUES(1, ('test', 1)::user_data);
EXECUTE router_with_only_function;
INSERT INTO user_info_data VALUES(1, ('test', 1)::user_data);
EXECUTE router_with_only_function;
INSERT INTO user_info_data VALUES(1, ('test', 1)::user_data);
EXECUTE router_with_only_function;
INSERT INTO user_info_data VALUES(1, ('test', 1)::user_data);
EXECUTE router_with_only_function;

\c - - - :worker_2_port

SET citus.log_local_commands TO ON;
SET search_path TO coordinator_evaluation_combinations_modify;

-- returns 2 on the worker
SET citus.enable_metadata_sync TO OFF;
CREATE OR REPLACE FUNCTION get_constant_stable()
RETURNS INT AS $$
BEGIN
  RETURN 2;
END; $$ language plpgsql STABLE;
RESET citus.enable_metadata_sync;


-- all local values
INSERT INTO user_info_data (user_id, u_data) VALUES
(3, '(''test3'', 3)'), (4, '(''test4'', 4)'), (7, '(''test7'', 7)'),
(9, '(''test9'', 9)'), (11, '(''test11'', 11)'), (12, '(''test12'', 12)'),
(14, '(''test14'', 14)'), (16, '(''test16'', 16)');


-- make sure that it is also true for  fast-path router queries with paramaters
PREPARE fast_path_router_with_param(int) AS DELETE FROM user_info_data WHERE user_id  = $1 RETURNING user_id, u_data;

execute fast_path_router_with_param(3);
execute fast_path_router_with_param(4);
execute fast_path_router_with_param(7);
execute fast_path_router_with_param(9);
execute fast_path_router_with_param(11);
execute fast_path_router_with_param(12);
execute fast_path_router_with_param(14);
execute fast_path_router_with_param(16);


INSERT INTO user_info_data (user_id, u_data) VALUES
(3, '(''test'', 2)'), (4, '(''test'', 2)'), (7, '(''test'', 2)'),
(9, '(''test'', 9)'), (11, '(''test'', 2)'), (12, '(''test'', 2)'),
(14, '(''test'', 2)'), (16, '(''test'', 2)');

-- make sure that it is also true for  fast-path router queries with paramaters
PREPARE fast_path_router_with_param_and_func(int) AS DELETE FROM user_info_data WHERE u_data = ('''test''', get_constant_stable())::user_data AND user_id  = $1 RETURNING user_id, u_data;

execute fast_path_router_with_param_and_func(3);
execute fast_path_router_with_param_and_func(4);
execute fast_path_router_with_param_and_func(7);
execute fast_path_router_with_param_and_func(9);
execute fast_path_router_with_param_and_func(11);
execute fast_path_router_with_param_and_func(12);
execute fast_path_router_with_param_and_func(14);
execute fast_path_router_with_param_and_func(16);


INSERT INTO user_info_data SELECT 3, ('test', get_local_node_id_stable() > 0)::user_data, i FROM generate_series(0,7)i;

PREPARE fast_path_router_with_param_and_func_on_non_dist_key(int) AS
	DELETE FROM user_info_data WHERE user_id = 3 AND user_index = $1 AND u_data = ('test', (get_local_node_id_stable() > 0)::int)::user_data RETURNING user_id, user_index;

EXECUTE fast_path_router_with_param_and_func_on_non_dist_key(0);
EXECUTE fast_path_router_with_param_and_func_on_non_dist_key(1);
EXECUTE fast_path_router_with_param_and_func_on_non_dist_key(2);
EXECUTE fast_path_router_with_param_and_func_on_non_dist_key(3);
EXECUTE fast_path_router_with_param_and_func_on_non_dist_key(4);
EXECUTE fast_path_router_with_param_and_func_on_non_dist_key(5);
EXECUTE fast_path_router_with_param_and_func_on_non_dist_key(6);
EXECUTE fast_path_router_with_param_and_func_on_non_dist_key(7);

PREPARE fast_path_router_with_param_on_non_dist_key_and_func(user_data) AS DELETE FROM user_info_data WHERE u_data = $1 AND user_id  = 3 RETURNING user_id, u_data;
INSERT INTO user_info_data (user_id, u_data) VALUES  (3, '(''test'', 2)'::user_data);
EXECUTE fast_path_router_with_param_on_non_dist_key_and_func(('''test''', get_constant_stable())::user_data);
INSERT INTO user_info_data (user_id, u_data) VALUES  (3, '(''test'', 2)');
EXECUTE fast_path_router_with_param_on_non_dist_key_and_func(('''test''', get_constant_stable())::user_data);
INSERT INTO user_info_data (user_id, u_data) VALUES  (3, '(''test'', 2)');
EXECUTE fast_path_router_with_param_on_non_dist_key_and_func(('''test''', get_constant_stable())::user_data);
INSERT INTO user_info_data (user_id, u_data) VALUES  (3, '(''test'', 2)');
EXECUTE fast_path_router_with_param_on_non_dist_key_and_func(('''test''', get_constant_stable())::user_data);
INSERT INTO user_info_data (user_id, u_data) VALUES  (3, '(''test'', 2)');
EXECUTE fast_path_router_with_param_on_non_dist_key_and_func(('''test''', get_constant_stable())::user_data);
INSERT INTO user_info_data (user_id, u_data) VALUES  (3, '(''test'', 2)');
EXECUTE fast_path_router_with_param_on_non_dist_key_and_func(('''test''', get_constant_stable())::user_data);
INSERT INTO user_info_data (user_id, u_data) VALUES  (3, '(''test'', 2)');
EXECUTE fast_path_router_with_param_on_non_dist_key_and_func(('''test''', get_constant_stable())::user_data);
INSERT INTO user_info_data (user_id, u_data) VALUES  (3, '(''test'', 2)');
EXECUTE fast_path_router_with_param_on_non_dist_key_and_func(('''test''', get_constant_stable())::user_data);

PREPARE fast_path_router_with_param_on_non_dist_key(user_data) AS DELETE FROM user_info_data WHERE u_data = $1 AND user_id  = 3 RETURNING user_id, u_data;
INSERT INTO user_info_data (user_id, u_data) VALUES  (3, ('test', 1)::user_data);
EXECUTE fast_path_router_with_param_on_non_dist_key(('test', 1)::user_data);
INSERT INTO user_info_data (user_id, u_data) VALUES  (3, ('test', 1)::user_data);
EXECUTE fast_path_router_with_param_on_non_dist_key(('test', 1)::user_data);
INSERT INTO user_info_data (user_id, u_data) VALUES  (3, ('test', 1)::user_data);
EXECUTE fast_path_router_with_param_on_non_dist_key(('test', 1)::user_data);
INSERT INTO user_info_data (user_id, u_data) VALUES  (3, ('test', 1)::user_data);
EXECUTE fast_path_router_with_param_on_non_dist_key(('test', 1)::user_data);
INSERT INTO user_info_data (user_id, u_data) VALUES  (3, ('test', 1)::user_data);
EXECUTE fast_path_router_with_param_on_non_dist_key(('test', 1)::user_data);
INSERT INTO user_info_data (user_id, u_data) VALUES  (3, ('test', 1)::user_data);
EXECUTE fast_path_router_with_param_on_non_dist_key(('test', 1)::user_data);
INSERT INTO user_info_data (user_id, u_data) VALUES  (3, ('test', 1)::user_data);
EXECUTE fast_path_router_with_param_on_non_dist_key(('test', 1)::user_data);
INSERT INTO user_info_data (user_id, u_data) VALUES  (3, ('test', 1)::user_data);
EXECUTE fast_path_router_with_param_on_non_dist_key(('test', 1)::user_data);


INSERT INTO user_info_data (user_id, u_data) VALUES
(3, ('test', 2)), (4, ('test', 2)), (7, ('test', 2)),
(9, ('test', 2)), (11, ('test', 2)), (12, ('test', 2)),
(14, ('test', 2)), (16, ('test', 2));

PREPARE fast_path_router_with_two_params(user_data, int) AS DELETE FROM user_info_data WHERE u_data = $1 AND user_id  = $2 RETURNING user_id, u_data;

EXECUTE fast_path_router_with_two_params(('test', 2)::user_data, 3);
EXECUTE fast_path_router_with_two_params(('test', 2)::user_data, 4);
EXECUTE fast_path_router_with_two_params(('test', 2)::user_data, 7);
EXECUTE fast_path_router_with_two_params(('test', 2)::user_data, 9);
EXECUTE fast_path_router_with_two_params(('test', 2)::user_data, 11);
EXECUTE fast_path_router_with_two_params(('test', 2)::user_data, 12);
EXECUTE fast_path_router_with_two_params(('test', 2)::user_data, 14);
EXECUTE fast_path_router_with_two_params(('test', 2)::user_data, 16);


PREPARE fast_path_router_with_only_function AS DELETE FROM user_info_data WHERE get_constant_stable() = 2 AND user_id = 3 RETURNING user_id, u_data;
INSERT INTO user_info_data (user_id, u_data) VALUES  (3, ('test', 2)::user_data);
EXECUTE fast_path_router_with_only_function;
INSERT INTO user_info_data (user_id, u_data) VALUES  (3, ('test', 2)::user_data);
EXECUTE fast_path_router_with_only_function;
INSERT INTO user_info_data (user_id, u_data) VALUES  (3, ('test', 2)::user_data);
EXECUTE fast_path_router_with_only_function;
INSERT INTO user_info_data (user_id, u_data) VALUES  (3, ('test', 2)::user_data);
EXECUTE fast_path_router_with_only_function;
INSERT INTO user_info_data (user_id, u_data) VALUES  (3, ('test', 2)::user_data);
EXECUTE fast_path_router_with_only_function;
INSERT INTO user_info_data (user_id, u_data) VALUES  (3, ('test', 2)::user_data);
EXECUTE fast_path_router_with_only_function;
INSERT INTO user_info_data (user_id, u_data) VALUES  (3, ('test', 2)::user_data);
EXECUTE fast_path_router_with_only_function;
INSERT INTO user_info_data (user_id, u_data) VALUES  (3, ('test', 2)::user_data);
EXECUTE fast_path_router_with_only_function;

PREPARE insert_with_function_and_param(user_data) AS INSERT INTO user_info_data VALUES (3, $1, (get_local_node_id_stable() > 0)::int) RETURNING user_id;
EXECUTE insert_with_function_and_param(('test', 1)::user_data);
EXECUTE insert_with_function_and_param(('test', 1)::user_data);
EXECUTE insert_with_function_and_param(('test', 1)::user_data);
EXECUTE insert_with_function_and_param(('test', 1)::user_data);
EXECUTE insert_with_function_and_param(('test', 1)::user_data);
EXECUTE insert_with_function_and_param(('test', 1)::user_data);
EXECUTE insert_with_function_and_param(('test', 1)::user_data);
EXECUTE insert_with_function_and_param(('test', 1)::user_data);

TRUNCATE user_info_data;

-- all local values
INSERT INTO user_info_data (user_id, u_data) VALUES
(3, '(''test3'', 3)'), (4, '(''test4'', 4)'), (7, '(''test7'', 7)'),
(9, '(''test9'', 9)'), (11, '(''test11'', 11)'), (12, '(''test12'', 12)'),
(14, '(''test14'', 14)'), (16, '(''test16'', 16)');

-- make sure that it is also true for  fast-path router queries with paramaters
PREPARE router_with_param(int) AS DELETE FROM user_info_data WHERE user_id  = $1 AND user_id = $1 RETURNING user_id, u_data;

execute router_with_param(3);
execute router_with_param(4);
execute router_with_param(7);
execute router_with_param(9);
execute router_with_param(11);
execute router_with_param(12);
execute router_with_param(14);
execute router_with_param(16);


INSERT INTO user_info_data (user_id, u_data) VALUES
(3, '(''test'', 2)'), (4, '(''test'', 2)'), (7, '(''test'', 2)'),
(9, '(''test'', 9)'), (11, '(''test'', 2)'), (12, '(''test'', 2)'),
(14, '(''test'', 2)'), (16, '(''test'', 2)');

-- make sure that it is also true for  fast-path router queries with paramaters
PREPARE router_with_param_and_func(int) AS DELETE FROM user_info_data WHERE u_data = ('''test''', get_constant_stable())::user_data AND user_id  = $1 AND user_id  = $1 RETURNING user_id, u_data;

execute router_with_param_and_func(3);
execute router_with_param_and_func(4);
execute router_with_param_and_func(7);
execute router_with_param_and_func(9);
execute router_with_param_and_func(11);
execute router_with_param_and_func(12);
execute router_with_param_and_func(14);
execute router_with_param_and_func(16);


INSERT INTO user_info_data SELECT 3, ('test', get_local_node_id_stable() > 0)::user_data, i FROM generate_series(0,7)i;

PREPARE router_with_param_and_func_on_non_dist_key(int) AS
	DELETE FROM user_info_data WHERE user_id = 3 AND user_id = 3 AND user_index = $1 AND u_data = ('test', (get_local_node_id_stable() > 0)::int)::user_data RETURNING user_id, user_index;

EXECUTE router_with_param_and_func_on_non_dist_key(0);
EXECUTE router_with_param_and_func_on_non_dist_key(1);
EXECUTE router_with_param_and_func_on_non_dist_key(2);
EXECUTE router_with_param_and_func_on_non_dist_key(3);
EXECUTE router_with_param_and_func_on_non_dist_key(4);
EXECUTE router_with_param_and_func_on_non_dist_key(5);
EXECUTE router_with_param_and_func_on_non_dist_key(6);
EXECUTE router_with_param_and_func_on_non_dist_key(7);

PREPARE router_with_param_on_non_dist_key_and_func(user_data) AS DELETE FROM user_info_data WHERE u_data = $1 AND user_id  = 3 AND user_id  = 3 RETURNING user_id, u_data;
INSERT INTO user_info_data (user_id, u_data) VALUES  (3, '(''test'', 2)'::user_data);
EXECUTE router_with_param_on_non_dist_key_and_func(('''test''', get_constant_stable())::user_data);
INSERT INTO user_info_data (user_id, u_data) VALUES  (3, '(''test'', 2)');
EXECUTE router_with_param_on_non_dist_key_and_func(('''test''', get_constant_stable())::user_data);
INSERT INTO user_info_data (user_id, u_data) VALUES  (3, '(''test'', 2)');
EXECUTE router_with_param_on_non_dist_key_and_func(('''test''', get_constant_stable())::user_data);
INSERT INTO user_info_data (user_id, u_data) VALUES  (3, '(''test'', 2)');
EXECUTE router_with_param_on_non_dist_key_and_func(('''test''', get_constant_stable())::user_data);
INSERT INTO user_info_data (user_id, u_data) VALUES  (3, '(''test'', 2)');
EXECUTE router_with_param_on_non_dist_key_and_func(('''test''', get_constant_stable())::user_data);
INSERT INTO user_info_data (user_id, u_data) VALUES  (3, '(''test'', 2)');
EXECUTE router_with_param_on_non_dist_key_and_func(('''test''', get_constant_stable())::user_data);
INSERT INTO user_info_data (user_id, u_data) VALUES  (3, '(''test'', 2)');
EXECUTE router_with_param_on_non_dist_key_and_func(('''test''', get_constant_stable())::user_data);
INSERT INTO user_info_data (user_id, u_data) VALUES  (3, '(''test'', 2)');
EXECUTE router_with_param_on_non_dist_key_and_func(('''test''', get_constant_stable())::user_data);


PREPARE router_with_param_on_non_dist_key(user_data) AS DELETE FROM user_info_data WHERE u_data = $1 AND user_id  = 3 AND user_id  = 3 RETURNING user_id, u_data;
INSERT INTO user_info_data (user_id, u_data) VALUES  (3, ('test', 1)::user_data);
EXECUTE router_with_param_on_non_dist_key(('test', 1)::user_data);
INSERT INTO user_info_data (user_id, u_data) VALUES  (3, ('test', 1)::user_data);
EXECUTE router_with_param_on_non_dist_key(('test', 1)::user_data);
INSERT INTO user_info_data (user_id, u_data) VALUES  (3, ('test', 1)::user_data);
EXECUTE router_with_param_on_non_dist_key(('test', 1)::user_data);
INSERT INTO user_info_data (user_id, u_data) VALUES  (3, ('test', 1)::user_data);
EXECUTE router_with_param_on_non_dist_key(('test', 1)::user_data);
INSERT INTO user_info_data (user_id, u_data) VALUES  (3, ('test', 1)::user_data);
EXECUTE router_with_param_on_non_dist_key(('test', 1)::user_data);
INSERT INTO user_info_data (user_id, u_data) VALUES  (3, ('test', 1)::user_data);
EXECUTE router_with_param_on_non_dist_key(('test', 1)::user_data);
INSERT INTO user_info_data (user_id, u_data) VALUES  (3, ('test', 1)::user_data);
EXECUTE router_with_param_on_non_dist_key(('test', 1)::user_data);
INSERT INTO user_info_data (user_id, u_data) VALUES  (3, ('test', 1)::user_data);
EXECUTE router_with_param_on_non_dist_key(('test', 1)::user_data);


INSERT INTO user_info_data (user_id, u_data) VALUES
(3, ('test', 2)), (4, ('test', 2)), (7, ('test', 2)),
(9, ('test', 2)), (11, ('test', 2)), (12, ('test', 2)),
(14, ('test', 2)), (16, ('test', 2));

PREPARE router_with_two_params(user_data, int) AS DELETE FROM user_info_data WHERE u_data = $1 AND user_id  = $2 AND user_id  = $2 RETURNING user_id, u_data;

EXECUTE router_with_two_params(('test', 2)::user_data, 3);
EXECUTE router_with_two_params(('test', 2)::user_data, 4);
EXECUTE router_with_two_params(('test', 2)::user_data, 7);
EXECUTE router_with_two_params(('test', 2)::user_data, 9);
EXECUTE router_with_two_params(('test', 2)::user_data, 11);
EXECUTE router_with_two_params(('test', 2)::user_data, 12);
EXECUTE router_with_two_params(('test', 2)::user_data, 14);
EXECUTE router_with_two_params(('test', 2)::user_data, 16);


PREPARE router_with_only_function AS DELETE FROM user_info_data WHERE get_constant_stable() = 2 AND user_id = 3 RETURNING user_id, u_data;
INSERT INTO user_info_data (user_id, u_data) VALUES  (3, ('test', 2)::user_data);
EXECUTE router_with_only_function;
INSERT INTO user_info_data (user_id, u_data) VALUES  (3, ('test', 2)::user_data);
EXECUTE router_with_only_function;
INSERT INTO user_info_data (user_id, u_data) VALUES  (3, ('test', 2)::user_data);
EXECUTE router_with_only_function;
INSERT INTO user_info_data (user_id, u_data) VALUES  (3, ('test', 2)::user_data);
EXECUTE router_with_only_function;
INSERT INTO user_info_data (user_id, u_data) VALUES  (3, ('test', 2)::user_data);
EXECUTE router_with_only_function;
INSERT INTO user_info_data (user_id, u_data) VALUES  (3, ('test', 2)::user_data);
EXECUTE router_with_only_function;
INSERT INTO user_info_data (user_id, u_data) VALUES  (3, ('test', 2)::user_data);
EXECUTE router_with_only_function;
INSERT INTO user_info_data (user_id, u_data) VALUES  (3, ('test', 2)::user_data);
EXECUTE router_with_only_function;

-- suppress notices
\c - - - :master_port
SET client_min_messages TO ERROR;
DROP SCHEMA coordinator_evaluation_combinations_modify CASCADE;
