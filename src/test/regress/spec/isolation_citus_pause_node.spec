setup
{
	SET citus.shard_replication_factor to 1;

	create table city	(id int , name text );
	SELECT create_reference_table('city');

    CREATE TABLE company(id int primary key, name text, city_id int);
	select create_distributed_table('company', 'id');

	create table employee(id int , name text, company_id int  );
	alter table employee add constraint employee_pkey primary key (id,company_id);

	select create_distributed_table('employee', 'company_id');

	insert into city values(1,'city1');
	insert into city values(2,'city2');


	insert into company values(1,'c1', 1);
	insert into company values(2,'c2',2);
	insert into company values(3,'c3',1);

	insert into employee values(1,'e1',1);
	insert into employee values(2,'e2',1);
	insert into employee values(3,'e3',1);

	insert into employee values(4,'e4',2);
	insert into employee values(5,'e5',2);
	insert into employee values(6,'e6',2);

	insert into employee values(7,'e7',3);
	insert into employee values(8,'e8',3);
	insert into employee values(9,'e9',3);
	insert into employee values(10,'e10',3);


}

teardown
{
    DROP TABLE employee,company,city;
}

session "s1"

step "s1-begin"
{
    BEGIN;
}

step "s1-node-not-found"
{
	DO $$
	DECLARE
		v_node_id int:= -1;
		v_node_exists boolean := true;
		v_exception_message text;
		v_expected_exception_message text := '';
	BEGIN
		select nextval('pg_dist_node_nodeid_seq')::int into v_node_id;
		select citus_pause_node_within_txn(v_node_id) ;
		EXCEPTION
		WHEN  SQLSTATE 'P0002' THEN
			GET STACKED DIAGNOSTICS v_exception_message = MESSAGE_TEXT;
			v_expected_exception_message := 'node ' || v_node_id || ' not found';
			if v_exception_message = v_expected_exception_message then
				RAISE NOTICE 'Node not found.';
			end if;
	END;
	$$
	LANGUAGE plpgsql;
}

step "s1-pause-node"
{
	SET client_min_messages = 'notice';
	DO $$
	DECLARE
		v_shard_id int;
		v_node_id int;
		v_node_name text;
		v_node_port int;
	BEGIN
		--The first message in the block is being printed on the top of the code block. So adding a dummy message
		--to make sure that the first message is printed in correct place.
		raise notice '';
		-- Get the shard id for the distribution column
		SELECT get_shard_id_for_distribution_column('employee', 3) into v_shard_id;

		--Get the node id for the shard id
		SELECT nodename,nodeport into v_node_name,v_node_port FROM citus_shards WHERE shardid = v_shard_id limit 1;

		-- Get the node id for the shard id
		SELECT nodeid into v_node_id FROM pg_dist_node WHERE nodename = v_node_name and nodeport = v_node_port limit 1;


		-- Pause the node
		perform pg_catalog.citus_pause_node_within_txn(v_node_id) ;
	END;
	$$
	LANGUAGE plpgsql;
}

step "s1-pause-node-force"
{
	SET client_min_messages = 'notice';
	DO $$
	DECLARE
		v_shard_id int;
		v_node_id int;
		v_node_name text;
		v_node_port int;
		v_force boolean := true;
		v_lock_cooldown int := 100;
	BEGIN
		--The first message in the block is being printed on the top of the code block. So adding a dummy message
		--to make sure that the first message is printed in correct place.

		raise notice '';
		-- Get the shard id for the distribution column
		SELECT get_shard_id_for_distribution_column('employee', 3) into v_shard_id;

		--Get the node id for the shard id
		SELECT nodename,nodeport into v_node_name,v_node_port FROM citus_shards WHERE shardid = v_shard_id limit 1;

		-- Get the node id for the shard id
		SELECT nodeid into v_node_id FROM pg_dist_node WHERE nodename = v_node_name and nodeport = v_node_port limit 1;


		-- Pause the node with force true
		perform pg_catalog.citus_pause_node_within_txn(v_node_id,v_force,v_lock_cooldown) ;
	END;
	$$
	LANGUAGE plpgsql;
}

step "s1-end"
{
    COMMIT;
}

session "s2"


step "s2-begin"
{
	BEGIN;
}

step "s2-insert-distributed"
{
	-- Execute the INSERT statement
	insert into employee values(11,'e11',3);

}

step "s2-insert-reference"{
	-- Execute the INSERT statement
	insert into city values(3,'city3');
}

step "s2-select-distributed"{

	select * from employee where id = 10;
}


step "s2-delete-distributed"{
	-- Execute the DELETE statement
	delete from employee where id = 9;
}

step "s2-end"
{
	COMMIT;
}

permutation "s1-begin" "s2-begin" "s1-pause-node" "s2-insert-distributed" "s1-end" "s2-end"
permutation "s1-begin" "s2-begin" "s1-pause-node" "s2-delete-distributed" "s1-end" "s2-end"
permutation "s1-begin"  "s1-pause-node" "s2-begin" "s2-select-distributed" "s1-end" "s2-end"
permutation "s1-begin"  "s2-begin" "s1-pause-node"  "s2-insert-reference" "s1-end" "s2-end"
permutation "s1-begin"  "s1-pause-node" "s1-pause-node" "s1-end"
permutation "s1-begin"  "s1-node-not-found" "s1-end"
permutation "s1-begin"  "s2-begin" "s2-insert-distributed" "s1-pause-node-force"(*) "s1-end" "s2-end"
