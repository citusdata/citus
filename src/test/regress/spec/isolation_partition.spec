setup
{
    SET citus.shard_replication_factor to 1;
	 CREATE TABLE test(
        time date,
        id bigint,
        name character varying,
        destination bigint) PARTITION BY RANGE (id, time);

    SELECT create_distributed_table('test', 'name');

	CREATE TABLE test_6_1 PARTITION OF test FOR VALUES FROM ('6', '2021-07-12') TO ('6', '2021-07-13');
	CREATE TABLE test_7_1 PARTITION OF test FOR VALUES FROM ('7', '2021-07-13') TO ('8', '2021-07-14');
	CREATE TABLE test_8_1 PARTITION OF test FOR VALUES FROM ('8', '2021-07-14') TO ('9', '2021-07-15');
	CREATE TABLE test_9_1 PARTITION OF test FOR VALUES FROM ('9', '2021-07-15') TO ('10', '2021-07-16');


}

teardown
{
        DROP TABLE IF EXISTS test CASCADE;
}

session "s1"

step "s1-begin" {
    BEGIN;
}

step "s1-update" {
    update test_9_1 set destination  = 1;
}

step "s1-commit" {
    COMMIT;
}

session "s2"

step "s2-begin" {
    BEGIN;
}

step "s2-drop" {
    drop table test_6_1;
}

step "s2-create" {
    CREATE TABLE test_10_1 PARTITION OF test FOR VALUES FROM ('10', '2021-07-16') TO ('11', '2021-07-17');

}

step "s2-commit" {
    COMMIT;
}

permutation "s1-begin" "s1-update" "s2-drop" "s1-commit"
permutation "s2-begin" "s2-drop" "s1-update" "s2-commit"
permutation "s1-begin" "s1-update" "s2-create" "s1-commit"
permutation "s2-begin" "s2-create" "s1-update" "s2-commit"
