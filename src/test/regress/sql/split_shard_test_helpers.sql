-- File to create functions and helpers needed for split shard tests

-- Populates shared memory mapping for parent shard with id 1.
-- targetNode1, targetNode2 are the locations where child shard 2 and 3 are placed respectively
CREATE OR REPLACE FUNCTION split_shard_replication_setup_helper(targetNode1 integer, targetNode2 integer) RETURNS text AS $$
DECLARE
    memoryId bigint := 0;
    memoryIdText text;
begin
	SELECT * into memoryId from split_shard_replication_setup(ARRAY[ARRAY[1,2,-2147483648,-1, targetNode1], ARRAY[1,3,0,2147483647,targetNode2]]);
    SELECT FORMAT('%s', memoryId) into memoryIdText;
    return memoryIdText;
end
$$ LANGUAGE plpgsql;

-- Create replication slots for targetNode1 and targetNode2 incase of non-colocated shards
CREATE OR REPLACE FUNCTION create_replication_slot(targetNode1 integer, targetNode2 integer) RETURNS text AS $$
DECLARE
    targetOneSlotName text;
    targetTwoSlotName text;
    sharedMemoryId text;
    derivedSlotName text;
begin

    SELECT * into sharedMemoryId from public.split_shard_replication_setup_helper(targetNode1, targetNode2);
    SELECT FORMAT('%s_%s_10', targetNode1, sharedMemoryId) into derivedSlotName;
    SELECT slot_name into targetOneSlotName from pg_create_logical_replication_slot(derivedSlotName, 'decoding_plugin_for_shard_split');

    -- if new child shards are placed on different nodes, create one more replication slot
    if (targetNode1 != targetNode2) then
        SELECT FORMAT('%s_%s_10', targetNode2, sharedMemoryId) into derivedSlotName;
        SELECT slot_name into targetTwoSlotName from pg_create_logical_replication_slot(derivedSlotName, 'decoding_plugin_for_shard_split');
        INSERT INTO slotName_table values(targetTwoSlotName, targetNode2, 1);
    end if;

    INSERT INTO slotName_table values(targetOneSlotName, targetNode1, 2);
    return targetOneSlotName;
end
$$ LANGUAGE plpgsql;

-- Populates shared memory mapping for colocated parent shards 4 and 7.
-- shard 4 has child shards 5 and 6. Shard 7 has child shards 8 and 9.
CREATE OR REPLACE FUNCTION split_shard_replication_setup_for_colocated_shards(targetNode1 integer, targetNode2 integer) RETURNS text AS $$
DECLARE
    memoryId bigint := 0;
    memoryIdText text;
begin
	SELECT * into memoryId from split_shard_replication_setup(
    ARRAY[
          ARRAY[4, 5, -2147483648,-1, targetNode1],
          ARRAY[4, 6, 0 ,2147483647,  targetNode2],
          ARRAY[7, 8, -2147483648,-1,  targetNode1],
          ARRAY[7, 9, 0, 2147483647 , targetNode2]
        ]);

    SELECT FORMAT('%s', memoryId) into memoryIdText;
    return memoryIdText;
end
$$ LANGUAGE plpgsql;

-- Create replication slots for targetNode1 and targetNode2 incase of colocated shards
CREATE OR REPLACE FUNCTION create_replication_slot_for_colocated_shards(targetNode1 integer, targetNode2 integer) RETURNS text AS $$
DECLARE
    targetOneSlotName text;
    targetTwoSlotName text;
    sharedMemoryId text;
    derivedSlotNameOne text;
    derivedSlotNameTwo text;
    tableOwnerOne bigint;
    tableOwnerTwo bigint;
begin
    -- setup shared memory information
    SELECT * into sharedMemoryId from public.split_shard_replication_setup_for_colocated_shards(targetNode1, targetNode2);

    SELECT relowner into tableOwnerOne from pg_class where relname='table_first';
    SELECT FORMAT('%s_%s_%s', targetNode1, sharedMemoryId, tableOwnerOne) into derivedSlotNameOne;
    SELECT slot_name into targetOneSlotName from pg_create_logical_replication_slot(derivedSlotNameOne, 'decoding_plugin_for_shard_split');

    SELECT relowner into tableOwnerTwo from pg_class where relname='table_second';
    SELECT FORMAT('%s_%s_%s', targetNode2, sharedMemoryId, tableOwnerTwo) into derivedSlotNameTwo;
    SELECT slot_name into targetTwoSlotName from pg_create_logical_replication_slot(derivedSlotNameTwo, 'decoding_plugin_for_shard_split');


    INSERT INTO slotName_table values(targetOneSlotName, targetNode1, 1);
    INSERT INTO slotName_table values(targetTwoSlotName, targetNode2, 2);

    return targetOneSlotName;
end
$$ LANGUAGE plpgsql;

-- create subscription on target node with given 'subscriptionName'
CREATE OR REPLACE FUNCTION create_subscription(targetNodeId integer, subscriptionName text) RETURNS text AS $$
DECLARE
    replicationSlotName text;
    nodeportLocal int;
    subname text;
begin
    SELECT name into replicationSlotName from slotName_table where nodeId = targetNodeId;
    EXECUTE FORMAT($sub$create subscription %s connection 'host=localhost port=57637 user=postgres dbname=regression' publication PUB1 with(create_slot=false, enabled=true, slot_name='%s', copy_data=false)$sub$, subscriptionName, replicationSlotName);
    return replicationSlotName;
end
$$ LANGUAGE plpgsql;

-- create subscription on target node with given 'subscriptionName'
CREATE OR REPLACE FUNCTION create_subscription_for_owner_one(targetNodeId integer, subscriptionName text) RETURNS text AS $$
DECLARE
    replicationSlotName text;
    nodeportLocal int;
    subname text;
begin
    SELECT name into replicationSlotName from slotName_table where id = 1;
    EXECUTE FORMAT($sub$create subscription %s connection 'host=localhost port=57637 user=postgres dbname=regression' publication PUB1 with(create_slot=false, enabled=true, slot_name='%s', copy_data=false)$sub$, subscriptionName, replicationSlotName);
    RAISE NOTICE 'sameer %', replicationSlotName;
    return replicationSlotName;
end
$$ LANGUAGE plpgsql;

-- create subscription on target node with given 'subscriptionName'
CREATE OR REPLACE FUNCTION create_subscription_for_owner_two(targetNodeId integer, subscriptionName text) RETURNS text AS $$
DECLARE
    replicationSlotName text;
    nodeportLocal int;
    subname text;
begin
    SELECT name into replicationSlotName from slotName_table where id = 2;
    EXECUTE FORMAT($sub$create subscription %s connection 'host=localhost port=57637 user=postgres dbname=regression' publication PUB2 with(create_slot=false, enabled=true, slot_name='%s', copy_data=false)$sub$, subscriptionName, replicationSlotName);
    RAISE NOTICE 'sameer %', replicationSlotName;
    return replicationSlotName;
end
$$ LANGUAGE plpgsql;
