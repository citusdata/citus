/*-------------------------------------------------------------------------
 *
 * shardsplit_logical_replication.c
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "miscadmin.h"
#include "nodes/pg_list.h"
#include "distributed/colocation_utils.h"
#include "distributed/metadata_cache.h"
#include "distributed/shardinterval_utils.h"
#include "distributed/connection_management.h"
#include "distributed/remote_commands.h"
#include "distributed/shard_split.h"
#include "distributed/listutils.h"
#include "distributed/shardsplit_logical_replication.h"
#include "distributed/multi_logical_replication.h"
#include "utils/builtins.h"
#include "commands/dbcommands.h"

static HTAB *ShardInfoHashMapForPublications = NULL;

/* function declarations */
static void AddShardEntryInMap(Oid tableOwner, uint32 nodeId, ShardInterval * shardInterval, bool isChildShardInterval);
ShardSplitPubSubMetadata * CreateShardSplitPubSubMetadata(Oid tableOwnerId, uint32 nodeId, List * shardIdList, List * replicationSlotInfoList);

static void
CreateShardSplitPublicationForNode(MultiConnection *connection, List *shardList,
							uint32_t publicationForTargetNodeId, Oid tableOwner);
static void
CreateShardSplitPublications(MultiConnection *sourceConnection, List * shardSplitPubSubMetadataList);
static void
CreateShardSplitSubscriptions(List * targetNodeConnectionList, List * shardSplitPubSubMetadataList, WorkerNode * sourceWorkerNode, char * superUser, char * databaseName);
static void
WaitForShardSplitRelationSubscriptionsBecomeReady(List * targetNodeConnectionList, List * shardSplitPubSubMetadataList);

static char *
ShardSplitPublicationName(uint32_t nodeId, Oid ownerId);

/*used for debuggin. Remove later*/
void PrintShardSplitPubSubMetadata(ShardSplitPubSubMetadata * shardSplitMetadata);

StringInfo CreateSplitShardReplicationSetupUDF(List *sourceColocatedShardIntervalList,
									   List *shardGroupSplitIntervalListList,
									   List *destinationWorkerNodesList)
{
    StringInfo splitChildrenRows = makeStringInfo();

	ShardInterval *sourceShardIntervalToCopy = NULL;
	List *splitChildShardIntervalList = NULL;
	bool addComma = false;
	forboth_ptr(sourceShardIntervalToCopy, sourceColocatedShardIntervalList,
				splitChildShardIntervalList, shardGroupSplitIntervalListList)
	{
		int64 sourceShardId = sourceShardIntervalToCopy->shardId;

		ShardInterval *splitChildShardInterval = NULL;
		WorkerNode *destinationWorkerNode = NULL;
		forboth_ptr(splitChildShardInterval, splitChildShardIntervalList,
				destinationWorkerNode, destinationWorkerNodesList)
		{
			if (addComma)
			{
				appendStringInfo(splitChildrenRows, ",");
			}

			StringInfo minValueString = makeStringInfo();
			appendStringInfo(minValueString, "%d", DatumGetInt32(splitChildShardInterval->minValue));

			StringInfo maxValueString = makeStringInfo();
			appendStringInfo(maxValueString, "%d", DatumGetInt32(splitChildShardInterval->maxValue));

			appendStringInfo(splitChildrenRows,
			"ROW(%lu, %lu, %s, %s, %u)::citus.split_shard_info",
			 sourceShardId,
			 splitChildShardInterval->shardId,
			 quote_literal_cstr(minValueString->data),
			 quote_literal_cstr(maxValueString->data),
			 destinationWorkerNode->nodeId);

			 addComma = true;
		}
	}

	StringInfo splitShardReplicationUDF = makeStringInfo();
	appendStringInfo(splitShardReplicationUDF, 
		"SELECT * FROM worker_split_shard_replication_setup(ARRAY[%s])", splitChildrenRows->data);

    return splitShardReplicationUDF;
}

List * ParseReplicationSlotInfoFromResult(PGresult * result)
{
    int64 rowCount = PQntuples(result);
	int64 colCount = PQnfields(result);

    List *replicationSlotInfoList = NIL;
    for (int64 rowIndex = 0; rowIndex < rowCount; rowIndex++)
	{
        ReplicationSlotInfo * replicationSlotInfo = (ReplicationSlotInfo *)palloc0(sizeof(ReplicationSlotInfo));

        char * targeNodeIdString = PQgetvalue(result, rowIndex, 0);
        replicationSlotInfo->targetNodeId = strtoul(targeNodeIdString, NULL, 10);

        /* we're using the pstrdup to copy the data into the current memory context */
        replicationSlotInfo->tableOwnerName = pstrdup(PQgetvalue(result, rowIndex, 1));

        replicationSlotInfo->slotName = pstrdup(PQgetvalue(result, rowIndex, 2));

        replicationSlotInfoList = lappend(replicationSlotInfoList, replicationSlotInfo);
	}
    
    /*TODO(saawasek): size of this should not be NULL */
    return replicationSlotInfoList;
}


List * CreateShardSplitPubSubMetadataList(List *sourceColocatedShardIntervalList,
									   List *shardGroupSplitIntervalListList,
									   List *destinationWorkerNodesList,
                                       List *replicationSlotInfoList)
{
    ShardInfoHashMapForPublications = SetupHashMapForShardInfo();
	ShardInterval *sourceShardIntervalToCopy = NULL;
	List *splitChildShardIntervalList = NULL;
	forboth_ptr(sourceShardIntervalToCopy, sourceColocatedShardIntervalList,
				splitChildShardIntervalList, shardGroupSplitIntervalListList)
	{
		ShardInterval *splitChildShardInterval = NULL;
		WorkerNode *destinationWorkerNode = NULL;
		forboth_ptr(splitChildShardInterval, splitChildShardIntervalList,
				destinationWorkerNode, destinationWorkerNodesList)
		{
            /* Table owner is same for both parent and child shard */
            Oid tableOwnerId = TableOwnerOid(sourceShardIntervalToCopy->relationId);
            uint32 destinationWorkerNodeId = destinationWorkerNode->nodeId;

            /* Add split child shard interval */
            AddShardEntryInMap(tableOwnerId, destinationWorkerNodeId, splitChildShardInterval, true /*isChildShardInterval*/);

            /* Add parent shard interval if not already added */
            AddShardEntryInMap(tableOwnerId, destinationWorkerNodeId, sourceShardIntervalToCopy, false /*isChildShardInterval*/);
		}
	}

    /* Populate pubsub meta data*/
    HASH_SEQ_STATUS status;
	hash_seq_init(&status, ShardInfoHashMapForPublications);
	
    List * shardSplitPubSubMetadataList = NIL;
    NodeShardMappingEntry *entry = NULL;
    while ((entry = (NodeShardMappingEntry *) hash_seq_search(&status)) != NULL)
    {
       	uint32_t nodeId = entry->key.nodeId;
		uint32_t tableOwnerId = entry->key.tableOwnerId;
        ShardSplitPubSubMetadata * shardSplitPubSubMetadata = CreateShardSplitPubSubMetadata(tableOwnerId, nodeId, entry->shardSplitInfoList, replicationSlotInfoList);

        shardSplitPubSubMetadataList = lappend(shardSplitPubSubMetadataList, shardSplitPubSubMetadata);
    }

    return shardSplitPubSubMetadataList;
}

static void AddShardEntryInMap(Oid tableOwnerId, uint32 nodeId, ShardInterval * shardInterval, bool isChildShardInterval)
{
    NodeShardMappingKey key;
	key.nodeId = nodeId;
	key.tableOwnerId = tableOwnerId;

	bool found = false;
	NodeShardMappingEntry *nodeMappingEntry =
		(NodeShardMappingEntry *) hash_search(ShardInfoHashMapForPublications, &key, HASH_ENTER,
											  &found);
	if (!found)
	{
		nodeMappingEntry->shardSplitInfoList = NIL;
	}
   
    if(isChildShardInterval)
    {
        nodeMappingEntry->shardSplitInfoList =
            lappend(nodeMappingEntry->shardSplitInfoList, (ShardInterval *) shardInterval);
        return;
    }

    ShardInterval * existingShardInterval = NULL;
    foreach_ptr(existingShardInterval, nodeMappingEntry->shardSplitInfoList)
    {
        if(existingShardInterval->shardId == shardInterval->shardId)
        {
            /* parent shard interval is already added hence return */
            return;
        }
    }

    /* Add parent shard Interval */
    nodeMappingEntry->shardSplitInfoList =
        lappend(nodeMappingEntry->shardSplitInfoList, (ShardInterval *) shardInterval);

}


ShardSplitPubSubMetadata * CreateShardSplitPubSubMetadata(Oid tableOwnerId, uint32 nodeId, List * shardIntervalList, List * replicationSlotInfoList)
{

    ShardSplitPubSubMetadata * shardSplitPubSubMetadata = palloc0(sizeof(ShardSplitPubSubMetadata));
    shardSplitPubSubMetadata->shardIntervalListForSubscription = shardIntervalList;
    shardSplitPubSubMetadata->tableOwnerId = tableOwnerId;

    char * tableOwnerName = GetUserNameFromId(tableOwnerId, false);
    ReplicationSlotInfo * replicationSlotInfo = NULL;
    foreach_ptr(replicationSlotInfo, replicationSlotInfoList)
    {
        if(nodeId == replicationSlotInfo->targetNodeId && 
          strcmp(tableOwnerName, replicationSlotInfo->tableOwnerName) == 0)
          {
              shardSplitPubSubMetadata->slotInfo = replicationSlotInfo;
              break;
          }
    }

    return shardSplitPubSubMetadata;
}


void LogicallReplicateSplitShards(WorkerNode *sourceWorkerNode, List* shardSplitPubSubMetadataList)
{
    char *superUser = CitusExtensionOwnerName();
	char *databaseName = get_database_name(MyDatabaseId);
	int connectionFlags = FORCE_NEW_CONNECTION;

    /* Get source node connection */
    MultiConnection *sourceConnection =
        GetNodeUserDatabaseConnection(connectionFlags, sourceWorkerNode->workerName, sourceWorkerNode->workerPort,
        superUser, databaseName);
    
    ClaimConnectionExclusively(sourceConnection);

    List * targetNodeConnectionList = NIL;
    ShardSplitPubSubMetadata * shardSplitPubSubMetadata = NULL;
    foreach_ptr(shardSplitPubSubMetadata, shardSplitPubSubMetadataList)
    {
        uint32 targetWorkerNodeId = shardSplitPubSubMetadata->slotInfo->targetNodeId;
        WorkerNode * targetWorkerNode = FindNodeWithNodeId(targetWorkerNodeId, false);
        
        MultiConnection *targetConnection =
        GetNodeUserDatabaseConnection(connectionFlags, targetWorkerNode->workerName, targetWorkerNode->workerPort,
        superUser, databaseName);
        ClaimConnectionExclusively(targetConnection);

        targetNodeConnectionList = lappend(targetNodeConnectionList, targetConnection);
    }

    /* create publications */
    CreateShardSplitPublications(sourceConnection, shardSplitPubSubMetadataList);

    CreateShardSplitSubscriptions(targetNodeConnectionList,
        shardSplitPubSubMetadataList,
        sourceWorkerNode,
        superUser,
        databaseName);

   WaitForShardSplitRelationSubscriptionsBecomeReady(targetNodeConnectionList, shardSplitPubSubMetadataList);
}


void PrintShardSplitPubSubMetadata(ShardSplitPubSubMetadata * shardSplitMetadata)
{
    printf("sameer: ShardSplitPubSbuMetadata\n");
    ReplicationSlotInfo * replicationInfo = shardSplitMetadata->slotInfo;

    List * shardIntervalList = shardSplitMetadata->shardIntervalListForSubscription;
    printf("shardIds: ");
    ShardInterval * shardInterval = NULL;
    foreach_ptr(shardInterval, shardIntervalList)
    {
        printf("%ld ", shardInterval->shardId);
    }

    printf("\nManual Username from OID at source: %s \n", GetUserNameFromId(shardSplitMetadata->tableOwnerId, false));
    printf("slotname:%s  targetNode:%u tableOwner:%s \n", replicationInfo->slotName, replicationInfo->targetNodeId, replicationInfo->tableOwnerName);
}


static void
CreateShardSplitPublications(MultiConnection *sourceConnection, List *shardSplitPubSubMetadataList)
{
    ShardSplitPubSubMetadata * shardSplitPubSubMetadata = NULL;
    foreach_ptr(shardSplitPubSubMetadata, shardSplitPubSubMetadataList)
    {
        uint32 publicationForNodeId = shardSplitPubSubMetadata->slotInfo->targetNodeId;
        Oid tableOwnerId = shardSplitPubSubMetadata->tableOwnerId;

        CreateShardSplitPublicationForNode(sourceConnection,
                                     shardSplitPubSubMetadata->shardIntervalListForSubscription,
                                     publicationForNodeId,
                                     tableOwnerId);
    }
}

static void
CreateShardSplitSubscriptions(List * targetNodeConnectionList,
    List * shardSplitPubSubMetadataList,
    WorkerNode * sourceWorkerNode,
    char * superUser,
    char * databaseName)
{
    MultiConnection * targetConnection = NULL;
    ShardSplitPubSubMetadata * shardSplitPubSubMetadata = NULL;
    forboth_ptr(targetConnection, targetNodeConnectionList,
                shardSplitPubSubMetadata, shardSplitPubSubMetadataList)
    {
        uint32 publicationForNodeId = shardSplitPubSubMetadata->slotInfo->targetNodeId;
        Oid ownerId = shardSplitPubSubMetadata->tableOwnerId;
        CreateShardSubscription(targetConnection, 
                sourceWorkerNode->workerName, 
                sourceWorkerNode->workerPort, 
                superUser, 
                databaseName, 
                ShardSplitPublicationName(publicationForNodeId, ownerId),
                ownerId);
    }
}


static void
CreateShardSplitPublicationForNode(MultiConnection *connection, List *shardList,
							uint32_t publicationForTargetNodeId, Oid ownerId)
{

    StringInfo createPublicationCommand = makeStringInfo();
    bool prefixWithComma = false;

    appendStringInfo(createPublicationCommand, "CREATE PUBLICATION %s FOR TABLE ",
                        ShardSplitPublicationName(publicationForTargetNodeId, ownerId));

    ShardInterval *shard = NULL;
    foreach_ptr(shard, shardList)
    {
        char *shardName = ConstructQualifiedShardName(shard);

        if (prefixWithComma)
        {
            appendStringInfoString(createPublicationCommand, ",");
        }

        appendStringInfoString(createPublicationCommand, shardName);
        prefixWithComma = true;
    }

    ExecuteCriticalRemoteCommand(connection, createPublicationCommand->data);
    pfree(createPublicationCommand->data);
    pfree(createPublicationCommand);
}


static char *
ShardSplitPublicationName(uint32_t nodeId, Oid ownerId)
{
	return psprintf("%s%u_%u", SHARD_SPLIT_PUBLICATION_PREFIX, nodeId, ownerId);
}


static void
WaitForShardSplitRelationSubscriptionsBecomeReady(List * targetNodeConnectionList, List * shardSplitPubSubMetadataList)
{
    MultiConnection * targetConnection = NULL;
    ShardSplitPubSubMetadata * shardSplitPubSubMetadata = NULL;
    forboth_ptr(targetConnection, targetNodeConnectionList,
        shardSplitPubSubMetadata, shardSplitPubSubMetadataList)
    {
        Bitmapset *tableOwnerIds = NULL;
        tableOwnerIds = bms_add_member(tableOwnerIds, shardSplitPubSubMetadata->tableOwnerId);
        WaitForRelationSubscriptionsBecomeReady(targetConnection, tableOwnerIds);
    }
}