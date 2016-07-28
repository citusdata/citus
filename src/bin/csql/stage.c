/*
 * csql - the Citus interactive terminal
 * stage.c
 *	  Helper routines to execute the csql meta-command \stage. These routines
 *	  communicate with the master and worker nodes; and create new shards and
 *	  upload data into them.
 *
 * Copyright (c) 2012-2016, Citus Data, Inc.
 *
 * $Id$
 */

#include <math.h>
#include <sys/stat.h>

#include "libpq-int.h"
#include "libpq/ip.h"
#include "common.h"
#include "copy.h"
#include "distributed/pg_dist_partition.h"
#include "settings.h"
#include "stage.h"


/* Local functions forward declarations */
static bool FileSize(char *filename, uint64 *fileSize);
static PGconn * ConnectToWorkerNode(const char *nodeName, uint32 nodePort,
									const char *nodeDatabase);
static PGresult * ExecuteRemoteCommand(PGconn *remoteConnection,
									   const char *remoteCommand,
									   const char **parameterValues, int parameterCount);
static TableMetadata * InitTableMetadata(const char *tableName);
static ShardMetadata * InitShardMetadata(int shardPlacementPolicy);
static void FreeTableMetadata(TableMetadata *tableMetadata);
static void FreeShardMetadata(ShardMetadata *shardMetadata);
static void FreeShardMetadataList(ShardMetadata **shardMetadataList);
static void FreeCommonStageData(copy_options *stageOptions, TableMetadata *tableMetadata,
								ShardMetadata **shardMetadataList);
static bool ColumnarTableOptionsOK(Oid relationOid);
static char * ExtendTablename(const char *baseTablename, uint64 shardId);
static uint64 GetValueUint64(const PGresult *result, int rowNumber, int columnNumber);
static bool MasterGetTableMetadata(const char *tableName, TableMetadata *tableMetadata);
static bool MasterGetTableDDLEvents(const char *tableName, TableMetadata *tableMetadata);
static bool MasterGetNewShardId(ShardMetadata *shardMetadata);
static bool MasterGetCandidateNodes(ShardMetadata *shardMetadata,
									int shardPlacementPolicy);
static bool MasterInsertShardRow(uint32 logicalRelid, char storageType,
								 const ShardMetadata *shardMetadata);
static bool MasterInsertPlacementRows(const ShardMetadata *shardMetadata);
static bool MasterInsertShardMetadata(uint32 logicalRelid, char storageType,
									  ShardMetadata **shardMetadataList);
static bool IssueTransactionCommand(PGconn *connection, const char *command);
static bool StageTableData(PGconn *workerNode, uint64 shardId,
						   TableMetadata *tableMetadata, copy_options *stageOptions,
						   uint64 currentFileOffset, uint64 *nextFileOffset);
static bool StageForeignData(PGconn *workerNode, uint64 shardId,
							 TableMetadata *tableMetaData, copy_options *stageOptions);
static bool CreateRegularTable(PGconn *workerNode, int64 shardId,
							   TableMetadata *tableMetadata);
static bool CreateForeignTable(PGconn *workerNode, uint64 shardId,
							   TableMetadata *tableMetaData, const char *tableName,
							   const char *filePath);
static bool ApplyShardDDLCommand(PGconn *workerNode, uint64 shardId, const char *command);
static bool TransmitTableData(PGconn *workerNode, uint64 shardId,
							  uint64 shardMaxSize, copy_options *stageOptions,
							  uint64 currentFileOffset, uint64 *nextFileOffset);
static bool TransmitFile(PGconn *workerNode, const char *localPath,
						 const char *remotePath);
static bool FileStreamOK(const copy_options *stageOptions);
static PQExpBuffer CreateCopyQueryString(const char *tableName, const char *columnList,
										 const char *afterToFrom);
static int64 ShardTableSize(PGconn *workerNode, const char *tablename, uint64 shardId);
static int64 ShardFileSize(PGconn *workerNode, const char *tablename, uint64 shardId);
static int64 ShardColumnarTableSize(PGconn *workerNode, const char *tablename,
									uint64 shardId);
static bool ShardMinMaxValues(PGconn *workerNode, const char *tablename,
							  const char *partitionKey, ShardMetadata *shardMetadata);


/*
 * DoStageData augments psql's copy meta-command with data loading functionality
 * for sharded databases. The function parses given command options, determines
 * the table name, and then retrieves table related metadata from the master.
 *
 * The function later breaks input data into shards of fixed sizes, and uploads
 * these shards in a loop. In this loop, the function asks the master node to
 * create metadata for a new shard and return shard metadata. The function then
 * uses these metadata, contacts worker nodes, and creates shards on them.
 *
 * Once all shards are created and staged (and all input data consumed), the
 * function finalizes shard metadata with the master, and returns true. In case
 * of a failure, the function aborts from finalizing shard metadata, and returns
 * false.
 */
bool
DoStageData(const char *stageCommand)
{
	TableMetadata *tableMetadata = NULL;
	copy_options *copyOptions = NULL;
	copy_options *stageOptions = NULL;
	ShardMetadata *shardMetadataList[MAX_SHARD_UPLOADS];
	uint32 shardCount = 0;
	uint32 shardIndex = 0;
	uint64 fileSize = 0;
	uint64 currentFileOffset = 0;
	uint64 nextFileOffset = 0;
	char tableStorageType = 0;
	char partitionMethod = 0;
	bool metadataOK = false;
	bool fileOK = false;

	/* parse the stage command, and validate its options */
	copyOptions = parse_slash_copy(stageCommand);
	if (copyOptions == NULL)
	{
		return false;
	}
	else if (copyOptions->from == false)
	{
		psql_error("\\stage: staging tablename to filename unsupported\n");
		free_copy_options(copyOptions);

		return false;
	}
	else if (copyOptions->file == NULL)
	{
		/* extending this function to read data from stdin should be easy */
		psql_error("\\stage: staging currently supports file inputs only\n");
		free_copy_options(copyOptions);

		return false;
	}

	/* parse in the additional options needed for staging */
	stageOptions = ParseStageOptions(copyOptions);

	/* get file size */
	fileOK = FileSize(stageOptions->file, &fileSize);
	if (!fileOK)
	{
		free_copy_options(stageOptions);
		return false;
	}

	/* allocate and retrieve table related metadata */
	tableMetadata = InitTableMetadata(stageOptions->tableName);
	if (tableMetadata == NULL)
	{
		free_copy_options(stageOptions);
		return false;
	}

	/* check that options specified by the user are reasonable */
	tableStorageType = tableMetadata->tableStorageType;
	if (tableStorageType == STORAGE_TYPE_FOREIGN)
	{
		if (stageOptions->after_tofrom != NULL)
		{
			psql_error("\\stage: options for foreign tables are not supported\n");
			free_copy_options(stageOptions);
			FreeTableMetadata(tableMetadata);

			return false;
		}
	}

	/* check that we are not staging into a hash partitioned table */
	partitionMethod = tableMetadata->partitionMethod;
	if (partitionMethod == DISTRIBUTE_BY_HASH)
	{
		psql_error("\\stage: staging data into hash partitioned tables is not "
				   "supported\n");
		free_copy_options(stageOptions);
		FreeTableMetadata(tableMetadata);

		return false;
	}

	/* check that the foreign table options are suitable for the \stage command */
	if (tableStorageType == STORAGE_TYPE_COLUMNAR)
	{
		bool tableOptionsOK = ColumnarTableOptionsOK(tableMetadata->logicalRelid);
		if (!tableOptionsOK)
		{
			return false;   /* error message already displayed */
		}
	}

	memset(shardMetadataList, 0, sizeof(shardMetadataList));
	shardCount = (fileSize / tableMetadata->shardMaxSize) + 1;

	/* foreign files may be compressed, and we can't split them yet */
	if (tableStorageType == STORAGE_TYPE_FOREIGN)
	{
		shardCount = 1;
	}

	if (shardCount > MAX_SHARD_UPLOADS)
	{
		psql_error("\\stage: cannot stage more than %u shards\n", MAX_SHARD_UPLOADS);
		free_copy_options(stageOptions);
		FreeTableMetadata(tableMetadata);

		return false;
	}

	/* while more file data left, continue to create and upload shards */
	while (currentFileOffset < fileSize)
	{
		ShardMetadata *shardMetadata = NULL;
		const char *tableName = stageOptions->tableName;
		const char *dbName = PQdb(pset.db);
		char shardIdString[MAXPGPATH];
		uint64 shardId = 0;
		uint32 nodeIndex = 0;
		uint32 stageCount = 0;

		/*
		 * Allocate and retrieve metadata for new shard on the basis of the shard
		 * placement policy.
		 */
		shardMetadata = InitShardMetadata(tableMetadata->shardPlacementPolicy);
		if (shardMetadata == NULL)
		{
			/*
			 * For now, we simply abort staging by not finalizing shard metadata
			 * on the master. This leaves invisible shard data on worker nodes.
			 */
			FreeCommonStageData(stageOptions, tableMetadata, shardMetadataList);

			return false;       /* abort immediately */
		}

		/* save allocated shard metadata */
		shardMetadataList[shardIndex] = shardMetadata;
		shardIndex++;

		shardId = shardMetadata->shardId;
		snprintf(shardIdString, MAXPGPATH, UINT64_FORMAT, shardId);

		/*
		 * We now have table and shard metadata, and can start uploading shard
		 * data to remote nodes. For this, we need to upload replicas to a
		 * predetermined number of remote nodes. If we fail to create and upload
		 * shards on a remote node, or to fetch shard statistics from it, we
		 * retry with the next node in the list. If we aren't able to stage to
		 * enough nodes, we error out.
		 */
		for (nodeIndex = 0; nodeIndex < shardMetadata->nodeCount; nodeIndex++)
		{
			char *remoteNodeName = shardMetadata->nodeNameList[nodeIndex];
			uint32 remoteNodePort = shardMetadata->nodePortList[nodeIndex];

			PGconn *remoteNode = NULL;
			bool shardReplicaCreated = false;
			bool shardStageFailed = false;
			bool minMaxOK = false;
			int64 shardSize = 0;

			remoteNode = ConnectToWorkerNode(remoteNodeName, remoteNodePort, dbName);
			if (remoteNode == NULL)
			{
				shardMetadata->nodeStageList[nodeIndex] = false;
				continue;
			}

			if (tableStorageType == STORAGE_TYPE_TABLE ||
				tableStorageType == STORAGE_TYPE_COLUMNAR)
			{
				shardReplicaCreated = StageTableData(remoteNode, shardId, tableMetadata,
													 stageOptions, currentFileOffset,
													 &nextFileOffset);
			}
			else if (tableStorageType == STORAGE_TYPE_FOREIGN)
			{
				shardReplicaCreated = StageForeignData(remoteNode, shardId,
													   tableMetadata, stageOptions);
				nextFileOffset = fileSize;
			}

			if (!shardReplicaCreated)
			{
				shardStageFailed = true;
			}

			/* if this is the first successful shard replica, fetch stats from it */
			if (!shardStageFailed && stageCount == 0)
			{
				/* fetch shard size */
				if (tableStorageType == STORAGE_TYPE_TABLE)
				{
					shardSize = ShardTableSize(remoteNode, tableName, shardId);
				}
				else if (tableStorageType == STORAGE_TYPE_FOREIGN)
				{
					shardSize = ShardFileSize(remoteNode, tableName, shardId);
				}
				else if (tableStorageType == STORAGE_TYPE_COLUMNAR)
				{
					shardSize = ShardColumnarTableSize(remoteNode, tableName, shardId);
				}

				/* fetch partition key's min/max values in shard */
				minMaxOK = ShardMinMaxValues(remoteNode, tableName,
											 tableMetadata->partitionKey, shardMetadata);

				if (shardSize >= 0 && minMaxOK)
				{
					shardMetadata->shardSize = shardSize;
				}
				else
				{
					shardStageFailed = true;
				}
			}

			if (shardStageFailed)
			{
				shardMetadata->nodeStageList[nodeIndex] = false;
			}
			else
			{
				shardMetadata->nodeStageList[nodeIndex] = true;
				stageCount++;
			}

			PQfinish(remoteNode);

			if (stageCount == tableMetadata->shardReplicaCount)
			{
				break;
			}
		}

		/* check that we staged data to enough nodes */
		if (stageCount < tableMetadata->shardReplicaCount)
		{
			psql_error("\\stage: failed to replicate shard to enough replicas\n");

			FreeCommonStageData(stageOptions, tableMetadata, shardMetadataList);

			return false;
		}

		/* update current file offset */
		currentFileOffset = nextFileOffset;
	}  /* while more file data left for sharding */

	/*
	 * At this stage, we have all file data staged into shards. We now finalize
	 * these shards by uploading their metadata information to the master.
	 */
	metadataOK = MasterInsertShardMetadata(tableMetadata->logicalRelid,
										   tableStorageType, shardMetadataList);

	/* clean up */
	FreeCommonStageData(stageOptions, tableMetadata, shardMetadataList);

	return metadataOK;
}


/* Canonicalize given file name, and determine file's size. */
static bool
FileSize(char *filename, uint64 *fileSize)
{
	struct stat fileStat;

	canonicalize_path(filename);
	if (stat(filename, &fileStat) < 0)
	{
		psql_error("%s: %s\n", filename, strerror(errno));
		return false;
	}

	(*fileSize) = fileStat.st_size;

	return true;
}


/*
 * ConnectToWorkerNode establishes connection to the worker node, and returns
 * the connection. If connection to the node fails, the function returns null.
 */
static PGconn *
ConnectToWorkerNode(const char *nodeName, uint32 nodePort, const char *nodeDatabase)
{
	PGconn *workerNode = NULL;
	const char *nodeOptions = NULL;
	const char *nodeTty = NULL;
	char nodePortString[MAXPGPATH];
	char connInfoString[MAXPGPATH];

	/* transcribe port number and connection info to their string values */
	snprintf(nodePortString, MAXPGPATH, "%u", nodePort);
	snprintf(connInfoString, MAXPGPATH, CONN_INFO_TEMPLATE,
			 nodeDatabase, CLIENT_CONNECT_TIMEOUT);

	workerNode = PQsetdb(nodeName, nodePortString, nodeOptions, nodeTty, connInfoString);

	if (PQstatus(workerNode) != CONNECTION_OK)
	{
		psql_error("worker node connection failed with %s", PQerrorMessage(workerNode));

		PQfinish(workerNode);
		workerNode = NULL;
	}

	return workerNode;
}


/*
 * ExecuteRemoteCommand executes commands on given remote node. The function
 * uses the text protocol both for parameters and results; and on success
 * returns the results object. On failure, the function emits an error message,
 * clears results and returns null.
 */
static PGresult *
ExecuteRemoteCommand(PGconn *remoteConnection, const char *remoteCommand,
					 const char **parameterValues, int parameterCount)
{
	PGresult *result = NULL;

	const Oid *parameterType = NULL;   /* let the backend deduce type */
	const int *parameterLength = NULL; /* text params do not need length */
	const int *parameterFormat = NULL; /* text params have Null by default */
	const int resultFormat = 0;        /* ask for results in text format */

	result = PQexecParams(remoteConnection, remoteCommand,
						  parameterCount, parameterType, parameterValues,
						  parameterLength, parameterFormat, resultFormat);

	if (PQresultStatus(result) != PGRES_COMMAND_OK &&
		PQresultStatus(result) != PGRES_TUPLES_OK)
	{
		psql_error("remote command \"%s\" failed with %s",
				   remoteCommand, PQerrorMessage(remoteConnection));
		PQclear(result);

		return NULL;
	}

	return result;
}


/*
 * InitTableMetadata allocates memory for table related metadata, and then
 * executes remote calls on the master to initialize these metadata. On success,
 * the function returns the fully initialized table metadata; on failure, it
 * returns null.
 */
static TableMetadata *
InitTableMetadata(const char *tableName)
{
	TableMetadata *tableMetadata = NULL;
	bool commandOK = true;

	tableMetadata = (TableMetadata *) pg_malloc0(sizeof(TableMetadata));

	commandOK = MasterGetTableMetadata(tableName, tableMetadata);
	if (!commandOK)
	{
		FreeTableMetadata(tableMetadata);
		return NULL;
	}

	commandOK = MasterGetTableDDLEvents(tableName, tableMetadata);
	if (!commandOK)
	{
		FreeTableMetadata(tableMetadata);
		return NULL;
	}

	return tableMetadata;
}


/* Frees memory allocated to table metadata structure. */
static void
FreeTableMetadata(TableMetadata *tableMetadata)
{
	if (tableMetadata->ddlEventList != NULL)
	{
		uint32 eventIndex = 0;
		uint32 eventCount = tableMetadata->ddlEventCount;

		for (eventIndex = 0; eventIndex < eventCount; eventIndex++)
		{
			char *ddlEvent = tableMetadata->ddlEventList[eventIndex];

			free(ddlEvent);
			ddlEvent = NULL;
		}
	}

	free(tableMetadata->ddlEventList);
	free(tableMetadata->partitionKey);

	free(tableMetadata);
}


/*
 * InitShardMetadata allocates memory for shard metadata, and then executes
 * remote calls on the master to initialize these metadata. On success, the
 * function returns the fully initialized shard metadata; on failure, it returns
 * null.
 */
static ShardMetadata *
InitShardMetadata(int shardPlacementPolicy)
{
	ShardMetadata *shardMetadata = NULL;
	uint32 nodeCount = 0;
	bool commandOK = true;

	shardMetadata = (ShardMetadata *) pg_malloc0(sizeof(ShardMetadata));

	commandOK = MasterGetNewShardId(shardMetadata);
	if (!commandOK)
	{
		FreeShardMetadata(shardMetadata);
		return NULL;
	}

	commandOK = MasterGetCandidateNodes(shardMetadata, shardPlacementPolicy);
	if (!commandOK)
	{
		FreeShardMetadata(shardMetadata);
		return NULL;
	}

	nodeCount = shardMetadata->nodeCount;
	shardMetadata->nodeStageList = (bool *) pg_malloc0(nodeCount * sizeof(bool));
	shardMetadata->shardMinValue = NULL;
	shardMetadata->shardMaxValue = NULL;
	shardMetadata->shardSize = 0;

	return shardMetadata;
}


/* Frees memory allocated to shard metadata structure. */
static void
FreeShardMetadata(ShardMetadata *shardMetadata)
{
	if (shardMetadata->nodeNameList != NULL)
	{
		uint32 nodeIndex = 0;
		uint32 nodeCount = shardMetadata->nodeCount;

		for (nodeIndex = 0; nodeIndex < nodeCount; nodeIndex++)
		{
			char *nodeName = shardMetadata->nodeNameList[nodeIndex];

			free(nodeName);
			nodeName = NULL;
		}
	}

	free(shardMetadata->nodeNameList);
	free(shardMetadata->nodePortList);

	free(shardMetadata->nodeStageList);
	free(shardMetadata->shardMinValue);
	free(shardMetadata->shardMaxValue);

	free(shardMetadata);
}


/* Frees memory allocated to all shard metadata structures in given list. */
static void
FreeShardMetadataList(ShardMetadata **shardMetadataList)
{
	uint32 listIndex = 0;
	for (listIndex = 0; listIndex < MAX_SHARD_UPLOADS; listIndex++)
	{
		ShardMetadata *shardMetadata = shardMetadataList[listIndex];
		if (shardMetadata != NULL)
		{
			FreeShardMetadata(shardMetadata);
			shardMetadata = NULL;
		}
	}
}


/* Frees memory common to all staging operations. */
static void
FreeCommonStageData(copy_options *stageOptions, TableMetadata *tableMetadata,
					ShardMetadata **shardMetadataList)
{
	free_copy_options(stageOptions);
	FreeTableMetadata(tableMetadata);
	FreeShardMetadataList(shardMetadataList);
}


/*
 * ColumnarTableOptionsOK checks if the foreign table options for the distributed
 * cstore_fdw table are suitable for staging or not. It returns true if they are
 * and false if they are not.
 */
static bool
ColumnarTableOptionsOK(Oid relationOid)
{
	PGconn *masterNode = pset.db;
	PGresult *queryResult = NULL;
	ExecStatusType queryStatus = 0;
	bool optionsOK = true;

	PQExpBuffer queryString = createPQExpBuffer();
	appendPQExpBuffer(queryString, GET_COLUMNAR_TABLE_FILENAME_OPTION, relationOid);

	/* get the filename option for the given cstore_fdw table */
	queryResult = PQexec(masterNode, queryString->data);

	queryStatus = PQresultStatus(queryResult);
	if (queryStatus == PGRES_TUPLES_OK)
	{
		int tupleCount = PQntuples(queryResult);
		Assert(tupleCount <= 1);
		if (tupleCount == 1)
		{
			/*
			 * Since the result has one row, it implies that the filename
			 * option was specified, which is not allowed for distributed
			 * cstore_fdw tables. So, we error out.
			 */
			psql_error("\\stage: filename option is not allowed for distributed "
					   "columnar tables\n");
			optionsOK = false;
		}
	}
	else
	{
		psql_error("\\stage: %s", PQerrorMessage(masterNode));
		optionsOK = false;
	}

	PQclear(queryResult);
	destroyPQExpBuffer(queryString);

	return optionsOK;
}


/*
 * ExtendTablename appends shardId to given tablename, and returns extended name
 * in a dynamically allocated string.
 */
static char *
ExtendTablename(const char *baseTablename, uint64 shardId)
{
	char *extendedTablename = (char *) pg_malloc0(NAMEDATALEN);

	snprintf(extendedTablename, NAMEDATALEN, "%s%c" UINT64_FORMAT,
			 baseTablename, SHARD_NAME_SEPARATOR, shardId);

	return extendedTablename;
}


/* Helper method that converts result value string to 64-bit unsigned integer. */
static uint64
GetValueUint64(const PGresult *result, int rowNumber, int columnNumber)
{
	char *valueString = NULL;
	char *valueStringEnd = NULL;
	uint64 value = 0;

	valueString = PQgetvalue(result, rowNumber, columnNumber);
	if (valueString == NULL || (*valueString) == '\0')
	{
		return INVALID_UINT64;
	}

	errno = 0;
	value = strtoull(valueString, &valueStringEnd, 0);

	if (errno != 0 || (*valueStringEnd) != '\0')
	{
		return INVALID_UINT64;
	}

	/* Server returned values for staging should never equal to "0". */
	Assert(value != INVALID_UINT64);

	return value;
}


/*
 * MasterGetTableMetadata fetches from the master metadata related to a
 * particular table. The function then parses relevant fields, assigns them to
 * master metadata, and on success returns true. On failure, the function
 * returns false.
 */
static bool
MasterGetTableMetadata(const char *tableName, TableMetadata *tableMetadata)
{
	const char *remoteCommand = MASTER_GET_TABLE_METADATA;
	const char *parameterValue[1] = { tableName };
	const int parameterCount = 1;
	int logicalRelidIndex = 0;
	int partStorageTypeIndex = 0;
	int partMethodIndex = 0;
	int partKeyIndex = 0;
	int partReplicaCountIndex = 0;
	int partMaxSizeIndex = 0;
	int partPlacementPolicyIndex = 0;

	PGconn *masterNode = pset.db;
	PGresult *result = NULL;
	char *tableStorageType = NULL;
	char *partitionMethod = NULL;
	char *partitionKey = NULL;
	int partitionKeyLength = 0;
	uint64 logicalRelid = 0;
	uint64 shardReplicaCount = 0;
	uint64 shardMaxSize = 0;
	uint64 shardPlacementPolicy = 0;

	/* fetch table metadata for partitioning */
	result = ExecuteRemoteCommand(masterNode, remoteCommand,
								  parameterValue, parameterCount);
	if (result == NULL)
	{
		return false;           /* error message already displayed */
	}

	/* find column numbers associated with column names */
	logicalRelidIndex = PQfnumber(result, LOGICAL_RELID_FIELD);
	partStorageTypeIndex = PQfnumber(result, PART_STORAGE_TYPE_FIELD);
	partMethodIndex = PQfnumber(result, PART_METHOD_FIELD);
	partKeyIndex = PQfnumber(result, PART_KEY_FIELD);
	partReplicaCountIndex = PQfnumber(result, PART_REPLICA_COUNT_FIELD);
	partMaxSizeIndex = PQfnumber(result, PART_MAX_SIZE_FIELD);
	partPlacementPolicyIndex = PQfnumber(result, PART_PLACEMENT_POLICY_FIELD);

	/* fetch variable length response */
	partitionKey = PQgetvalue(result, 0, partKeyIndex);
	partitionKeyLength = PQgetlength(result, 0, partKeyIndex);

	/* fetch fixed length responses and convert those that are integers */
	tableStorageType = PQgetvalue(result, 0, partStorageTypeIndex);
	partitionMethod = PQgetvalue(result, 0, partMethodIndex);
	logicalRelid = GetValueUint64(result, 0, logicalRelidIndex);
	shardReplicaCount = GetValueUint64(result, 0, partReplicaCountIndex);
	shardMaxSize = GetValueUint64(result, 0, partMaxSizeIndex);
	shardPlacementPolicy = GetValueUint64(result, 0, partPlacementPolicyIndex);

	if (partitionKeyLength <= 0 || logicalRelid == INVALID_UINT64 ||
		shardReplicaCount == INVALID_UINT64 || shardMaxSize == INVALID_UINT64 ||
		shardPlacementPolicy == INVALID_UINT64)
	{
		psql_error("remote command \"%s:%s\" failed with invalid table metadata\n",
				   remoteCommand, tableName);
		PQclear(result);

		return false;
	}

	/* set metadata related to the table */
	tableMetadata->partitionKey = (char *) pg_malloc0(partitionKeyLength + 1);
	strncpy(tableMetadata->partitionKey, partitionKey, partitionKeyLength + 1);

	tableMetadata->tableStorageType = tableStorageType[0];
	tableMetadata->partitionMethod = partitionMethod[0];
	tableMetadata->logicalRelid = (uint32) logicalRelid;
	tableMetadata->shardReplicaCount = (uint32) shardReplicaCount;
	tableMetadata->shardMaxSize = (uint64) shardMaxSize;
	tableMetadata->shardPlacementPolicy = (uint32) shardPlacementPolicy;

	PQclear(result);

	return true;
}


/*
 * MasterGetTableDDLEvents fetches from the master a list of DDL events that
 * relate to a particular table. The function then deep copies these DDL events
 * to metadata, and on success returns true. On failure, the function returns
 * false.
 */
static bool
MasterGetTableDDLEvents(const char *tableName, TableMetadata *tableMetadata)
{
	const char *remoteCommand = MASTER_GET_TABLE_DDL_EVENTS;
	const char *parameterValue[1] = { tableName };
	const int parameterCount = 1;

	PGconn *masterNode = pset.db;
	PGresult *result = NULL;
	int ddlEventCount = 0;
	int ddlEventIndex = 0;

	/* fetch DDL events needed for table creation */
	result = ExecuteRemoteCommand(masterNode, remoteCommand,
								  parameterValue, parameterCount);
	if (result == NULL)
	{
		return false;
	}

	/* check that we have at least one DDL event */
	ddlEventCount = PQntuples(result);
	if (ddlEventCount <= 0)
	{
		psql_error("remote command \"%s:%s\" failed to fetch DDL events\n",
				   remoteCommand, tableName);
		PQclear(result);

		return false;
	}

	/* allocate memory for DDL event list in metadata */
	tableMetadata->ddlEventList = (char **) pg_malloc0(ddlEventCount * sizeof(char *));
	tableMetadata->ddlEventCount = ddlEventCount;

	/* walk over fetched DDL event list, and assign them to metadata */
	for (ddlEventIndex = 0; ddlEventIndex < ddlEventCount; ddlEventIndex++)
	{
		char *ddlEvent = NULL;
		char *ddlEventValue = PQgetvalue(result, ddlEventIndex, 0);
		int ddlEventLength = PQgetlength(result, ddlEventIndex, 0);

		if (ddlEventLength <= 0)
		{
			psql_error("remote command \"%s:%s\" fetched empty DDL event\n",
					   remoteCommand, tableName);
			PQclear(result);

			return false;
		}

		/* deep copy DDL event and assign to metadata */
		ddlEvent = (char *) pg_malloc0(ddlEventLength + 1);
		strncpy(ddlEvent, ddlEventValue, ddlEventLength + 1);

		tableMetadata->ddlEventList[ddlEventIndex] = ddlEvent;
	}

	PQclear(result);

	return true;
}


/*
 * MasterGetNewShardId fetches from the master a global shardId for the new
 * shard to be created. The function then sets the shardId field in metadata,
 * and on success returns true. On failure, the function returns false.
 */
static bool
MasterGetNewShardId(ShardMetadata *shardMetadata)
{
	const char *remoteCommand = MASTER_GET_NEW_SHARDID;
	const char **parameterValue = NULL;
	const int parameterCount = 0;

	PGconn *masterNode = pset.db;
	PGresult *result = NULL;
	uint64 shardId = 0;

	/* fetch unique shardId for shard to be created */
	result = ExecuteRemoteCommand(masterNode, remoteCommand,
								  parameterValue, parameterCount);
	if (result == NULL)
	{
		return false;
	}

	/* get shard value string and convert it to 64-bit integer */
	shardId = GetValueUint64(result, 0, 0);
	if (shardId == INVALID_UINT64)
	{
		psql_error("remote command \"%s\" failed with invalid shardId\n",
				   remoteCommand);
		PQclear(result);

		return false;
	}

	/* set metadata shardId; clear results */
	shardMetadata->shardId = shardId;
	PQclear(result);

	return true;
}


/*
 * MasterGetCandidateNodes fetches from the master a list of worker node names
 * and port numbers for staging (uploading) shard data on the basis of the
 * shard placement policy passed to it. The function then parses and deep copies
 * these node names and port numbers to metadata, and on success returns true.
 * On failure, the function returns false.
 */
static bool
MasterGetCandidateNodes(ShardMetadata *shardMetadata, int shardPlacementPolicy)
{
	const char *remoteCommand = NULL;
	const char **parameterValue = NULL;
	int parameterCount = 0;
	char shardIdString[NAMEDATALEN];
	int nodeNameIndex = 0;
	int nodePortIndex = 0;

	PGconn *masterNode = pset.db;
	PGresult *result = NULL;
	int nodeCount = 0;
	int nodeIndex = 0;

	/*
	 * We choose the remote command for fetching node names and node ports, and the
	 * parameters to be passed to it on the basis of the shard placement policy.
	 */
	Assert(shardPlacementPolicy == SHARD_PLACEMENT_LOCAL_NODE_FIRST ||
		   shardPlacementPolicy == SHARD_PLACEMENT_ROUND_ROBIN);
	if (shardPlacementPolicy == SHARD_PLACEMENT_LOCAL_NODE_FIRST)
	{
		remoteCommand = MASTER_GET_LOCAL_FIRST_CANDIDATE_NODES;
		parameterCount = 0;

		/*
		 * The master uses its connection's remote socket address to determine
		 * the client's hostname. The master then uses this hostname to allocate
		 * the first candidate node in the local node first policy. For all of
		 * this to happen, we should have connected to the master over TCP/IP.
		 */
		if (masterNode->laddr.addr.ss_family != AF_INET &&
			masterNode->laddr.addr.ss_family != AF_INET6)
		{
			psql_error("remote command \"%s\" needs TCP/IP connection to master node\n",
					   remoteCommand);

			return false;
		}
	}
	else if (shardPlacementPolicy == SHARD_PLACEMENT_ROUND_ROBIN)
	{
		remoteCommand = MASTER_GET_ROUND_ROBIN_CANDIDATE_NODES;
		parameterCount = 1;

		/* convert parameter to its string representation */
		snprintf(shardIdString, NAMEDATALEN, UINT64_FORMAT, shardMetadata->shardId);

		parameterValue = (const char **) pg_malloc0(parameterCount * sizeof(char *));
		parameterValue[0] = shardIdString;
	}

	/* fetch worker node name/port list for uploading shard data */
	result = ExecuteRemoteCommand(masterNode, remoteCommand,
								  parameterValue, parameterCount);

	if (parameterValue != NULL)
	{
		free(parameterValue);
	}

	if (result == NULL)
	{
		return false;
	}

	/* find column numbers associated with column names */
	nodeNameIndex = PQfnumber(result, NODE_NAME_FIELD);
	nodePortIndex = PQfnumber(result, NODE_PORT_FIELD);
	if (nodeNameIndex < 0 || nodePortIndex < 0)
	{
		psql_error("remote command \"%s\" failed with invalid response\n",
				   remoteCommand);
		PQclear(result);

		return false;
	}

	nodeCount = PQntuples(result);
	if (nodeCount <= 0)
	{
		psql_error("remote command \"%s\" failed to fetch worker nodes\n",
				   remoteCommand);
		PQclear(result);

		return false;
	}

	/* allocate memory for node name/port list in metadata */
	shardMetadata->nodeNameList = (char **) pg_malloc0(nodeCount * sizeof(char *));
	shardMetadata->nodePortList = (uint32 *) pg_malloc0(nodeCount * sizeof(uint32));
	shardMetadata->nodeCount = nodeCount;

	/* walk over fetched node name/port list, and assign them to metadata */
	for (nodeIndex = 0; nodeIndex < nodeCount; nodeIndex++)
	{
		char *nodeName = NULL;
		uint64 nodePort = 0;

		char *nodeNameValue = PQgetvalue(result, nodeIndex, nodeNameIndex);
		int nodeNameLength = PQgetlength(result, nodeIndex, nodeNameIndex);

		if (nodeNameLength <= 0)
		{
			psql_error("remote command \"%s\" fetched empty node name\n",
					   remoteCommand);
			PQclear(result);

			return false;
		}

		/* deep copy node name and assign to metadata */
		nodeName = (char *) pg_malloc0(nodeNameLength + 1);
		strncpy(nodeName, nodeNameValue, nodeNameLength + 1);

		shardMetadata->nodeNameList[nodeIndex] = nodeName;

		/* convert port value string to 64-bit integer, and assign to metadata */
		nodePort = GetValueUint64(result, nodeIndex, nodePortIndex);
		if (nodePort == INVALID_UINT64)
		{
			psql_error("remote command \"%s\" failed to fetch valid port number\n",
					   remoteCommand);
			PQclear(result);

			return false;
		}

		shardMetadata->nodePortList[nodeIndex] = (uint32) nodePort;
	}

	PQclear(result);

	return true;
}


/* Executes command on the master to insert shard tuple to pg_dist_shard. */
static bool
MasterInsertShardRow(uint32 logicalRelid, char storageType,
					 const ShardMetadata *shardMetadata)
{
	const char *remoteCommand = MASTER_INSERT_SHARD_ROW;
	const char *parameterValue[5];
	const int parameterCount = 5;
	char logicalRelidString[NAMEDATALEN];
	char shardIdString[NAMEDATALEN];
	char storageTypeString[NAMEDATALEN];

	PGconn *masterNode = pset.db;
	PGresult *result = NULL;

	/* convert parameters to their string representations */
	snprintf(logicalRelidString, NAMEDATALEN, "%u", logicalRelid);
	snprintf(shardIdString, NAMEDATALEN, UINT64_FORMAT, shardMetadata->shardId);
	snprintf(storageTypeString, NAMEDATALEN, "%c", storageType);

	parameterValue[0] = logicalRelidString;
	parameterValue[1] = shardIdString;
	parameterValue[2] = storageTypeString;
	parameterValue[3] = shardMetadata->shardMinValue;
	parameterValue[4] = shardMetadata->shardMaxValue;

	/* insert shard metadata to master's system catalogs */
	result = ExecuteRemoteCommand(masterNode, remoteCommand,
								  parameterValue, parameterCount);
	if (result == NULL)
	{
		return false;
	}

	PQclear(result);
	return true;
}


/*
 * MasterInsertPlacementRows executes commands on the master node to insert
 * shard placement tuples to pg_dist_shard_placement.
 */
static bool
MasterInsertPlacementRows(const ShardMetadata *shardMetadata)
{
	const char *remoteCommand = MASTER_INSERT_PLACEMENT_ROW;
	const char *parameterValue[5];
	const int parameterCount = 5;
	char shardIdString[NAMEDATALEN];
	char shardLengthString[NAMEDATALEN];
	char nodePortString[NAMEDATALEN];

	PGconn *masterNode = pset.db;
	PGresult *result = NULL;
	uint32 nodeIndex = 0;

	/* convert parameters to their string representations */
	snprintf(shardIdString, NAMEDATALEN, UINT64_FORMAT, shardMetadata->shardId);
	snprintf(shardLengthString, NAMEDATALEN, UINT64_FORMAT, shardMetadata->shardSize);

	parameterValue[0] = shardIdString;
	parameterValue[1] = FILE_FINALIZED;
	parameterValue[2] = shardLengthString;

	for (nodeIndex = 0; nodeIndex < shardMetadata->nodeCount; nodeIndex++)
	{
		bool staged = shardMetadata->nodeStageList[nodeIndex];
		if (staged)
		{
			char *nodeName = shardMetadata->nodeNameList[nodeIndex];
			uint32 nodePort = shardMetadata->nodePortList[nodeIndex];

			/* convert parameter to its string representation */
			snprintf(nodePortString, NAMEDATALEN, "%u", nodePort);

			parameterValue[3] = nodeName;
			parameterValue[4] = nodePortString;

			result = ExecuteRemoteCommand(masterNode, remoteCommand,
										  parameterValue, parameterCount);
			if (result == NULL)
			{
				return false;
			}

			PQclear(result);
		}
	}

	return true;
}


/*
 * MasterInsertShardMetadata finalizes with the master metadata for all shards
 * staged to worker nodes. The function executes shard metadata insert commands
 * within a single transaction so that either all or none of the metadata are
 * finalized. On success, the function commits the transaction and returns true.
 * On failure, the function rolls back the transaction and returns false.
 */
static bool
MasterInsertShardMetadata(uint32 logicalRelid, char storageType,
						  ShardMetadata **shardMetadataList)
{
	PGconn *masterNode = pset.db;
	uint32 listIndex = 0;
	bool issued = true;
	bool metadataOK = true;

	issued = IssueTransactionCommand(masterNode, BEGIN_COMMAND);
	if (!issued)
	{
		return false;
	}

	for (listIndex = 0; listIndex < MAX_SHARD_UPLOADS; listIndex++)
	{
		ShardMetadata *shardMetadata = shardMetadataList[listIndex];
		if (shardMetadata != NULL)
		{
			bool shardRowOK = MasterInsertShardRow(logicalRelid, storageType,
												   shardMetadata);
			bool workerRowOK = MasterInsertPlacementRows(shardMetadata);

			if (!shardRowOK || !workerRowOK)
			{
				IssueTransactionCommand(masterNode, ROLLBACK_COMMAND);

				metadataOK = false;
				break;
			}
		}
	}

	if (metadataOK)
	{
		issued = IssueTransactionCommand(masterNode, COMMIT_COMMAND);
		if (!issued)
		{
			return false;
		}
	}

	return metadataOK;
}


/* Issues given transaction command on the remote node. */
static bool
IssueTransactionCommand(PGconn *connection, const char *command)
{
	PGresult *result = PQexec(connection, command);
	ExecStatusType resultStatus = PQresultStatus(result);

	if (resultStatus != PGRES_COMMAND_OK)
	{
		psql_error("%s", PQerrorMessage(connection));
		PQclear(result);

		return false;
	}

	PQclear(result);
	return true;
}


/*
 * StageTableData creates the table on the remote node, and then uses the
 * copy protocol to upload data to the table. On success, the function updates
 * given file offset, commits the transaction, and returns true. On failure, the
 * function rolls back the transaction and returns false.
 */
static bool
StageTableData(PGconn *workerNode, uint64 shardId,
			   TableMetadata *tableMetadata, copy_options *stageOptions,
			   uint64 currentFileOffset, uint64 *nextFileOffset)
{
	bool issued = false;
	bool createOK = false;
	bool transmitOK = false;

	/* start explicit transaction; this also skips WAL-logging */
	issued = IssueTransactionCommand(workerNode, BEGIN_COMMAND);
	if (!issued)
	{
		return false;
	}

	createOK = CreateRegularTable(workerNode, shardId, tableMetadata);
	if (!createOK)
	{
		IssueTransactionCommand(workerNode, ROLLBACK_COMMAND);
		return false;
	}

	transmitOK = TransmitTableData(workerNode, shardId, tableMetadata->shardMaxSize,
								   stageOptions, currentFileOffset, nextFileOffset);
	if (!transmitOK)
	{
		IssueTransactionCommand(workerNode, ROLLBACK_COMMAND);
		return false;
	}

	issued = IssueTransactionCommand(workerNode, COMMIT_COMMAND);
	if (!issued)
	{
		return false;
	}

	return true;
}


/*
 * StageForeignData determines the remote file path to upload the given file,
 * creates the foreign table on the remote node, and uploads the file to the
 * remote node at the determined path. Then, it commits the transaction and
 * returns true. On failure, the function rolls back the transaction and
 * returns false.
 */
static bool
StageForeignData(PGconn *workerNode, uint64 shardId, TableMetadata *tableMetadata,
				 copy_options *stageOptions)
{
	bool issued = false;
	bool transmitOK = false;
	bool createOK = false;

	char remoteFilePath[MAXPGPATH];
	char *extendedTableName = ExtendTablename(stageOptions->tableName, shardId);
	snprintf(remoteFilePath, MAXPGPATH, "%s/%s", FOREIGN_CACHED_DIR, extendedTableName);
	free(extendedTableName);

	/* start explicit transaction */
	issued = IssueTransactionCommand(workerNode, BEGIN_COMMAND);
	if (!issued)
	{
		return false;
	}

	createOK = CreateForeignTable(workerNode, shardId, tableMetadata,
								  stageOptions->tableName, remoteFilePath);
	if (!createOK)
	{
		IssueTransactionCommand(workerNode, ROLLBACK_COMMAND);
		return false;
	}

	transmitOK = TransmitFile(workerNode, stageOptions->file, remoteFilePath);
	if (!transmitOK)
	{
		IssueTransactionCommand(workerNode, ROLLBACK_COMMAND);
		return false;
	}

	issued = IssueTransactionCommand(workerNode, COMMIT_COMMAND);
	if (!issued)
	{
		return false;
	}

	return true;
}


/*
 * CreateRegularTable executes DDL commands to create the table on the worker
 * node. On success the function returns true, otherwise it returns false.
 */
static bool
CreateRegularTable(PGconn *workerNode, int64 shardId, TableMetadata *tableMetadata)
{
	bool createOK = true;
	const uint32 ddlEventCount = tableMetadata->ddlEventCount;
	uint32 ddlEventIndex = 0;

	/* execute DDL statements on remote node to create table and indexes */
	for (ddlEventIndex = 0; ddlEventIndex < ddlEventCount; ddlEventIndex++)
	{
		char *ddlEvent = tableMetadata->ddlEventList[ddlEventIndex];

		bool ddlApplied = ApplyShardDDLCommand(workerNode, shardId, ddlEvent);
		if (!ddlApplied)
		{
			createOK = false;
			break;
		}
	}

	return createOK;
}


/*
 * CreateForeignTable executes DDL commands to create the foreign table on the
 * worker node, and then sets the foreign table's filename to the given file
 * path. On success the function returns true, otherwise it returns false.
 */
static bool
CreateForeignTable(PGconn *workerNode, uint64 shardId, TableMetadata *tableMetaData,
				   const char *tableName, const char *filePath)
{
	bool createOK = false;
	bool alterQueryOK = true;
	PQExpBuffer alterQueryString = NULL;
	uint32 ddlEventIndex = 0;
	uint32 ddlEventCount = tableMetaData->ddlEventCount;

	/* replay DDL commands to create foreign table */
	for (ddlEventIndex = 0; ddlEventIndex < ddlEventCount; ddlEventIndex++)
	{
		char *ddlEvent = tableMetaData->ddlEventList[ddlEventIndex];

		bool ddlApplied = ApplyShardDDLCommand(workerNode, shardId, ddlEvent);
		if (!ddlApplied)
		{
			return false;
		}
	}

	/* then issue DDL command to set foreign table's file path */
	alterQueryString = createPQExpBuffer();
	appendPQExpBuffer(alterQueryString, SET_FOREIGN_TABLE_FILENAME,
					  tableName, filePath);

	alterQueryOK = ApplyShardDDLCommand(workerNode, shardId, alterQueryString->data);
	if (alterQueryOK)
	{
		createOK = true;
	}

	destroyPQExpBuffer(alterQueryString);

	return createOK;
}


/*
 * ApplyShardDDLCommand calls remote function on the worker node to extend the
 * given ddl command with the shardId, and apply this extended command on the
 * worker database. The function then returns its success status.
 */
static bool
ApplyShardDDLCommand(PGconn *workerNode, uint64 shardId, const char *ddlCommand)
{
	const char *remoteCommand = APPLY_SHARD_DDL_COMMAND;
	const char *parameterValue[2];
	const int parameterCount = 2;
	PGresult *ddlResult = NULL;

	char shardIdString[NAMEDATALEN];
	snprintf(shardIdString, NAMEDATALEN, UINT64_FORMAT, shardId);

	parameterValue[0] = shardIdString;
	parameterValue[1] = ddlCommand;

	ddlResult = ExecuteRemoteCommand(workerNode, remoteCommand,
									 parameterValue, parameterCount);
	if (ddlResult == NULL)
	{
		return false;
	}

	PQclear(ddlResult);
	return true;
}


/*
 * TransmitTableData uploads data from the local file to the remote table using
 * the copy protocol. On success, the function updates given file offset and
 * returns true. On failure, it returns false.
 */
static bool
TransmitTableData(PGconn *workerNode, uint64 shardId,
				  uint64 shardMaxSize, copy_options *stageOptions,
				  uint64 currentFileOffset, uint64 *nextFileOffset)
{
	bool transmitOK = true;
	bool fileOK = true;
	bool closeFileOK = true;
	int seeked = 0;

	PQExpBuffer queryString = NULL;
	PGresult *copyResult = NULL;
	ExecStatusType copyStatus = 0;
	bool copyIsBinary = false;
	FILE *stageStream = NULL;
	char *extendedTablename = NULL;

	/* if file doesn't exists, return immediately */
	fileOK = FileStreamOK((const copy_options *) stageOptions);
	if (!fileOK)
	{
		return false;
	}

	/* open a file stream to stage data from, and seek to given file offset */
	stageStream = OpenCopyStream(stageOptions);
	if (stageStream == NULL)
	{
		return false;
	}

	seeked = fseeko(stageStream, (off_t) (currentFileOffset), SEEK_SET);
	if (seeked < 0)
	{
		psql_error("%s: %s\n", stageOptions->file, strerror(errno));
		return false;
	}

	/* extend table name with shardId for the query */
	extendedTablename = ExtendTablename(stageOptions->tableName, shardId);

	/*
	 * Now start staging data to the worker table. For this, we first form the
	 * query string, and then execute the query over the copy protocol.
	 */
	queryString = CreateCopyQueryString(extendedTablename, stageOptions->columnList,
										stageOptions->after_tofrom);

	copyResult = PQexec(workerNode, queryString->data);

	copyStatus = PQresultStatus(copyResult);
	copyIsBinary = (bool) PQbinaryTuples(copyResult);

	transmitOK = HandleCopyData(workerNode, copyStatus, copyIsBinary,
								stageStream, shardMaxSize);

	/* clean up after remote query */
	PQclear(copyResult);
	destroyPQExpBuffer(queryString);
	free(extendedTablename);

	/* determine new offset in file */
	if (transmitOK && stageOptions->file != NULL)
	{
		int64 nextStreamOffset = ftello(stageStream);
		if (nextStreamOffset < 0)
		{
			psql_error("%s: %s\n", stageOptions->file, strerror(errno));
			transmitOK = false;
		}
		else
		{
			Assert(nextStreamOffset >= currentFileOffset);
			(*nextFileOffset) = nextStreamOffset;
		}
	}

	closeFileOK = CloseCopyStream(stageOptions, stageStream);
	if (!closeFileOK)
	{
		transmitOK = false;
	}

	return transmitOK;
}


/*
 * TransmitFile uploads file on the local path to the given remote file path on
 * worker node. The function uses the copy protocol to upload data, and then
 * returns its success status.
 */
static bool
TransmitFile(PGconn *workerNode, const char *localPath, const char *remotePath)
{
	const uint64 copySizeUnlimited = 0;
	FILE *transmitStream = NULL;
	PQExpBuffer queryString = NULL;
	PGresult *copyResult = NULL;
	ExecStatusType copyStatus = 0;
	bool copyIsBinary = false;
	bool transmitOK = false;
	int closeStatus = 0;

	transmitStream = fopen(localPath, PG_BINARY_R);
	if (transmitStream == NULL)
	{
		psql_error("%s: %s\n", localPath, strerror(errno));

		return false;
	}

	queryString = createPQExpBuffer();
	appendPQExpBuffer(queryString, TRANSMIT_REGULAR_COMMAND, remotePath);

	/* execute remote query to start uploading data over copy protocol */
	copyResult = PQexec(workerNode, queryString->data);
	copyStatus = PQresultStatus(copyResult);
	copyIsBinary = (bool) PQbinaryTuples(copyResult);

	PQclear(copyResult);
	destroyPQExpBuffer(queryString);

	transmitOK = HandleCopyData(workerNode, copyStatus, copyIsBinary,
								transmitStream, copySizeUnlimited);
	if (!transmitOK)
	{
		return false;
	}

	closeStatus = fclose(transmitStream);
	if (closeStatus != 0)
	{
		psql_error("%s: %s\n", localPath, strerror(errno));

		return false;
	}

	return true;
}


/* Checks if a stream can be opened from the given stage options. */
static bool
FileStreamOK(const copy_options *stageOptions)
{
	FILE *fileStream = OpenCopyStream(stageOptions);
	if (fileStream == NULL)
	{
		return false;
	}

	CloseCopyStream(stageOptions, fileStream);

	return true;
}


/* Creates query string for copy command from the given staging options. */
static PQExpBuffer
CreateCopyQueryString(const char *tableName, const char *columnList,
					  const char *afterToFrom)
{
	PQExpBuffer queryString = createPQExpBuffer();

	printfPQExpBuffer(queryString, "COPY ");
	appendPQExpBuffer(queryString, "%s ", tableName);

	if (columnList != NULL)
	{
		appendPQExpBuffer(queryString, "%s ", columnList);
	}

	appendPQExpBuffer(queryString, "FROM STDIN ");

	if (afterToFrom != NULL)
	{
		appendPQExpBufferStr(queryString, afterToFrom);
	}

	return queryString;
}


/*
 * ShardTableSize executes a command on the worker node to determine the size of
 * the table representing the shard. On success, the function returns the table
 * size on disk. On failure, the function returns -1.
 */
static int64
ShardTableSize(PGconn *workerNode, const char *tablename, uint64 shardId)
{
	PGresult *result = NULL;
	char remoteCommand[MAXPGPATH];
	char *extendedTablename = NULL;
	int64 shardTableSize = -1;

	extendedTablename = ExtendTablename(tablename, shardId);
	snprintf(remoteCommand, MAXPGPATH, SHARD_TABLE_SIZE_COMMAND, extendedTablename);

	result = PQexec(workerNode, remoteCommand);
	if (result == NULL)
	{
		PQclear(result);
		free(extendedTablename);

		return -1;
	}

	shardTableSize = (int64) GetValueUint64(result, 0, 0);
	if (shardTableSize <= 0)
	{
		psql_error("remote command \"%s\" fetched invalid table size\n", remoteCommand);
		PQclear(result);
		free(extendedTablename);

		return -1;
	}

	PQclear(result);
	free(extendedTablename);

	return shardTableSize;
}


/*
 * ShardFileSize executes a command on the worker node to determine the size of
 * the foreign file represneting the shard. On success, the function returns the
 * file size on disk. On failure, the function returns -1.
 */
static int64
ShardFileSize(PGconn *workerNode, const char *tablename, uint64 shardId)
{
	int64 tableSize = -1;
	char remoteFilePath[MAXPGPATH];
	char remoteCommand[MAXPGPATH];
	PGresult *result = NULL;

	char *extendedTableName = ExtendTablename(tablename, shardId);
	snprintf(remoteFilePath, MAXPGPATH, "%s/%s", FOREIGN_CACHED_DIR, extendedTableName);
	free(extendedTableName);

	snprintf(remoteCommand, MAXPGPATH, REMOTE_FILE_SIZE_COMMAND, remoteFilePath);

	result = PQexec(workerNode, remoteCommand);
	if (result == NULL)
	{
		PQclear(result);
		return -1;
	}

	tableSize = (int64) GetValueUint64(result, 0, 0);
	if (tableSize <= 0)
	{
		psql_error("remote command \"%s\" fetched invalid table size\n", remoteCommand);
		PQclear(result);

		return -1;
	}

	PQclear(result);

	return tableSize;
}


/*
 * ShardColumnarTableSize executes a command on the worker node to determine the
 * size of the cstore_fdw table representing the shard. On success, the function
 * returns the table size on disk. On failure, the function returns -1.
 */
static int64
ShardColumnarTableSize(PGconn *workerNode, const char *tablename, uint64 shardId)
{
	PGresult *result = NULL;
	char remoteCommand[MAXPGPATH];
	char *extendedTablename = NULL;
	int64 shardTableSize = -1;

	extendedTablename = ExtendTablename(tablename, shardId);
	snprintf(remoteCommand, MAXPGPATH, SHARD_COLUMNAR_TABLE_SIZE_COMMAND,
			 extendedTablename);

	result = PQexec(workerNode, remoteCommand);
	if (result == NULL)
	{
		PQclear(result);
		free(extendedTablename);

		return -1;
	}

	shardTableSize = (int64) GetValueUint64(result, 0, 0);
	if (shardTableSize <= 0)
	{
		psql_error("remote command \"%s\" fetched invalid table size\n", remoteCommand);
		PQclear(result);
		free(extendedTablename);

		return -1;
	}

	PQclear(result);
	free(extendedTablename);

	return shardTableSize;
}


/*
 * ShardMinMaxValues executes command on the worker node to determine minimum
 * and maximum values for partition key expression. On successful execution, the
 * function sets min and max values in shard metadata and returns true. On
 * failure, the function returns false.
 */
static bool
ShardMinMaxValues(PGconn *workerNode, const char *tablename,
				  const char *partitionKey, ShardMetadata *shardMetadata)
{
	const int MinValueIndex = 0;
	const int MaxValueIndex = 1;

	PGresult *result = NULL;
	char remoteCommand[MAXPGPATH];
	char *extendedTablename = NULL;
	char *minValue = NULL;
	char *maxValue = NULL;
	int minValueLength = 0;
	int maxValueLength = 0;

	extendedTablename = ExtendTablename(tablename, shardMetadata->shardId);
	snprintf(remoteCommand, MAXPGPATH, SHARD_MIN_MAX_COMMAND,
			 partitionKey, partitionKey, extendedTablename);

	result = PQexec(workerNode, remoteCommand);
	if (result == NULL)
	{
		PQclear(result);
		free(extendedTablename);

		return false;
	}

	minValue = PQgetvalue(result, 0, MinValueIndex);
	maxValue = PQgetvalue(result, 0, MaxValueIndex);

	minValueLength = PQgetlength(result, 0, MinValueIndex);
	maxValueLength = PQgetlength(result, 0, MaxValueIndex);

	if (minValueLength <= 0 || maxValueLength <= 0)
	{
		psql_error("remote command \"%s\" fetched empty min/max values\n",
				   remoteCommand);
		PQclear(result);
		free(extendedTablename);

		return false;
	}

	shardMetadata->shardMinValue = (char *) pg_malloc0(minValueLength + 1);
	shardMetadata->shardMaxValue = (char *) pg_malloc0(maxValueLength + 1);

	strncpy(shardMetadata->shardMinValue, minValue, minValueLength + 1);
	strncpy(shardMetadata->shardMaxValue, maxValue, maxValueLength + 1);

	PQclear(result);
	free(extendedTablename);

	return true;
}
