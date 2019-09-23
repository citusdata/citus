/*-------------------------------------------------------------------------
 *
 * function.c
 *    Commands for FUNCTION statements.
 *
 *    We currently support replicating function definitions on the
 *    coordinator in all the worker nodes in the form of
 *
 *    CREATE OR REPLACE FUNCTION ... queries.
 *
 *    ALTER or DROP operations are not yet propagated.
 *
 * Copyright (c) 2019, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "miscadmin.h"
#include "funcapi.h"

#if PG_VERSION_NUM >= 120000
#include "access/genam.h"
#endif
#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/namespace.h"
#include "catalog/pg_proc.h"
#include "distributed/commands.h"
#include "catalog/pg_type.h"
#include "distributed/colocation_utils.h"
#include "distributed/master_protocol.h"
#include "distributed/maintenanced.h"
#include "distributed/metadata_sync.h"
#include "distributed/metadata/distobject.h"
#include "distributed/metadata/pg_dist_object.h"
#include "distributed/multi_executor.h"
#include "distributed/relation_access_tracking.h"
#include "distributed/worker_transaction.h"
#include "storage/lmgr.h"
#include "utils/builtins.h"
#include "utils/fmgrprotos.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

#define argumentStartsWith(arg, prefix) \
	(strncmp(arg, prefix, strlen(prefix)) == 0)

/* forward declaration for helper functions*/
static char * GetFunctionDDLCommand(const RegProcedure funcOid);
static int GetDistributionArgIndex(Oid functionOid, char *distributionArgumentName,
								   Oid *distributionArgumentOid);
static int GetFunctionColocationId(Oid functionOid, char *colocateWithName, Oid
								   distributionArgumentOid);
static void EnsureFunctionCanBeColocatedWithTable(Oid functionOid, Oid
												  distributionColumnType, Oid
												  sourceRelationId);
static void UpdateFunctionDistributionInfo(const ObjectAddress *distAddress,
										   int *distribution_argument_index,
										   int *colocationId);
static void EnsureSequentialModeForFunctionDDL(void);
static void TriggerSyncMetadataToPrimaryNodes(void);


PG_FUNCTION_INFO_V1(create_distributed_function);


/*
 * create_distributed_function gets a function or procedure name with their list of
 * argument types in parantheses, then it creates a new distributed function.
 */
Datum
create_distributed_function(PG_FUNCTION_ARGS)
{
	RegProcedure funcOid = PG_GETARG_OID(0);

	text *distributionArgumentNameText = NULL; /* optional */
	text *colocateWithText = NULL; /* optional */

	const char *ddlCommand = NULL;
	ObjectAddress functionAddress = { 0 };

	int distributionArgumentIndex = -1;
	Oid distributionArgumentOid = InvalidOid;
	int colocationId = -1;

	char *distributionArgumentName = NULL;
	char *colocateWithTableName = NULL;

	/* if called on NULL input, error out */
	if (funcOid == InvalidOid)
	{
		ereport(ERROR, (errmsg("the first parameter for create_distributed_function() "
							   "should be a single a valid function or procedure name "
							   "followed by a list of parameters in parantheses"),
						errhint("skip the parameters with OUT argtype as they are not "
								"part of the signature in PostgreSQL")));
	}

	if (PG_ARGISNULL(1))
	{
		/*
		 * Using the  default value, so distribute the function but do not set
		 * the  distribution argument.
		 */
		distributionArgumentName = NULL;
	}
	else
	{
		distributionArgumentNameText = PG_GETARG_TEXT_P(1);
		distributionArgumentName = text_to_cstring(distributionArgumentNameText);
	}

	if (PG_ARGISNULL(2))
	{
		ereport(ERROR, (errmsg("colocate_with parameter should not be NULL"),
						errhint("To use the default value, set colocate_with option "
								"to \"default\"")));
	}
	else
	{
		colocateWithText = PG_GETARG_TEXT_P(2);
		colocateWithTableName = text_to_cstring(colocateWithText);
	}


	ObjectAddressSet(functionAddress, ProcedureRelationId, funcOid);

	/*
	 * when we allow propagation within a transaction block we should make sure to only
	 * allow this in sequential mode
	 */
	EnsureSequentialModeForFunctionDDL();

	EnsureDependenciesExistsOnAllNodes(&functionAddress);

	ddlCommand = GetFunctionDDLCommand(funcOid);
	SendCommandToWorkers(ALL_WORKERS, ddlCommand);

	MarkObjectDistributed(&functionAddress);

	if (distributionArgumentName == NULL)
	{
		/* cannot provide colocate_with without distribution_arg_name */
		if (pg_strncasecmp(colocateWithTableName, "default", NAMEDATALEN) != 0)
		{
			char *functionName = get_func_name(funcOid);


			ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							errmsg("cannot distribute the function \"%s\" since the "
								   "distribution argument is not valid ", functionName),
							errhint("To provide \"colocate_with\" option, the"
									" distribution argument parameter should also "
									"be provided")));
		}

		/* set distribution argument and colocationId to NULL */
		UpdateFunctionDistributionInfo(&functionAddress, NULL, NULL);
	}
	else if (distributionArgumentName != NULL)
	{
		/* get the argument index, or error out if we cannot find a valid index */
		distributionArgumentIndex =
			GetDistributionArgIndex(funcOid, distributionArgumentName,
									&distributionArgumentOid);

		/* get the colocation id, or error out if we cannot find an appropriate one */
		colocationId =
			GetFunctionColocationId(funcOid, colocateWithTableName,
									distributionArgumentOid);

		/* if provided, make sure to record the distribution argument and colocationId */
		UpdateFunctionDistributionInfo(&functionAddress, &distributionArgumentIndex,
									   &colocationId);

		/*
		 * Once we have at least one distributed function/procedure with distribution
		 * argument, we sync the metadata to nodes so that the function/procedure
		 * delegation can be handled locally on the nodes.
		 */
		TriggerSyncMetadataToPrimaryNodes();
	}

	PG_RETURN_VOID();
}


/*
 * CreateFunctionDDLCommandsIdempotent returns a list of DDL statements (const char *) to be
 * executed on a node to recreate the function addressed by the functionAddress.
 */
List *
CreateFunctionDDLCommandsIdempotent(const ObjectAddress *functionAddress)
{
	char *ddlCommand = NULL;

	Assert(functionAddress->classId == ProcedureRelationId);

	ddlCommand = GetFunctionDDLCommand(functionAddress->objectId);
	return list_make1(ddlCommand);
}


/*
 * GetDistributionArgIndex calculates the distribution argument with the given
 * parameters. The function errors out if no valid argument is found.
 */
static int
GetDistributionArgIndex(Oid functionOid, char *distributionArgumentName,
						Oid *distributionArgumentOid)
{
	int distributionArgumentIndex = -1;

	int numberOfArgs = 0;
	int argIndex = 0;
	Oid *argTypes = NULL;
	char **argNames = NULL;
	char *argModes = NULL;

	HeapTuple proctup = NULL;

	*distributionArgumentOid = InvalidOid;

	proctup = SearchSysCache1(PROCOID, ObjectIdGetDatum(functionOid));
	if (!HeapTupleIsValid(proctup))
	{
		elog(ERROR, "cache lookup failed for function %u", functionOid);
	}

	numberOfArgs = get_func_arg_info(proctup, &argTypes, &argNames, &argModes);

	if (argumentStartsWith(distributionArgumentName, "$"))
	{
		/* skip the first character, we're safe because text_to_cstring pallocs */
		distributionArgumentName++;

		/* throws error if the input is not an integer */
		distributionArgumentIndex = pg_atoi(distributionArgumentName, 4, 0);

		if (distributionArgumentIndex < 1 || distributionArgumentIndex > numberOfArgs)
		{
			char *functionName = get_func_name(functionOid);

			ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							errmsg("cannot distribute the function \"%s\" since "
								   "the distribution argument is not valid",
								   functionName),
							errhint("Either provide a valid function argument name "
									"or a valid \"$paramIndex\" to "
									"create_distributed_function()")));
		}

		/*
		 * Internal representation for the distributionArgumentIndex
		 * starts from 0 whereas user facing API starts from 1.
		 */
		distributionArgumentIndex -= 1;
		*distributionArgumentOid = argTypes[distributionArgumentIndex];

		ReleaseSysCache(proctup);

		Assert(*distributionArgumentOid != InvalidOid);

		return distributionArgumentIndex;
	}

	/*
	 * The user didn't provid "$paramIndex" but potentially the name of the paramater.
	 * So, loop over the arguments and try to find the argument name that matches
	 * the parameter that user provided.
	 */
	for (argIndex = 0; argIndex < numberOfArgs; ++argIndex)
	{
		char *argNameOnIndex = argNames != NULL ? argNames[argIndex] : NULL;

		if (argNameOnIndex != NULL &&
			pg_strncasecmp(argNameOnIndex, distributionArgumentName, NAMEDATALEN) == 0)
		{
			distributionArgumentIndex = argIndex;

			*distributionArgumentOid = argTypes[argIndex];

			/* we found, no need to continue */
			break;
		}
	}

	/* we still couldn't find the argument, so error out */
	if (distributionArgumentIndex == -1)
	{
		char *functionName = get_func_name(functionOid);

		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("cannot distribute the function \"%s\" since the "
							   "distribution argument is not valid ", functionName),
						errhint("Either provide a valid function argument name "
								"or a valid \"$paramIndex\" to "
								"create_distributed_function()")));
	}

	ReleaseSysCache(proctup);

	Assert(*distributionArgumentOid != InvalidOid);

	return distributionArgumentIndex;
}


/*
 * GetFunctionColocationId gets the parameters for deciding the colocationId
 * of the function that is being distributed. The function errors out if it is
 * not possible to assign a colocationId to the input function.
 */
static int
GetFunctionColocationId(Oid functionOid, char *colocateWithTableName,
						Oid distributionArgumentOid)
{
	int colocationId = INVALID_COLOCATION_ID;
	bool createdColocationGroup = false;

	/*
	 * Get an exclusive lock on the colocation system catalog. Therefore, we
	 * can be sure that there will no modifications on the colocation table
	 * until this transaction is committed.
	 */
	Relation pgDistColocation = heap_open(DistColocationRelationId(), ExclusiveLock);

	if (pg_strncasecmp(colocateWithTableName, "default", NAMEDATALEN) == 0)
	{
		/* check for default colocation group */
		colocationId = ColocationId(ShardCount, ShardReplicationFactor,
									distributionArgumentOid);

		if (colocationId == INVALID_COLOCATION_ID)
		{
			colocationId =
				CreateColocationGroup(ShardCount, ShardReplicationFactor,
									  distributionArgumentOid);

			createdColocationGroup = true;
		}
	}
	else
	{
		Oid sourceRelationId =
			ResolveRelationId(cstring_to_text(colocateWithTableName), false);

		EnsureFunctionCanBeColocatedWithTable(functionOid, distributionArgumentOid,
											  sourceRelationId);

		colocationId = TableColocationId(sourceRelationId);
	}

	/*
	 * If we created a new colocation group then we need to keep the lock to
	 * prevent a concurrent create_distributed_table call from creating another
	 * colocation group with the same parameters. If we're using an existing
	 * colocation group then other transactions will use the same one.
	 */
	if (createdColocationGroup)
	{
		/* keep the exclusive lock */
		heap_close(pgDistColocation, NoLock);
	}
	else
	{
		/* release the exclusive lock */
		heap_close(pgDistColocation, ExclusiveLock);
	}


	return colocationId;
}


/*
 * EnsureFunctionCanBeColocatedWithTable checks whether the given arguments are
 * suitable to distribute the function to be colocated with given source table.
 */
static void
EnsureFunctionCanBeColocatedWithTable(Oid functionOid, Oid distributionColumnType,
									  Oid sourceRelationId)
{
	DistTableCacheEntry *sourceTableEntry = DistributedTableCacheEntry(sourceRelationId);
	char sourceDistributionMethod = sourceTableEntry->partitionMethod;
	char sourceReplicationModel = sourceTableEntry->replicationModel;
	Var *sourceDistributionColumn = DistPartitionKey(sourceRelationId);
	Oid sourceDistributionColumnType = InvalidOid;

	if (sourceDistributionMethod != DISTRIBUTE_BY_HASH)
	{
		char *functionName = get_func_name(functionOid);
		char *sourceRelationName = get_rel_name(sourceRelationId);

		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot colocate function \"%s\" and table \"%s\" because "
							   "colocate_with option is only supported for hash "
							   "distributed tables.", functionName,
							   sourceRelationName)));
	}

	if (sourceReplicationModel != REPLICATION_MODEL_STREAMING)
	{
		char *functionName = get_func_name(functionOid);
		char *sourceRelationName = get_rel_name(sourceRelationId);

		ereport(ERROR, (errmsg("cannot colocate function \"%s\" and table \"%s\"",
							   functionName, sourceRelationName),
						errdetail("Citus currently only supports colocating function "
								  "with distributed tables that are created using "
								  "streaming replication model."),
						errhint("When distributing tables make sure that "
								"\"citus.replication_model\" is set to \"streaming\"")));
	}

	sourceDistributionColumnType = sourceDistributionColumn->vartype;
	if (sourceDistributionColumnType != distributionColumnType)
	{
		char *functionName = get_func_name(functionOid);
		char *sourceRelationName = get_rel_name(sourceRelationId);

		ereport(ERROR, (errmsg("cannot colocate function \"%s\" and table \"%s\" "
							   "because distribution column types don't match",
							   sourceRelationName, functionName)));
	}
}


/*
 * UpdateFunctionDistributionInfo gets object address of a function and
 * updates its distribution_argument_index and colocationId in pg_dist_object.
 */
static void
UpdateFunctionDistributionInfo(const ObjectAddress *distAddress,
							   int *distribution_argument_index,
							   int *colocationId)
{
	const bool indexOK = true;

	Relation pgDistObjectRel = NULL;
	TupleDesc tupleDescriptor = NULL;
	ScanKeyData scanKey[3];
	SysScanDesc scanDescriptor = NULL;
	HeapTuple heapTuple = NULL;
	Datum values[Natts_pg_dist_object];
	bool isnull[Natts_pg_dist_object];
	bool replace[Natts_pg_dist_object];

	pgDistObjectRel = heap_open(DistObjectRelationId(), RowExclusiveLock);
	tupleDescriptor = RelationGetDescr(pgDistObjectRel);

	/* scan pg_dist_object for classid = $1 AND objid = $2 AND objsubid = $3 via index */
	ScanKeyInit(&scanKey[0], Anum_pg_dist_object_classid, BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(distAddress->classId));
	ScanKeyInit(&scanKey[1], Anum_pg_dist_object_objid, BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(distAddress->objectId));
	ScanKeyInit(&scanKey[2], Anum_pg_dist_object_objsubid, BTEqualStrategyNumber,
				F_INT4EQ, ObjectIdGetDatum(distAddress->objectSubId));

	scanDescriptor = systable_beginscan(pgDistObjectRel, DistObjectPrimaryKeyIndexId(),
										indexOK,
										NULL, 3, scanKey);

	heapTuple = systable_getnext(scanDescriptor);
	if (!HeapTupleIsValid(heapTuple))
	{
		ereport(ERROR, (errmsg("could not find valid entry for node \"%d,%d,%d\" "
							   "in pg_dist_object", distAddress->classId,
							   distAddress->objectId, distAddress->objectSubId)));
	}

	memset(replace, 0, sizeof(replace));

	replace[Anum_pg_dist_object_distribution_argument_index - 1] = true;

	if (distribution_argument_index != NULL)
	{
		values[Anum_pg_dist_object_distribution_argument_index - 1] = Int32GetDatum(
			*distribution_argument_index);
		isnull[Anum_pg_dist_object_distribution_argument_index - 1] = false;
	}
	else
	{
		isnull[Anum_pg_dist_object_distribution_argument_index - 1] = true;
	}

	replace[Anum_pg_dist_object_colocationid - 1] = true;
	if (colocationId != NULL)
	{
		values[Anum_pg_dist_object_colocationid - 1] = Int32GetDatum(*colocationId);
		isnull[Anum_pg_dist_object_colocationid - 1] = false;
	}
	else
	{
		isnull[Anum_pg_dist_object_colocationid - 1] = true;
	}

	heapTuple = heap_modify_tuple(heapTuple, tupleDescriptor, values, isnull, replace);

	CatalogTupleUpdate(pgDistObjectRel, &heapTuple->t_self, heapTuple);

	CitusInvalidateRelcacheByRelid(DistObjectRelationId());

	CommandCounterIncrement();

	systable_endscan(scanDescriptor);

	heap_close(pgDistObjectRel, NoLock);
}


/*
 * GetFunctionDDLCommand returns the complete "CREATE OR REPLACE FUNCTION ..." statement for
 * the specified function.
 */
static char *
GetFunctionDDLCommand(const RegProcedure funcOid)
{
	OverrideSearchPath *overridePath = NULL;
	Datum sqlTextDatum = 0;
	char *sql = NULL;

	/*
	 * Set search_path to NIL so that all objects outside of pg_catalog will be
	 * schema-prefixed. pg_catalog will be added automatically when we call
	 * PushOverrideSearchPath(), since we set addCatalog to true;
	 */
	overridePath = GetOverrideSearchPath(CurrentMemoryContext);
	overridePath->schemas = NIL;
	overridePath->addCatalog = true;
	PushOverrideSearchPath(overridePath);

	sqlTextDatum = DirectFunctionCall1(pg_get_functiondef,
									   ObjectIdGetDatum(funcOid));

	/* revert back to original search_path */
	PopOverrideSearchPath();

	sql = TextDatumGetCString(sqlTextDatum);
	return sql;
}


/*
 * EnsureSequentialModeForFunctionDDL makes sure that the current transaction is already in
 * sequential mode, or can still safely be put in sequential mode, it errors if that is
 * not possible. The error contains information for the user to retry the transaction with
 * sequential mode set from the beginnig.
 *
 * As functions are node scoped objects there exists only 1 instance of the function used by
 * potentially multiple shards. To make sure all shards in the transaction can interact
 * with the function the function needs to be visible on all connections used by the transaction,
 * meaning we can only use 1 connection per node.
 */
static void
EnsureSequentialModeForFunctionDDL(void)
{
	if (ParallelQueryExecutedInTransaction())
	{
		ereport(ERROR, (errmsg("cannot create function because there was a "
							   "parallel operation on a distributed table in the "
							   "transaction"),
						errdetail("When creating a distributed function, Citus needs to "
								  "perform all operations over a single connection per "
								  "node to ensure consistency."),
						errhint("Try re-running the transaction with "
								"\"SET LOCAL citus.multi_shard_modify_mode TO "
								"\'sequential\';\"")));
	}

	ereport(DEBUG1, (errmsg("switching to sequential query execution mode"),
					 errdetail(
						 "A distributed function is created. To make sure subsequent "
						 "commands see the type correctly we need to make sure to "
						 "use only one connection for all future commands")));
	SetLocalMultiShardModifyModeToSequential();
}


/*
 * TriggerSyncMetadataToPrimaryNodes iterates over the active primary nodes,
 * and triggers the metadata syncs if the node has not the metadata. Later,
 * maintenance daemon will sync the metadata to nodes.
 */
static void
TriggerSyncMetadataToPrimaryNodes(void)
{
	List *workerList = ActivePrimaryNodeList(ShareLock);
	ListCell *workerCell = NULL;
	bool triggerMetadataSync = false;

	foreach(workerCell, workerList)
	{
		WorkerNode *workerNode = (WorkerNode *) lfirst(workerCell);

		/* if already has metadata, no need to do it again */
		if (!workerNode->hasMetadata)
		{
			/*
			 * Let the maintanince deamon do the hard work of syncing the metadata. We prefer
			 * this because otherwise node activation might fail withing transaction blocks.
			 */
			LockRelationOid(DistNodeRelationId(), ExclusiveLock);
			MarkNodeHasMetadata(workerNode->workerName, workerNode->workerPort, true);

			triggerMetadataSync = true;
		}
	}

	/* let the maintanince deamon know about the metadata sync */
	if (triggerMetadataSync)
	{
		TriggerMetadataSync(MyDatabaseId);
	}
}
