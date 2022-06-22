/*-------------------------------------------------------------------------
 *
 * function.c
 *    Commands for FUNCTION statements.
 *
 *    We currently support replicating function definitions on the
 *    coordinator in all the worker nodes in the form of
 *
 *    CREATE OR REPLACE FUNCTION ... queries and
 *    GRANT ... ON FUNCTION queries
 *
 *
 *    ALTER or DROP operations are not yet propagated.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "miscadmin.h"
#include "funcapi.h"

#include "distributed/pg_version_constants.h"

#include "access/genam.h"
#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/pg_aggregate.h"
#include "catalog/dependency.h"
#include "catalog/namespace.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "commands/extension.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/citus_safe_lib.h"
#include "distributed/colocation_utils.h"
#include "distributed/commands.h"
#include "distributed/commands/utility_hook.h"
#include "distributed/deparser.h"
#include "distributed/listutils.h"
#include "distributed/maintenanced.h"
#include "distributed/metadata_utility.h"
#include "distributed/metadata/dependency.h"
#include "distributed/coordinator_protocol.h"
#include "distributed/metadata/distobject.h"
#include "distributed/metadata/pg_dist_object.h"
#include "distributed/metadata_sync.h"
#include "distributed/multi_executor.h"
#include "distributed/namespace_utils.h"
#include "distributed/pg_dist_node.h"
#include "distributed/reference_table_utils.h"
#include "distributed/relation_access_tracking.h"
#include "distributed/version_compat.h"
#include "distributed/worker_create_or_replace.h"
#include "distributed/worker_transaction.h"
#include "nodes/makefuncs.h"
#include "parser/parse_coerce.h"
#include "parser/parse_type.h"
#include "storage/lmgr.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/fmgrprotos.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/regproc.h"

#define DISABLE_LOCAL_CHECK_FUNCTION_BODIES "SET LOCAL check_function_bodies TO off;"
#define RESET_CHECK_FUNCTION_BODIES "RESET check_function_bodies;"
#define argumentStartsWith(arg, prefix) \
	(strncmp(arg, prefix, strlen(prefix)) == 0)

/* forward declaration for helper functions*/
static bool RecreateSameNonColocatedFunction(ObjectAddress functionAddress,
											 char *distributionArgumentName,
											 bool colocateWithTableNameDefault,
											 bool *forceDelegationAddress);
static void ErrorIfAnyNodeDoesNotHaveMetadata(void);
static char * GetAggregateDDLCommand(const RegProcedure funcOid, bool useCreateOrReplace);
static char * GetFunctionAlterOwnerCommand(const RegProcedure funcOid);
static int GetDistributionArgIndex(Oid functionOid, char *distributionArgumentName,
								   Oid *distributionArgumentOid);
static int GetFunctionColocationId(Oid functionOid, char *colocateWithName, Oid
								   distributionArgumentOid);
static void EnsureFunctionCanBeColocatedWithTable(Oid functionOid, Oid
												  distributionColumnType, Oid
												  sourceRelationId);
static bool ShouldPropagateCreateFunction(CreateFunctionStmt *stmt);
static bool ShouldPropagateAlterFunction(const ObjectAddress *address);
static bool ShouldAddFunctionSignature(FunctionParameterMode mode);
static ObjectAddress FunctionToObjectAddress(ObjectType objectType,
											 ObjectWithArgs *objectWithArgs,
											 bool missing_ok);
static void ErrorIfUnsupportedAlterFunctionStmt(AlterFunctionStmt *stmt);
static char * quote_qualified_func_name(Oid funcOid);
static void DistributeFunctionWithDistributionArgument(RegProcedure funcOid,
													   char *distributionArgumentName,
													   Oid distributionArgumentOid,
													   char *colocateWithTableName,
													   bool *forceDelegationAddress,
													   const ObjectAddress *
													   functionAddress);
static void DistributeFunctionColocatedWithDistributedTable(RegProcedure funcOid,
															char *colocateWithTableName,
															const ObjectAddress *
															functionAddress);
static void DistributeFunctionColocatedWithReferenceTable(const
														  ObjectAddress *functionAddress);
static List * FilterDistributedFunctions(GrantStmt *grantStmt);

static void EnsureExtensionFunctionCanBeDistributed(const ObjectAddress functionAddress,
													const ObjectAddress extensionAddress,
													char *distributionArgumentName);

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

	StringInfoData ddlCommand = { 0 };
	ObjectAddress functionAddress = { 0 };

	Oid distributionArgumentOid = InvalidOid;
	bool colocatedWithReferenceTable = false;

	char *distributionArgumentName = NULL;
	char *colocateWithTableName = NULL;
	bool colocateWithTableNameDefault = false;
	bool *forceDelegationAddress = NULL;
	bool forceDelegation = false;
	ObjectAddress extensionAddress = { 0 };

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
		 * Using the default value, so distribute the function but do not set
		 * the distribution argument.
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

		if (pg_strncasecmp(colocateWithTableName, "default", NAMEDATALEN) == 0)
		{
			colocateWithTableNameDefault = true;
		}

		/* check if the colocation belongs to a reference table */
		if (!colocateWithTableNameDefault)
		{
			Oid colocationRelationId = ResolveRelationId(colocateWithText, false);
			colocatedWithReferenceTable = IsCitusTableType(colocationRelationId,
														   REFERENCE_TABLE);
		}
	}

	/* check if the force_delegation flag is explicitly set (default is NULL) */
	if (PG_ARGISNULL(3))
	{
		forceDelegationAddress = NULL;
	}
	else
	{
		forceDelegation = PG_GETARG_BOOL(3);
		forceDelegationAddress = &forceDelegation;
	}

	EnsureCoordinator();
	EnsureFunctionOwner(funcOid);

	ObjectAddressSet(functionAddress, ProcedureRelationId, funcOid);

	if (RecreateSameNonColocatedFunction(functionAddress,
										 distributionArgumentName,
										 colocateWithTableNameDefault,
										 forceDelegationAddress))
	{
		char *schemaName = get_namespace_name(get_func_namespace(funcOid));
		char *functionName = get_func_name(funcOid);
		char *qualifiedName = quote_qualified_identifier(schemaName, functionName);
		ereport(NOTICE, (errmsg("procedure %s is already distributed", qualifiedName),
						 errdetail("Citus distributes procedures with CREATE "
								   "[PROCEDURE|FUNCTION|AGGREGATE] commands")));
		PG_RETURN_VOID();
	}

	/*
	 * If the function is owned by an extension, only update the
	 * pg_dist_object, and not propagate the CREATE FUNCTION. Function
	 * will be created by the virtue of the extension creation.
	 */
	if (IsObjectAddressOwnedByExtension(&functionAddress, &extensionAddress))
	{
		EnsureExtensionFunctionCanBeDistributed(functionAddress, extensionAddress,
												distributionArgumentName);
	}
	else
	{
		/*
		 * when we allow propagation within a transaction block we should make sure
		 * to only allow this in sequential mode.
		 */
		EnsureSequentialMode(OBJECT_FUNCTION);

		EnsureDependenciesExistOnAllNodes(&functionAddress);

		const char *createFunctionSQL = GetFunctionDDLCommand(funcOid, true);
		const char *alterFunctionOwnerSQL = GetFunctionAlterOwnerCommand(funcOid);
		initStringInfo(&ddlCommand);
		appendStringInfo(&ddlCommand, "%s;%s;%s", DISABLE_METADATA_SYNC,
						 createFunctionSQL, alterFunctionOwnerSQL);
		List *grantDDLCommands = GrantOnFunctionDDLCommands(funcOid);
		char *grantOnFunctionSQL = NULL;
		foreach_ptr(grantOnFunctionSQL, grantDDLCommands)
		{
			appendStringInfo(&ddlCommand, ";%s", grantOnFunctionSQL);
		}

		appendStringInfo(&ddlCommand, ";%s", ENABLE_METADATA_SYNC);

		SendCommandToWorkersAsUser(NON_COORDINATOR_NODES, CurrentUserName(),
								   ddlCommand.data);
	}

	MarkObjectDistributed(&functionAddress);

	if (distributionArgumentName != NULL)
	{
		/*
		 * Prior to Citus 11, this code was triggering metadata
		 * syncing. However, with Citus 11+, we expect the metadata
		 * has already been synced.
		 */
		ErrorIfAnyNodeDoesNotHaveMetadata();

		DistributeFunctionWithDistributionArgument(funcOid, distributionArgumentName,
												   distributionArgumentOid,
												   colocateWithTableName,
												   forceDelegationAddress,
												   &functionAddress);
	}
	else if (!colocatedWithReferenceTable)
	{
		DistributeFunctionColocatedWithDistributedTable(funcOid, colocateWithTableName,
														&functionAddress);
	}
	else if (colocatedWithReferenceTable)
	{
		/*
		 * Prior to Citus 11, this code was triggering metadata
		 * syncing. However, with Citus 11+, we expect the metadata
		 * has already been synced.
		 */
		ErrorIfAnyNodeDoesNotHaveMetadata();

		DistributeFunctionColocatedWithReferenceTable(&functionAddress);
	}

	PG_RETURN_VOID();
}


/*
 * RecreateSameNonColocatedFunction returns true if the given parameters of
 * create_distributed_function will not change anything on the given function.
 * Returns false otherwise.
 */
static bool
RecreateSameNonColocatedFunction(ObjectAddress functionAddress,
								 char *distributionArgumentName,
								 bool colocateWithTableNameDefault,
								 bool *forceDelegationAddress)
{
	DistObjectCacheEntry *cacheEntry =
		LookupDistObjectCacheEntry(ProcedureRelationId,
								   functionAddress.objectId,
								   InvalidOid);

	if (cacheEntry == NULL || !cacheEntry->isValid || !cacheEntry->isDistributed)
	{
		return false;
	}

	/*
	 * If the colocationId, forceDelegation and distributionArgIndex fields of a
	 * pg_dist_object entry of a distributed function are all set to zero, it means
	 * that function is either automatically distributed by ddl propagation, without
	 * calling create_distributed_function. Or, it could be distributed via
	 * create_distributed_function, but with no parameters.
	 *
	 * For these cases, calling create_distributed_function for that function,
	 * without parameters would be idempotent. Hence we can simply early return here,
	 * by providing a notice message to the user.
	 */

	/* are pg_dist_object fields set to zero? */
	bool functionDistributedWithoutParams =
		cacheEntry->colocationId == 0 &&
		cacheEntry->forceDelegation == 0 &&
		cacheEntry->distributionArgIndex == 0;

	/* called create_distributed_function without parameters? */
	bool distributingAgainWithNoParams =
		distributionArgumentName == NULL &&
		colocateWithTableNameDefault &&
		forceDelegationAddress == NULL;

	return functionDistributedWithoutParams && distributingAgainWithNoParams;
}


/*
 * ErrorIfAnyNodeDoesNotHaveMetadata throws error if any
 * of the worker nodes does not have the metadata.
 */
static void
ErrorIfAnyNodeDoesNotHaveMetadata(void)
{
	List *workerNodeList =
		ActivePrimaryNonCoordinatorNodeList(ShareLock);

	WorkerNode *workerNode = NULL;
	foreach_ptr(workerNode, workerNodeList)
	{
		if (!workerNode->hasMetadata)
		{
			ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							errmsg("cannot process the distributed function "
								   "since the node %s:%d does not have metadata "
								   "synced and this command requires all the nodes "
								   "have the metadata sycned", workerNode->workerName,
								   workerNode->workerPort),
							errhint("To sync the metadata execute: "
									"SELECT enable_citus_mx_for_pre_citus11();")));
		}
	}
}


/*
 * DistributeFunctionWithDistributionArgument updates pg_dist_object records for
 * a function/procedure that has a distribution argument, and triggers metadata
 * sync so that the functions can be delegated on workers.
 */
static void
DistributeFunctionWithDistributionArgument(RegProcedure funcOid,
										   char *distributionArgumentName,
										   Oid distributionArgumentOid,
										   char *colocateWithTableName,
										   bool *forceDelegationAddress,
										   const ObjectAddress *functionAddress)
{
	/* get the argument index, or error out if we cannot find a valid index */
	int distributionArgumentIndex =
		GetDistributionArgIndex(funcOid, distributionArgumentName,
								&distributionArgumentOid);

	/* get the colocation id, or error out if we cannot find an appropriate one */
	int colocationId =
		GetFunctionColocationId(funcOid, colocateWithTableName,
								distributionArgumentOid);

	/* record the distribution argument and colocationId */
	UpdateFunctionDistributionInfo(functionAddress, &distributionArgumentIndex,
								   &colocationId,
								   forceDelegationAddress);
}


/*
 * DistributeFunctionColocatedWithDistributedTable updates pg_dist_object records for
 * a function/procedure that is colocated with a distributed table.
 */
static void
DistributeFunctionColocatedWithDistributedTable(RegProcedure funcOid,
												char *colocateWithTableName,
												const ObjectAddress *functionAddress)
{
	/*
	 * cannot provide colocate_with without distribution_arg_name when the function
	 * is not colocated with a reference table
	 */
	if (pg_strncasecmp(colocateWithTableName, "default", NAMEDATALEN) != 0)
	{
		char *functionName = get_func_name(funcOid);


		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("cannot distribute the function \"%s\" since the "
							   "distribution argument is not valid ", functionName),
						errhint("To provide \"colocate_with\" option with a"
								" distributed table, the distribution argument"
								" parameter should also be provided")));
	}

	/* set distribution argument and colocationId to NULL */
	UpdateFunctionDistributionInfo(functionAddress, NULL, NULL, NULL);
}


/*
 * DistributeFunctionColocatedWithReferenceTable updates pg_dist_object records for
 * a function/procedure that is colocated with a reference table.
 */
static void
DistributeFunctionColocatedWithReferenceTable(const ObjectAddress *functionAddress)
{
	/* get the reference table colocation id */
	int colocationId = CreateReferenceTableColocationId();

	/* set distribution argument to NULL and colocationId to the reference table colocation id */
	int *distributionArgumentIndex = NULL;
	UpdateFunctionDistributionInfo(functionAddress, distributionArgumentIndex,
								   &colocationId,
								   NULL);
}


/*
 * CreateFunctionDDLCommandsIdempotent returns a list of DDL statements (const char *) to be
 * executed on a node to recreate the function addressed by the functionAddress.
 */
List *
CreateFunctionDDLCommandsIdempotent(const ObjectAddress *functionAddress)
{
	Assert(functionAddress->classId == ProcedureRelationId);

	char *ddlCommand = GetFunctionDDLCommand(functionAddress->objectId, true);
	char *alterFunctionOwnerSQL = GetFunctionAlterOwnerCommand(functionAddress->objectId);

	return list_make4(
		DISABLE_LOCAL_CHECK_FUNCTION_BODIES,
		ddlCommand,
		alterFunctionOwnerSQL,
		RESET_CHECK_FUNCTION_BODIES);
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

	Oid *argTypes = NULL;
	char **argNames = NULL;
	char *argModes = NULL;


	*distributionArgumentOid = InvalidOid;

	HeapTuple proctup = SearchSysCache1(PROCOID, ObjectIdGetDatum(functionOid));
	if (!HeapTupleIsValid(proctup))
	{
		elog(ERROR, "cache lookup failed for function %u", functionOid);
	}

	int numberOfArgs = get_func_arg_info(proctup, &argTypes, &argNames, &argModes);

	if (argumentStartsWith(distributionArgumentName, "$"))
	{
		/* skip the first character, we're safe because text_to_cstring pallocs */
		distributionArgumentName++;

		/* throws error if the input is not an integer */
		distributionArgumentIndex = pg_strtoint32(distributionArgumentName);

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
	 * The user didn't provid "$paramIndex" but potentially the name of the parameter.
	 * So, loop over the arguments and try to find the argument name that matches
	 * the parameter that user provided.
	 */
	for (int argIndex = 0; argIndex < numberOfArgs; ++argIndex)
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
	Relation pgDistColocation = table_open(DistColocationRelationId(), ShareLock);

	if (pg_strncasecmp(colocateWithTableName, "default", NAMEDATALEN) == 0)
	{
		/* check for default colocation group */
		colocationId = ColocationId(ShardCount, ShardReplicationFactor,
									distributionArgumentOid, get_typcollation(
										distributionArgumentOid));

		if (colocationId == INVALID_COLOCATION_ID)
		{
			char *functionName = get_func_name(functionOid);

			ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							errmsg("cannot distribute the function \"%s\" since there "
								   "is no table to colocate with", functionName),
							errhint("Provide a distributed table via \"colocate_with\" "
									"option to create_distributed_function()")));
		}

		Oid colocatedTableId = ColocatedTableId(colocationId);
		if (colocatedTableId != InvalidOid)
		{
			EnsureFunctionCanBeColocatedWithTable(functionOid, distributionArgumentOid,
												  colocatedTableId);
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

	/* keep the lock */
	table_close(pgDistColocation, NoLock);

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
	CitusTableCacheEntry *sourceTableEntry = GetCitusTableCacheEntry(sourceRelationId);
	char sourceReplicationModel = sourceTableEntry->replicationModel;

	if (!IsCitusTableTypeCacheEntry(sourceTableEntry, HASH_DISTRIBUTED) &&
		!IsCitusTableTypeCacheEntry(sourceTableEntry, REFERENCE_TABLE))
	{
		char *functionName = get_func_name(functionOid);
		char *sourceRelationName = get_rel_name(sourceRelationId);

		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot colocate function \"%s\" and table \"%s\" because "
							   "colocate_with option is only supported for hash "
							   "distributed tables and reference tables.",
							   functionName, sourceRelationName)));
	}

	if (IsCitusTableTypeCacheEntry(sourceTableEntry, REFERENCE_TABLE) &&
		distributionColumnType != InvalidOid)
	{
		char *functionName = get_func_name(functionOid);
		char *sourceRelationName = get_rel_name(sourceRelationId);

		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot colocate function \"%s\" and table \"%s\" because "
							   "distribution arguments are not supported when "
							   "colocating with reference tables.",
							   functionName, sourceRelationName)));
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
								"citus.shard_replication_factor = 1")));
	}

	/*
	 * If the types are the same, we're good. If not, we still check if there
	 * is any coercion path between the types.
	 */
	Var *sourceDistributionColumn = DistPartitionKeyOrError(sourceRelationId);
	Oid sourceDistributionColumnType = sourceDistributionColumn->vartype;
	if (sourceDistributionColumnType != distributionColumnType)
	{
		Oid coercionFuncId = InvalidOid;

		CoercionPathType coercionType =
			find_coercion_pathway(distributionColumnType, sourceDistributionColumnType,
								  COERCION_EXPLICIT, &coercionFuncId);

		/* if there is no path for coercion, error out*/
		if (coercionType == COERCION_PATH_NONE)
		{
			char *functionName = get_func_name(functionOid);
			char *sourceRelationName = get_rel_name(sourceRelationId);

			ereport(ERROR, (errmsg("cannot colocate function \"%s\" and table \"%s\" "
								   "because distribution column types don't match and "
								   "there is no coercion path", sourceRelationName,
								   functionName)));
		}
	}
}


/*
 * UpdateFunctionDistributionInfo gets object address of a function and
 * updates its distribution_argument_index and colocationId in pg_dist_object.
 * Then update pg_dist_object on nodes with metadata if object propagation is on.
 */
void
UpdateFunctionDistributionInfo(const ObjectAddress *distAddress,
							   int *distribution_argument_index,
							   int *colocationId,
							   bool *forceDelegation)
{
	const bool indexOK = true;

	ScanKeyData scanKey[3];
	Datum values[Natts_pg_dist_object];
	bool isnull[Natts_pg_dist_object];
	bool replace[Natts_pg_dist_object];

	Relation pgDistObjectRel = table_open(DistObjectRelationId(), RowExclusiveLock);
	TupleDesc tupleDescriptor = RelationGetDescr(pgDistObjectRel);

	/* scan pg_dist_object for classid = $1 AND objid = $2 AND objsubid = $3 via index */
	ScanKeyInit(&scanKey[0], Anum_pg_dist_object_classid, BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(distAddress->classId));
	ScanKeyInit(&scanKey[1], Anum_pg_dist_object_objid, BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(distAddress->objectId));
	ScanKeyInit(&scanKey[2], Anum_pg_dist_object_objsubid, BTEqualStrategyNumber,
				F_INT4EQ, ObjectIdGetDatum(distAddress->objectSubId));

	SysScanDesc scanDescriptor = systable_beginscan(pgDistObjectRel,
													DistObjectPrimaryKeyIndexId(),
													indexOK,
													NULL, 3, scanKey);

	HeapTuple heapTuple = systable_getnext(scanDescriptor);
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

	replace[Anum_pg_dist_object_force_delegation - 1] = true;
	if (forceDelegation != NULL)
	{
		values[Anum_pg_dist_object_force_delegation - 1] = BoolGetDatum(
			*forceDelegation);
		isnull[Anum_pg_dist_object_force_delegation - 1] = false;
	}
	else
	{
		isnull[Anum_pg_dist_object_force_delegation - 1] = true;
	}

	heapTuple = heap_modify_tuple(heapTuple, tupleDescriptor, values, isnull, replace);

	CatalogTupleUpdate(pgDistObjectRel, &heapTuple->t_self, heapTuple);

	CitusInvalidateRelcacheByRelid(DistObjectRelationId());

	CommandCounterIncrement();

	systable_endscan(scanDescriptor);

	table_close(pgDistObjectRel, NoLock);

	if (EnableMetadataSync)
	{
		List *objectAddressList = list_make1((ObjectAddress *) distAddress);
		List *distArgumentIndexList = NIL;
		List *colocationIdList = NIL;
		List *forceDelegationList = NIL;

		if (distribution_argument_index == NULL)
		{
			distArgumentIndexList = list_make1_int(INVALID_DISTRIBUTION_ARGUMENT_INDEX);
		}
		else
		{
			distArgumentIndexList = list_make1_int(*distribution_argument_index);
		}

		if (colocationId == NULL)
		{
			colocationIdList = list_make1_int(INVALID_COLOCATION_ID);
		}
		else
		{
			colocationIdList = list_make1_int(*colocationId);
		}

		if (forceDelegation == NULL)
		{
			forceDelegationList = list_make1_int(NO_FORCE_PUSHDOWN);
		}
		else
		{
			forceDelegationList = list_make1_int(*forceDelegation);
		}

		char *workerPgDistObjectUpdateCommand =
			MarkObjectsDistributedCreateCommand(objectAddressList,
												distArgumentIndexList,
												colocationIdList,
												forceDelegationList);
		SendCommandToWorkersWithMetadata(workerPgDistObjectUpdateCommand);
	}
}


/*
 * GetFunctionDDLCommand returns the complete "CREATE OR REPLACE FUNCTION ..." statement for
 * the specified function.
 *
 * useCreateOrReplace is ignored for non-aggregate functions.
 */
char *
GetFunctionDDLCommand(const RegProcedure funcOid, bool useCreateOrReplace)
{
	char *createFunctionSQL = NULL;

	if (get_func_prokind(funcOid) == PROKIND_AGGREGATE)
	{
		createFunctionSQL = GetAggregateDDLCommand(funcOid, useCreateOrReplace);
	}
	else
	{
		Datum sqlTextDatum = (Datum) 0;

		PushOverrideEmptySearchPath(CurrentMemoryContext);

		sqlTextDatum = DirectFunctionCall1(pg_get_functiondef,
										   ObjectIdGetDatum(funcOid));
		createFunctionSQL = TextDatumGetCString(sqlTextDatum);

		/* revert back to original search_path */
		PopOverrideSearchPath();
	}

	return createFunctionSQL;
}


/*
 * GetFunctionAlterOwnerCommand returns "ALTER FUNCTION .. SET OWNER .." statement for
 * the specified function.
 */
static char *
GetFunctionAlterOwnerCommand(const RegProcedure funcOid)
{
	HeapTuple proctup = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcOid));
	StringInfo alterCommand = makeStringInfo();
	Oid procOwner = InvalidOid;


	if (HeapTupleIsValid(proctup))
	{
		Form_pg_proc procform = (Form_pg_proc) GETSTRUCT(proctup);

		procOwner = procform->proowner;

		ReleaseSysCache(proctup);
	}
	else if (!OidIsValid(funcOid) || !HeapTupleIsValid(proctup))
	{
		ereport(ERROR, (errmsg("cannot find function with oid: %d", funcOid)));
	}

	/*
	 * If the function exists we want to use format_procedure_qualified to
	 * serialize its canonical arguments
	 */
	char *functionSignature = format_procedure_qualified(funcOid);
	char *functionOwner = GetUserNameFromId(procOwner, false);

	appendStringInfo(alterCommand, "ALTER ROUTINE %s OWNER TO %s;",
					 functionSignature,
					 quote_identifier(functionOwner));

	return alterCommand->data;
}


/*
 * GetAggregateDDLCommand returns a string for creating an aggregate.
 * CREATE OR REPLACE AGGREGATE was only introduced in pg12,
 * so a second parameter useCreateOrReplace signals whether to
 * to create a plain CREATE AGGREGATE or not. In pg11 we return a string
 * which is a call to worker_create_or_replace_object in lieu of
 * CREATE OR REPLACE AGGREGATE.
 */
static char *
GetAggregateDDLCommand(const RegProcedure funcOid, bool useCreateOrReplace)
{
	StringInfoData buf = { 0 };
	int i = 0;
	Oid *argtypes = NULL;
	char **argnames = NULL;
	char *argmodes = NULL;
	int insertorderbyat = -1;
	int argsprinted = 0;
	int inputargno = 0;

	HeapTuple proctup = SearchSysCache1(PROCOID, funcOid);
	if (!HeapTupleIsValid(proctup))
	{
		elog(ERROR, "cache lookup failed for %d", funcOid);
	}

	Form_pg_proc proc = (Form_pg_proc) GETSTRUCT(proctup);

	Assert(proc->prokind == PROKIND_AGGREGATE);

	initStringInfo(&buf);

	const char *name = NameStr(proc->proname);
	const char *nsp = get_namespace_name(proc->pronamespace);

	if (useCreateOrReplace)
	{
		appendStringInfo(&buf, "CREATE OR REPLACE AGGREGATE %s(",
						 quote_qualified_identifier(nsp, name));
	}
	else
	{
		appendStringInfo(&buf, "CREATE AGGREGATE %s(",
						 quote_qualified_identifier(nsp, name));
	}

	/* Parameters, borrows heavily from print_function_arguments in postgres */
	int numargs = get_func_arg_info(proctup, &argtypes, &argnames, &argmodes);

	HeapTuple aggtup = SearchSysCache1(AGGFNOID, funcOid);
	if (!HeapTupleIsValid(aggtup))
	{
		elog(ERROR, "cache lookup failed for %d", funcOid);
	}
	Form_pg_aggregate agg = (Form_pg_aggregate) GETSTRUCT(aggtup);

	if (AGGKIND_IS_ORDERED_SET(agg->aggkind))
	{
		insertorderbyat = agg->aggnumdirectargs;
	}

	/*
	 * For zero-argument aggregate, write * in place of the list of arguments
	 */
	if (numargs == 0)
	{
		appendStringInfo(&buf, "*");
	}

	for (i = 0; i < numargs; i++)
	{
		Oid argtype = argtypes[i];
		char *argname = argnames ? argnames[i] : NULL;
		char argmode = argmodes ? argmodes[i] : PROARGMODE_IN;
		const char *modename;

		switch (argmode)
		{
			case PROARGMODE_IN:
			{
				modename = "";
				break;
			}

			case PROARGMODE_VARIADIC:
			{
				modename = "VARIADIC ";
				break;
			}

			default:
			{
				elog(ERROR, "unexpected parameter mode '%c'", argmode);
				modename = NULL;
				break;
			}
		}

		inputargno++;       /* this is a 1-based counter */
		if (argsprinted == insertorderbyat)
		{
			appendStringInfoString(&buf, " ORDER BY ");
		}
		else if (argsprinted)
		{
			appendStringInfoString(&buf, ", ");
		}

		appendStringInfoString(&buf, modename);

		if (argname && argname[0])
		{
			appendStringInfo(&buf, "%s ", quote_identifier(argname));
		}

		appendStringInfoString(&buf, format_type_be_qualified(argtype));

		argsprinted++;

		/* nasty hack: print the last arg twice for variadic ordered-set agg */
		if (argsprinted == insertorderbyat && i == numargs - 1)
		{
			i--;
		}
	}

	appendStringInfo(&buf, ") (STYPE = %s,SFUNC = %s",
					 format_type_be_qualified(agg->aggtranstype),
					 quote_qualified_func_name(agg->aggtransfn));

	if (agg->aggtransspace != 0)
	{
		appendStringInfo(&buf, ", SSPACE = %d", agg->aggtransspace);
	}

	if (agg->aggfinalfn != InvalidOid)
	{
		const char *finalmodifystring = NULL;
		switch (agg->aggfinalmodify)
		{
			case AGGMODIFY_READ_ONLY:
			{
				finalmodifystring = "READ_ONLY";
				break;
			}

			case AGGMODIFY_SHAREABLE:
			{
				finalmodifystring = "SHAREABLE";
				break;
			}

			case AGGMODIFY_READ_WRITE:
			{
				finalmodifystring = "READ_WRITE";
				break;
			}
		}

		appendStringInfo(&buf, ", FINALFUNC = %s",
						 quote_qualified_func_name(agg->aggfinalfn));

		if (finalmodifystring != NULL)
		{
			appendStringInfo(&buf, ", FINALFUNC_MODIFY = %s", finalmodifystring);
		}

		if (agg->aggfinalextra)
		{
			appendStringInfoString(&buf, ", FINALFUNC_EXTRA");
		}
	}

	if (agg->aggmtransspace != 0)
	{
		appendStringInfo(&buf, ", MSSPACE = %d", agg->aggmtransspace);
	}

	if (agg->aggmfinalfn)
	{
		const char *mfinalmodifystring = NULL;
		switch (agg->aggfinalmodify)
		{
			case AGGMODIFY_READ_ONLY:
			{
				mfinalmodifystring = "READ_ONLY";
				break;
			}

			case AGGMODIFY_SHAREABLE:
			{
				mfinalmodifystring = "SHAREABLE";
				break;
			}

			case AGGMODIFY_READ_WRITE:
			{
				mfinalmodifystring = "READ_WRITE";
				break;
			}
		}

		appendStringInfo(&buf, ", MFINALFUNC = %s",
						 quote_qualified_func_name(agg->aggmfinalfn));

		if (mfinalmodifystring != NULL)
		{
			appendStringInfo(&buf, ", MFINALFUNC_MODIFY = %s", mfinalmodifystring);
		}

		if (agg->aggmfinalextra)
		{
			appendStringInfoString(&buf, ", MFINALFUNC_EXTRA");
		}
	}

	if (agg->aggmtransfn)
	{
		appendStringInfo(&buf, ", MSFUNC = %s",
						 quote_qualified_func_name(agg->aggmtransfn));

		if (agg->aggmtranstype)
		{
			appendStringInfo(&buf, ", MSTYPE = %s",
							 format_type_be_qualified(agg->aggmtranstype));
		}
	}

	if (agg->aggtransspace != 0)
	{
		appendStringInfo(&buf, ", SSPACE = %d", agg->aggtransspace);
	}

	if (agg->aggminvtransfn)
	{
		appendStringInfo(&buf, ", MINVFUNC = %s",
						 quote_qualified_func_name(agg->aggminvtransfn));
	}

	if (agg->aggcombinefn)
	{
		appendStringInfo(&buf, ", COMBINEFUNC = %s",
						 quote_qualified_func_name(agg->aggcombinefn));
	}

	if (agg->aggserialfn)
	{
		appendStringInfo(&buf, ", SERIALFUNC = %s",
						 quote_qualified_func_name(agg->aggserialfn));
	}

	if (agg->aggdeserialfn)
	{
		appendStringInfo(&buf, ", DESERIALFUNC = %s",
						 quote_qualified_func_name(agg->aggdeserialfn));
	}

	if (agg->aggsortop != InvalidOid)
	{
		appendStringInfo(&buf, ", SORTOP = %s",
						 generate_operator_name(agg->aggsortop, argtypes[0],
												argtypes[0]));
	}

	{
		const char *parallelstring = NULL;
		switch (proc->proparallel)
		{
			case PROPARALLEL_SAFE:
			{
				parallelstring = "SAFE";
				break;
			}

			case PROPARALLEL_RESTRICTED:
			{
				parallelstring = "RESTRICTED";
				break;
			}

			case PROPARALLEL_UNSAFE:
			{
				break;
			}

			default:
			{
				elog(WARNING, "Unknown parallel option, ignoring: %c", proc->proparallel);
				break;
			}
		}

		if (parallelstring != NULL)
		{
			appendStringInfo(&buf, ", PARALLEL = %s", parallelstring);
		}
	}

	{
		bool isNull = false;
		Datum textInitVal = SysCacheGetAttr(AGGFNOID, aggtup,
											Anum_pg_aggregate_agginitval,
											&isNull);
		if (!isNull)
		{
			char *strInitVal = TextDatumGetCString(textInitVal);
			char *strInitValQuoted = quote_literal_cstr(strInitVal);

			appendStringInfo(&buf, ", INITCOND = %s", strInitValQuoted);

			pfree(strInitValQuoted);
			pfree(strInitVal);
		}
	}

	{
		bool isNull = false;
		Datum textInitVal = SysCacheGetAttr(AGGFNOID, aggtup,
											Anum_pg_aggregate_aggminitval,
											&isNull);
		if (!isNull)
		{
			char *strInitVal = TextDatumGetCString(textInitVal);
			char *strInitValQuoted = quote_literal_cstr(strInitVal);

			appendStringInfo(&buf, ", MINITCOND = %s", strInitValQuoted);

			pfree(strInitValQuoted);
			pfree(strInitVal);
		}
	}

	if (agg->aggkind == AGGKIND_HYPOTHETICAL)
	{
		appendStringInfoString(&buf, ", HYPOTHETICAL");
	}

	appendStringInfoChar(&buf, ')');

	ReleaseSysCache(aggtup);
	ReleaseSysCache(proctup);

	return buf.data;
}


/*
 * ShouldPropagateCreateFunction tests if we need to propagate a CREATE FUNCTION
 * statement.
 */
static bool
ShouldPropagateCreateFunction(CreateFunctionStmt *stmt)
{
	if (!ShouldPropagate())
	{
		return false;
	}

	if (!ShouldPropagateCreateInCoordinatedTransction())
	{
		return false;
	}

	return true;
}


/*
 * ShouldPropagateAlterFunction returns, based on the address of a function, if alter
 * statements targeting the function should be propagated.
 */
static bool
ShouldPropagateAlterFunction(const ObjectAddress *address)
{
	if (creating_extension)
	{
		/*
		 * extensions should be created separately on the workers, functions cascading
		 * from an extension should therefore not be propagated.
		 */
		return false;
	}

	if (!EnableMetadataSync)
	{
		/*
		 * we are configured to disable object propagation, should not propagate anything
		 */
		return false;
	}

	if (!IsObjectDistributed(address))
	{
		/* do not propagate alter function for non-distributed functions */
		return false;
	}

	return true;
}


/*
 * PreprocessCreateFunctionStmt is called during the planning phase for CREATE [OR REPLACE]
 * FUNCTION before it is created on the local node internally.
 *
 * Since we use pg_get_functiondef to get the ddl command we actually do not do any
 * planning here, instead we defer the plan creation to the postprocessing step.
 *
 * Instead we do our basic housekeeping where we make sure we are on the coordinator and
 * can propagate the function in sequential mode.
 */
List *
PreprocessCreateFunctionStmt(Node *node, const char *queryString,
							 ProcessUtilityContext processUtilityContext)
{
	CreateFunctionStmt *stmt = castNode(CreateFunctionStmt, node);

	if (!ShouldPropagateCreateFunction(stmt))
	{
		return NIL;
	}

	EnsureCoordinator();

	EnsureSequentialMode(OBJECT_FUNCTION);

	/*
	 * ddl jobs will be generated during the postprocessing phase as we need the function to
	 * be updated in the catalog to get its sql representation
	 */
	return NIL;
}


/*
 * PostprocessCreateFunctionStmt actually creates the plan we need to execute for function
 * propagation. This is the downside of using pg_get_functiondef to get the sql statement.
 *
 * If function depends on any non-distributed relation (except sequence and composite type),
 * Citus can not distribute it. In order to not to prevent users from creating local
 * functions on the coordinator WARNING message will be sent to the customer about the case
 * instead of erroring out.
 *
 * Besides creating the plan we also make sure all (new) dependencies of the function are
 * created on all nodes.
 */
List *
PostprocessCreateFunctionStmt(Node *node, const char *queryString)
{
	CreateFunctionStmt *stmt = castNode(CreateFunctionStmt, node);

	if (!ShouldPropagateCreateFunction(stmt))
	{
		return NIL;
	}

	ObjectAddress functionAddress = GetObjectAddressFromParseTree((Node *) stmt, false);

	if (IsObjectAddressOwnedByExtension(&functionAddress, NULL))
	{
		return NIL;
	}

	/* If the function has any unsupported dependency, create it locally */
	DeferredErrorMessage *errMsg = DeferErrorIfHasUnsupportedDependency(&functionAddress);

	if (errMsg != NULL)
	{
		RaiseDeferredError(errMsg, WARNING);
		return NIL;
	}

	EnsureDependenciesExistOnAllNodes(&functionAddress);

	List *commands = list_make1(DISABLE_DDL_PROPAGATION);
	commands = list_concat(commands, CreateFunctionDDLCommandsIdempotent(
							   &functionAddress));
	commands = list_concat(commands, list_make1(ENABLE_DDL_PROPAGATION));

	return NodeDDLTaskList(NON_COORDINATOR_NODES, commands);
}


/*
 * CreateFunctionStmtObjectAddress returns the ObjectAddress for the subject of the
 * CREATE [OR REPLACE] FUNCTION statement. If missing_ok is false it will error with the
 * normal postgres error for unfound functions.
 */
ObjectAddress
CreateFunctionStmtObjectAddress(Node *node, bool missing_ok)
{
	CreateFunctionStmt *stmt = castNode(CreateFunctionStmt, node);
	ObjectType objectType = OBJECT_FUNCTION;

	if (stmt->is_procedure)
	{
		objectType = OBJECT_PROCEDURE;
	}

	ObjectWithArgs *objectWithArgs = makeNode(ObjectWithArgs);
	objectWithArgs->objname = stmt->funcname;

	FunctionParameter *funcParam = NULL;
	foreach_ptr(funcParam, stmt->parameters)
	{
		if (ShouldAddFunctionSignature(funcParam->mode))
		{
			objectWithArgs->objargs = lappend(objectWithArgs->objargs,
											  funcParam->argType);
		}
	}

	return FunctionToObjectAddress(objectType, objectWithArgs, missing_ok);
}


/*
 * DefineAggregateStmtObjectAddress finds the ObjectAddress for the composite type described
 * by the DefineStmtObjectAddress. If missing_ok is false this function throws an error if the
 * aggregate does not exist.
 *
 * objectId in the address can be invalid if missing_ok was set to true.
 */
ObjectAddress
DefineAggregateStmtObjectAddress(Node *node, bool missing_ok)
{
	DefineStmt *stmt = castNode(DefineStmt, node);

	Assert(stmt->kind == OBJECT_AGGREGATE);

	ObjectWithArgs *objectWithArgs = makeNode(ObjectWithArgs);
	objectWithArgs->objname = stmt->defnames;

	if (stmt->args != NIL)
	{
		FunctionParameter *funcParam = NULL;
		foreach_ptr(funcParam, linitial(stmt->args))
		{
			objectWithArgs->objargs = lappend(objectWithArgs->objargs,
											  funcParam->argType);
		}
	}
	else
	{
		DefElem *defItem = NULL;
		foreach_ptr(defItem, stmt->definition)
		{
			/*
			 * If no explicit args are given, pg includes basetype in the signature.
			 * If the basetype given is a type, like int4, we should include it in the
			 * signature. In that case, defItem->arg would be a TypeName.
			 * If the basetype given is a string, like "ANY", we shouldn't include it.
			 */
			if (strcmp(defItem->defname, "basetype") == 0 && IsA(defItem->arg, TypeName))
			{
				objectWithArgs->objargs = lappend(objectWithArgs->objargs,
												  defItem->arg);
			}
		}
	}

	return FunctionToObjectAddress(OBJECT_AGGREGATE, objectWithArgs, missing_ok);
}


/*
 * PreprocessAlterFunctionStmt is invoked for alter function statements with actions. Here we
 * plan the jobs to be executed on the workers for functions that have been distributed in
 * the cluster.
 */
List *
PreprocessAlterFunctionStmt(Node *node, const char *queryString,
							ProcessUtilityContext processUtilityContext)
{
	AlterFunctionStmt *stmt = castNode(AlterFunctionStmt, node);
	AssertObjectTypeIsFunctional(stmt->objtype);

	ObjectAddress address = GetObjectAddressFromParseTree((Node *) stmt, false);
	if (!ShouldPropagateAlterFunction(&address))
	{
		return NIL;
	}

	EnsureCoordinator();
	ErrorIfUnsupportedAlterFunctionStmt(stmt);
	EnsureSequentialMode(OBJECT_FUNCTION);
	QualifyTreeNode((Node *) stmt);
	const char *sql = DeparseTreeNode((Node *) stmt);

	List *commands = list_make3(DISABLE_DDL_PROPAGATION,
								(void *) sql,
								ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(NON_COORDINATOR_NODES, commands);
}


/*
 * PreprocessAlterFunctionDependsStmt is called during the planning phase of an
 * ALTER FUNCION ... DEPENDS ON EXTENSION ... statement. Since functions depending on
 * extensions are assumed to be Owned by an extension we assume the extension to keep the
 * function in sync.
 *
 * If we would allow users to create a dependency between a distributed function and an
 * extension our pruning logic for which objects to distribute as dependencies of other
 * objects will change significantly which could cause issues adding new workers. Hence we
 * don't allow this dependency to be created.
 */
List *
PreprocessAlterFunctionDependsStmt(Node *node, const char *queryString,
								   ProcessUtilityContext processUtilityContext)
{
	AlterObjectDependsStmt *stmt = castNode(AlterObjectDependsStmt, node);
	AssertObjectTypeIsFunctional(stmt->objectType);

	if (creating_extension)
	{
		/*
		 * extensions should be created separately on the workers, types cascading from an
		 * extension should therefore not be propagated here.
		 */
		return NIL;
	}

	if (!EnableMetadataSync)
	{
		/*
		 * we are configured to disable object propagation, should not propagate anything
		 */
		return NIL;
	}

	ObjectAddress address = GetObjectAddressFromParseTree((Node *) stmt, true);
	if (!IsObjectDistributed(&address))
	{
		return NIL;
	}

	/*
	 * Distributed objects should not start depending on an extension, this will break
	 * the dependency resolving mechanism we use to replicate distributed objects to new
	 * workers
	 */

	const char *functionName =
		getObjectIdentity_compat(&address, /* missingOk: */ false);
	ereport(ERROR, (errmsg("distrtibuted functions are not allowed to depend on an "
						   "extension"),
					errdetail("Function \"%s\" is already distributed. Functions from "
							  "extensions are expected to be created on the workers by "
							  "the extension they depend on.", functionName)));
}


/*
 * AlterFunctionDependsStmtObjectAddress resolves the ObjectAddress of the function that
 * is the subject of an ALTER FUNCTION ... DEPENS ON EXTENSION ... statement. If
 * missing_ok is set to false the lookup will raise an error.
 */
ObjectAddress
AlterFunctionDependsStmtObjectAddress(Node *node, bool missing_ok)
{
	AlterObjectDependsStmt *stmt = castNode(AlterObjectDependsStmt, node);
	AssertObjectTypeIsFunctional(stmt->objectType);

	return FunctionToObjectAddress(stmt->objectType,
								   castNode(ObjectWithArgs, stmt->object), missing_ok);
}


/*
 * AlterFunctionStmtObjectAddress returns the ObjectAddress of the subject in the
 * AlterFunctionStmt. If missing_ok is set to false an error will be raised if postgres
 * was unable to find the function/procedure that was the target of the statement.
 */
ObjectAddress
AlterFunctionStmtObjectAddress(Node *node, bool missing_ok)
{
	AlterFunctionStmt *stmt = castNode(AlterFunctionStmt, node);
	return FunctionToObjectAddress(stmt->objtype, stmt->func, missing_ok);
}


/*
 * RenameFunctionStmtObjectAddress returns the ObjectAddress of the function that is the
 * subject of the RenameStmt. Errors if missing_ok is false.
 */
ObjectAddress
RenameFunctionStmtObjectAddress(Node *node, bool missing_ok)
{
	RenameStmt *stmt = castNode(RenameStmt, node);
	return FunctionToObjectAddress(stmt->renameType,
								   castNode(ObjectWithArgs, stmt->object), missing_ok);
}


/*
 * AlterFunctionOwnerObjectAddress returns the ObjectAddress of the function that is the
 * subject of the AlterOwnerStmt. Errors if missing_ok is false.
 */
ObjectAddress
AlterFunctionOwnerObjectAddress(Node *node, bool missing_ok)
{
	AlterOwnerStmt *stmt = castNode(AlterOwnerStmt, node);
	return FunctionToObjectAddress(stmt->objectType,
								   castNode(ObjectWithArgs, stmt->object), missing_ok);
}


/*
 * AlterFunctionSchemaStmtObjectAddress returns the ObjectAddress of the function that is
 * the subject of the AlterObjectSchemaStmt. Errors if missing_ok is false.
 *
 * This could be called both before or after it has been applied locally. It will look in
 * the old schema first, if the function cannot be found in that schema it will look in
 * the new schema. Errors if missing_ok is false and the type cannot be found in either of
 * the schemas.
 */
ObjectAddress
AlterFunctionSchemaStmtObjectAddress(Node *node, bool missing_ok)
{
	AlterObjectSchemaStmt *stmt = castNode(AlterObjectSchemaStmt, node);
	AssertObjectTypeIsFunctional(stmt->objectType);

	ObjectWithArgs *objectWithArgs = castNode(ObjectWithArgs, stmt->object);
	Oid funcOid = LookupFuncWithArgs(stmt->objectType, objectWithArgs, true);
	List *names = objectWithArgs->objname;

	if (funcOid == InvalidOid)
	{
		/*
		 * couldn't find the function, might have already been moved to the new schema, we
		 * construct a new objname that uses the new schema to search in.
		 */

		/* the name of the function is the last in the list of names */
		String *funcNameStr = lfirst(list_tail(names));
		List *newNames = list_make2(makeString(stmt->newschema), funcNameStr);

		/*
		 * we don't error here either, as the error would be not a good user facing
		 * error if the type didn't exist in the first place.
		 */
		objectWithArgs->objname = newNames;
		funcOid = LookupFuncWithArgs(stmt->objectType, objectWithArgs, true);
		objectWithArgs->objname = names; /* restore the original names */

		/*
		 * if the function is still invalid we couldn't find the function, cause postgres
		 * to error by preforming a lookup once more. Since we know the
		 */
		if (!missing_ok && funcOid == InvalidOid)
		{
			/*
			 * this will most probably throw an error, unless for some reason the function
			 * has just been created (if possible at all). For safety we assign the
			 * funcOid.
			 */
			funcOid = LookupFuncWithArgs(stmt->objectType, objectWithArgs,
										 missing_ok);
		}
	}

	ObjectAddress address = { 0 };
	ObjectAddressSet(address, ProcedureRelationId, funcOid);

	return address;
}


/*
 * GenerateBackupNameForProcCollision generates a new proc name for an existing proc. The
 * name is generated in such a way that the new name doesn't overlap with an existing proc
 * by adding a suffix with incrementing number after the new name.
 */
char *
GenerateBackupNameForProcCollision(const ObjectAddress *address)
{
	char *newName = palloc0(NAMEDATALEN);
	char suffix[NAMEDATALEN] = { 0 };
	int count = 0;
	String *namespace = makeString(get_namespace_name(get_func_namespace(
														  address->objectId)));
	char *baseName = get_func_name(address->objectId);
	int baseLength = strlen(baseName);
	Oid *argtypes = NULL;
	char **argnames = NULL;
	char *argmodes = NULL;
	HeapTuple proctup = SearchSysCache1(PROCOID, address->objectId);

	if (!HeapTupleIsValid(proctup))
	{
		elog(ERROR, "citus cache lookup failed.");
	}

	int numargs = get_func_arg_info(proctup, &argtypes, &argnames, &argmodes);
	ReleaseSysCache(proctup);

	while (true)
	{
		int suffixLength = SafeSnprintf(suffix, NAMEDATALEN - 1, "(citus_backup_%d)",
										count);

		/* trim the base name at the end to leave space for the suffix and trailing \0 */
		baseLength = Min(baseLength, NAMEDATALEN - suffixLength - 1);

		/* clear newName before copying the potentially trimmed baseName and suffix */
		memset(newName, 0, NAMEDATALEN);
		strncpy_s(newName, NAMEDATALEN, baseName, baseLength);
		strncpy_s(newName + baseLength, NAMEDATALEN - baseLength, suffix,
				  suffixLength);

		List *newProcName = list_make2(namespace, makeString(newName));

		/* don't need to rename if the input arguments don't match */
		FuncCandidateList clist = FuncnameGetCandidates_compat(newProcName, numargs, NIL,
															   false, false, false, true);
		for (; clist; clist = clist->next)
		{
			if (memcmp(clist->args, argtypes, sizeof(Oid) * numargs) == 0)
			{
				break;
			}
		}

		if (!clist)
		{
			return newName;
		}

		count++;
	}
}


/*
 * ObjectWithArgsFromOid returns the corresponding ObjectWithArgs node for a given pg_proc oid
 */
ObjectWithArgs *
ObjectWithArgsFromOid(Oid funcOid)
{
	ObjectWithArgs *objectWithArgs = makeNode(ObjectWithArgs);
	List *objargs = NIL;
	Oid *argTypes = NULL;
	char **argNames = NULL;
	char *argModes = NULL;
	HeapTuple proctup = SearchSysCache1(PROCOID, funcOid);

	if (!HeapTupleIsValid(proctup))
	{
		elog(ERROR, "citus cache lookup failed.");
	}

	int numargs = get_func_arg_info(proctup, &argTypes, &argNames, &argModes);

	objectWithArgs->objname = list_make2(
		makeString(get_namespace_name(get_func_namespace(funcOid))),
		makeString(get_func_name(funcOid))
		);

	for (int i = 0; i < numargs; i++)
	{
		if (argModes == NULL || ShouldAddFunctionSignature(argModes[i]))
		{
			objargs = lappend(objargs, makeTypeNameFromOid(argTypes[i], -1));
		}
	}
	objectWithArgs->objargs = objargs;

	ReleaseSysCache(proctup);

	return objectWithArgs;
}


/*
 * ShouldAddFunctionSignature takes a FunctionParameterMode and returns true if it should
 * be included in the function signature. Returns false otherwise.
 */
static bool
ShouldAddFunctionSignature(FunctionParameterMode mode)
{
	/* only input parameters should be added to the generated signature */
	switch (mode)
	{
		case FUNC_PARAM_IN:
		case FUNC_PARAM_INOUT:
		case FUNC_PARAM_VARIADIC:
		{
			return true;
		}

		case FUNC_PARAM_OUT:
		case FUNC_PARAM_TABLE:
		{
			return false;
		}

		default:
			return true;
	}
}


/*
 * FunctionToObjectAddress returns the ObjectAddress of a Function, Procedure or
 * Aggregate based on its type and ObjectWithArgs describing the
 * Function/Procedure/Aggregate. If missing_ok is set to false an error will be
 * raised by postgres explaining the Function/Procedure could not be found.
 */
static ObjectAddress
FunctionToObjectAddress(ObjectType objectType, ObjectWithArgs *objectWithArgs,
						bool missing_ok)
{
	AssertObjectTypeIsFunctional(objectType);

	Oid funcOid = LookupFuncWithArgs(objectType, objectWithArgs, missing_ok);
	ObjectAddress address = { 0 };
	ObjectAddressSet(address, ProcedureRelationId, funcOid);

	return address;
}


/*
 * ErrorIfUnsupportedAlterFunctionStmt raises an error if the AlterFunctionStmt contains a
 * construct that is not supported to be altered on a distributed function. It is assumed
 * the statement passed in is already tested to be targeting a distributed function, and
 * will only execute the checks to error on unsupported constructs.
 *
 * Unsupported Constructs:
 *  - ALTER FUNCTION ... SET ... FROM CURRENT
 */
static void
ErrorIfUnsupportedAlterFunctionStmt(AlterFunctionStmt *stmt)
{
	DefElem *action = NULL;
	foreach_ptr(action, stmt->actions)
	{
		if (strcmp(action->defname, "set") == 0)
		{
			VariableSetStmt *setStmt = castNode(VariableSetStmt, action->arg);
			if (setStmt->kind == VAR_SET_CURRENT)
			{
				/* check if the set action is a SET ... FROM CURRENT */
				ereport(ERROR, (errmsg("unsupported ALTER FUNCTION ... SET ... FROM "
									   "CURRENT for a distributed function"),
								errhint("SET FROM CURRENT is not supported for "
										"distributed functions, instead use the SET ... "
										"TO ... syntax with a constant value.")));
			}
		}
	}
}


/* returns the quoted qualified name of a given function oid */
static char *
quote_qualified_func_name(Oid funcOid)
{
	return quote_qualified_identifier(
		get_namespace_name(get_func_namespace(funcOid)),
		get_func_name(funcOid));
}


/*
 * EnsureExtensionFuncionCanBeCreated checks if the dependent objects
 * (including extension) exists on all nodes, if not, creates them. In
 * addition, it also checks if distribution argument is passed.
 */
static void
EnsureExtensionFunctionCanBeDistributed(const ObjectAddress functionAddress,
										const ObjectAddress extensionAddress,
										char *distributionArgumentName)
{
	if (CitusExtensionObject(&extensionAddress))
	{
		/*
		 * Citus extension is a special case. It's the extension that
		 * provides the 'distributed capabilities' in the first place.
		 * Trying to distribute it's own function(s) doesn't make sense.
		 */
		ereport(ERROR, (errmsg("Citus extension functions(%s) "
							   "cannot be distributed.",
							   get_func_name(functionAddress.objectId))));
	}

	/*
	 * Distributing functions from extensions has the most benefit when
	 * distribution argument is specified.
	 */
	if (distributionArgumentName == NULL)
	{
		ereport(ERROR, (errmsg("Extension functions(%s) "
							   "without distribution argument "
							   "are not supported.",
							   get_func_name(functionAddress.objectId))));
	}

	/*
	 * Ensure corresponding extension is in pg_dist_object.
	 * Functions owned by an extension are depending internally on that extension,
	 * hence EnsureDependenciesExistOnAllNodes() creates the extension, which in
	 * turn creates the function, and thus we don't have to create it ourself like
	 * we do for non-extension functions.
	 */
	ereport(DEBUG1, (errmsg("Extension(%s) owning the "
							"function(%s) is not distributed, "
							"attempting to propogate the extension",
							get_extension_name(extensionAddress.objectId),
							get_func_name(functionAddress.objectId))));

	EnsureDependenciesExistOnAllNodes(&functionAddress);
}


/*
 * PreprocessGrantOnFunctionStmt is executed before the statement is applied to the local
 * postgres instance.
 *
 * In this stage we can prepare the commands that need to be run on all workers to grant
 * on distributed functions, procedures, routines.
 */
List *
PreprocessGrantOnFunctionStmt(Node *node, const char *queryString,
							  ProcessUtilityContext processUtilityContext)
{
	GrantStmt *stmt = castNode(GrantStmt, node);
	Assert(isFunction(stmt->objtype));

	List *distributedFunctions = FilterDistributedFunctions(stmt);

	if (list_length(distributedFunctions) == 0 || !ShouldPropagate())
	{
		return NIL;
	}

	EnsureCoordinator();

	List *grantFunctionList = NIL;
	ObjectAddress *functionAddress = NULL;
	foreach_ptr(functionAddress, distributedFunctions)
	{
		ObjectWithArgs *distFunction = ObjectWithArgsFromOid(
			functionAddress->objectId);
		grantFunctionList = lappend(grantFunctionList, distFunction);
	}

	List *originalObjects = stmt->objects;
	GrantTargetType originalTargtype = stmt->targtype;

	stmt->objects = grantFunctionList;
	stmt->targtype = ACL_TARGET_OBJECT;

	char *sql = DeparseTreeNode((Node *) stmt);

	stmt->objects = originalObjects;
	stmt->targtype = originalTargtype;

	List *commandList = list_make3(DISABLE_DDL_PROPAGATION,
								   (void *) sql,
								   ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(NON_COORDINATOR_NODES, commandList);
}


/*
 * PostprocessGrantOnFunctionStmt makes sure dependencies of each
 * distributed function in the statement exist on all nodes
 */
List *
PostprocessGrantOnFunctionStmt(Node *node, const char *queryString)
{
	GrantStmt *stmt = castNode(GrantStmt, node);

	List *distributedFunctions = FilterDistributedFunctions(stmt);

	if (list_length(distributedFunctions) == 0)
	{
		return NIL;
	}

	ObjectAddress *functionAddress = NULL;
	foreach_ptr(functionAddress, distributedFunctions)
	{
		EnsureDependenciesExistOnAllNodes(functionAddress);
	}
	return NIL;
}


/*
 * FilterDistributedFunctions determines and returns a list of distributed functions
 * ObjectAddress-es from given grant statement.
 */
static List *
FilterDistributedFunctions(GrantStmt *grantStmt)
{
	List *grantFunctionList = NIL;

	bool grantOnFunctionCommand = (grantStmt->targtype == ACL_TARGET_OBJECT &&
								   isFunction(grantStmt->objtype));
	bool grantAllFunctionsOnSchemaCommand = (grantStmt->targtype ==
											 ACL_TARGET_ALL_IN_SCHEMA &&
											 isFunction(grantStmt->objtype));

	/* we are only interested in function/procedure/routine level grants */
	if (!grantOnFunctionCommand && !grantAllFunctionsOnSchemaCommand)
	{
		return NIL;
	}

	if (grantAllFunctionsOnSchemaCommand)
	{
		List *distributedFunctionList = DistributedFunctionList();
		ObjectAddress *distributedFunction = NULL;
		List *namespaceOidList = NIL;

		/* iterate over all namespace names provided to get their oid's */
		String *namespaceValue = NULL;
		foreach_ptr(namespaceValue, grantStmt->objects)
		{
			char *nspname = strVal(namespaceValue);
			bool missing_ok = false;
			Oid namespaceOid = get_namespace_oid(nspname, missing_ok);
			namespaceOidList = list_append_unique_oid(namespaceOidList, namespaceOid);
		}

		/*
		 * iterate over all distributed functions to filter the ones
		 * that belong to one of the namespaces from above
		 */
		foreach_ptr(distributedFunction, distributedFunctionList)
		{
			Oid namespaceOid = get_func_namespace(distributedFunction->objectId);

			/*
			 * if this distributed function's schema is one of the schemas
			 * specified in the GRANT .. ALL FUNCTIONS IN SCHEMA ..
			 * add it to the list
			 */
			if (list_member_oid(namespaceOidList, namespaceOid))
			{
				grantFunctionList = lappend(grantFunctionList, distributedFunction);
			}
		}
	}
	else
	{
		bool missingOk = false;
		ObjectWithArgs *objectWithArgs = NULL;
		foreach_ptr(objectWithArgs, grantStmt->objects)
		{
			ObjectAddress *functionAddress = palloc0(sizeof(ObjectAddress));
			functionAddress->classId = ProcedureRelationId;
			functionAddress->objectId = LookupFuncWithArgs(grantStmt->objtype,
														   objectWithArgs,
														   missingOk);
			functionAddress->objectSubId = 0;

			/*
			 * if this function from GRANT .. ON FUNCTION .. is a distributed
			 * function, add it to the list
			 */
			if (IsObjectDistributed(functionAddress))
			{
				grantFunctionList = lappend(grantFunctionList, functionAddress);
			}
		}
	}
	return grantFunctionList;
}
