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
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "miscadmin.h"
#include "funcapi.h"

#include "distributed/pg_version_constants.h"

#if PG_VERSION_NUM >= PG_VERSION_12
#include "access/genam.h"
#endif
#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/pg_aggregate.h"
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
#include "distributed/master_metadata_utility.h"
#include "distributed/master_protocol.h"
#include "distributed/metadata/distobject.h"
#include "distributed/metadata/pg_dist_object.h"
#include "distributed/metadata_sync.h"
#include "distributed/multi_executor.h"
#include "distributed/relation_access_tracking.h"
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
static char * GetAggregateDDLCommand(const RegProcedure funcOid, bool useCreateOrReplace);
static char * GetFunctionAlterOwnerCommand(const RegProcedure funcOid);
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
static bool ShouldPropagateCreateFunction(CreateFunctionStmt *stmt);
static bool ShouldPropagateAlterFunction(const ObjectAddress *address);
static ObjectAddress FunctionToObjectAddress(ObjectType objectType,
											 ObjectWithArgs *objectWithArgs,
											 bool missing_ok);
static void ErrorIfUnsupportedAlterFunctionStmt(AlterFunctionStmt *stmt);
static void ErrorIfFunctionDependsOnExtension(const ObjectAddress *functionAddress);
static char * quote_qualified_func_name(Oid funcOid);


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
	}

	EnsureCoordinator();
	EnsureFunctionOwner(funcOid);

	ObjectAddressSet(functionAddress, ProcedureRelationId, funcOid);
	ErrorIfFunctionDependsOnExtension(&functionAddress);

	/*
	 * when we allow propagation within a transaction block we should make sure to only
	 * allow this in sequential mode
	 */
	EnsureSequentialModeForFunctionDDL();

	EnsureDependenciesExistOnAllNodes(&functionAddress);

	const char *createFunctionSQL = GetFunctionDDLCommand(funcOid, true);
	const char *alterFunctionOwnerSQL = GetFunctionAlterOwnerCommand(funcOid);
	initStringInfo(&ddlCommand);
	appendStringInfo(&ddlCommand, "%s;%s", createFunctionSQL, alterFunctionOwnerSQL);
	SendCommandToWorkersAsUser(ALL_WORKERS, CurrentUserName(), ddlCommand.data);

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
	Relation pgDistColocation = heap_open(DistColocationRelationId(), ShareLock);

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
		else if (ReplicationModel == REPLICATION_MODEL_COORDINATOR)
		{
			/* streaming replication model is required for metadata syncing */
			ereport(ERROR, (errmsg("cannot create a function with a distribution "
								   "argument when citus.replication_model is "
								   "'statement'"),
							errhint("Set citus.replication_model to 'streaming' "
									"before creating distributed tables")));
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
	heap_close(pgDistColocation, NoLock);

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
	char sourceDistributionMethod = sourceTableEntry->partitionMethod;
	char sourceReplicationModel = sourceTableEntry->replicationModel;

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
								"citus.replication_model = 'streaming'")));
	}

	/*
	 * If the types are the same, we're good. If not, we still check if there
	 * is any coercion path between the types.
	 */
	Var *sourceDistributionColumn = ForceDistPartitionKey(sourceRelationId);
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
 */
static void
UpdateFunctionDistributionInfo(const ObjectAddress *distAddress,
							   int *distribution_argument_index,
							   int *colocationId)
{
	const bool indexOK = true;

	ScanKeyData scanKey[3];
	Datum values[Natts_pg_dist_object];
	bool isnull[Natts_pg_dist_object];
	bool replace[Natts_pg_dist_object];

	Relation pgDistObjectRel = heap_open(DistObjectRelationId(), RowExclusiveLock);
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

	heapTuple = heap_modify_tuple(heapTuple, tupleDescriptor, values, isnull, replace);

	CatalogTupleUpdate(pgDistObjectRel, &heapTuple->t_self, heapTuple);

	CitusInvalidateRelcacheByRelid(DistObjectRelationId());

	CommandCounterIncrement();

	systable_endscan(scanDescriptor);

	heap_close(pgDistObjectRel, NoLock);
}


/*
 * GetFunctionDDLCommand returns the complete "CREATE OR REPLACE FUNCTION ..." statement for
 * the specified function followed by "ALTER FUNCTION .. SET OWNER ..".
 *
 * useCreateOrReplace is ignored for non-aggregate functions.
 */
char *
GetFunctionDDLCommand(const RegProcedure funcOid, bool useCreateOrReplace)
{
	OverrideSearchPath *overridePath = NULL;
	char *createFunctionSQL = NULL;

	if (get_func_prokind(funcOid) == PROKIND_AGGREGATE)
	{
		createFunctionSQL = GetAggregateDDLCommand(funcOid, useCreateOrReplace);
	}
	else
	{
		Datum sqlTextDatum = (Datum) 0;

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
	HeapTuple aggtup = NULL;
	Form_pg_aggregate agg = NULL;
	int numargs = 0;
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

#if PG_VERSION_NUM >= PG_VERSION_12
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
#else
	appendStringInfo(&buf, "CREATE AGGREGATE %s(",
					 quote_qualified_identifier(nsp, name));
#endif

	/* Parameters, borrows heavily from print_function_arguments in postgres */
	numargs = get_func_arg_info(proctup, &argtypes, &argnames, &argmodes);

	aggtup = SearchSysCache1(AGGFNOID, funcOid);
	if (!HeapTupleIsValid(aggtup))
	{
		elog(ERROR, "cache lookup failed for %d", funcOid);
	}
	agg = (Form_pg_aggregate) GETSTRUCT(aggtup);

	if (AGGKIND_IS_ORDERED_SET(agg->aggkind))
	{
		insertorderbyat = agg->aggnumdirectargs;
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

#if PG_VERSION_NUM < PG_VERSION_12
	if (useCreateOrReplace)
	{
		return WrapCreateOrReplace(buf.data);
	}
#endif
	return buf.data;
}


/*
 * EnsureSequentialModeForFunctionDDL makes sure that the current transaction is already in
 * sequential mode, or can still safely be put in sequential mode, it errors if that is
 * not possible. The error contains information for the user to retry the transaction with
 * sequential mode set from the beginning.
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
	List *workerList = ActivePrimaryWorkerNodeList(ShareLock);
	bool triggerMetadataSync = false;

	WorkerNode *workerNode = NULL;
	foreach_ptr(workerNode, workerList)
	{
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


/*
 * ShouldPropagateCreateFunction tests if we need to propagate a CREATE FUNCTION
 * statement. We only propagate replace's of distributed functions to keep the function on
 * the workers in sync with the one on the coordinator.
 */
static bool
ShouldPropagateCreateFunction(CreateFunctionStmt *stmt)
{
	if (creating_extension)
	{
		/*
		 * extensions should be created separately on the workers, functions cascading
		 * from an extension should therefore not be propagated.
		 */
		return false;
	}

	if (!EnableDependencyCreation)
	{
		/*
		 * we are configured to disable object propagation, should not propagate anything
		 */
		return false;
	}

	if (!stmt->replace)
	{
		/*
		 * Since we only care for a replace of distributed functions if the statement is
		 * not a replace we are going to ignore.
		 */
		return false;
	}

	/*
	 * Even though its a replace we should accept an non-existing function, it will just
	 * not be distributed
	 */
	ObjectAddress address = GetObjectAddressFromParseTree((Node *) stmt, true);
	if (!IsObjectDistributed(&address))
	{
		/* do not propagate alter function for non-distributed functions */
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

	if (!EnableDependencyCreation)
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
 * FUNCTION. We primarily care for the replace variant of this statement to keep
 * distributed functions in sync. We bail via a check on ShouldPropagateCreateFunction
 * which checks for the OR REPLACE modifier.
 *
 * Since we use pg_get_functiondef to get the ddl command we actually do not do any
 * planning here, instead we defer the plan creation to the processing step.
 *
 * Instead we do our basic housekeeping where we make sure we are on the coordinator and
 * can propagate the function in sequential mode.
 */
List *
PreprocessCreateFunctionStmt(Node *node, const char *queryString)
{
	CreateFunctionStmt *stmt = castNode(CreateFunctionStmt, node);

	if (!ShouldPropagateCreateFunction(stmt))
	{
		return NIL;
	}

	EnsureCoordinator();

	EnsureSequentialModeForFunctionDDL();

	/*
	 * ddl jobs will be generated during the Processing phase as we need the function to
	 * be updated in the catalog to get its sql representation
	 */
	return NIL;
}


/*
 * PostprocessCreateFunctionStmt actually creates the plan we need to execute for function
 * propagation. This is the downside of using pg_get_functiondef to get the sql statement.
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

	ObjectAddress address = GetObjectAddressFromParseTree((Node *) stmt, false);
	EnsureDependenciesExistOnAllNodes(&address);

	List *commands = list_make4(DISABLE_DDL_PROPAGATION,
								GetFunctionDDLCommand(address.objectId, true),
								GetFunctionAlterOwnerCommand(address.objectId),
								ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(ALL_WORKERS, commands);
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
		objectWithArgs->objargs = lappend(objectWithArgs->objargs, funcParam->argType);
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

	FunctionParameter *funcParam = NULL;
	foreach_ptr(funcParam, linitial(stmt->args))
	{
		objectWithArgs->objargs = lappend(objectWithArgs->objargs, funcParam->argType);
	}

	return FunctionToObjectAddress(OBJECT_AGGREGATE, objectWithArgs, missing_ok);
}


/*
 * PreprocessAlterFunctionStmt is invoked for alter function statements with actions. Here we
 * plan the jobs to be executed on the workers for functions that have been distributed in
 * the cluster.
 */
List *
PreprocessAlterFunctionStmt(Node *node, const char *queryString)
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
	EnsureSequentialModeForFunctionDDL();
	QualifyTreeNode((Node *) stmt);
	const char *sql = DeparseTreeNode((Node *) stmt);

	List *commands = list_make3(DISABLE_DDL_PROPAGATION,
								(void *) sql,
								ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(ALL_WORKERS, commands);
}


/*
 * PreprocessRenameFunctionStmt is called when the user is renaming a function. The invocation
 * happens before the statement is applied locally.
 *
 * As the function already exists we have access to the ObjectAddress, this is used to
 * check if it is distributed. If so the rename is executed on all the workers to keep the
 * types in sync across the cluster.
 */
List *
PreprocessRenameFunctionStmt(Node *node, const char *queryString)
{
	RenameStmt *stmt = castNode(RenameStmt, node);
	AssertObjectTypeIsFunctional(stmt->renameType);

	ObjectAddress address = GetObjectAddressFromParseTree((Node *) stmt, false);
	if (!ShouldPropagateAlterFunction(&address))
	{
		return NIL;
	}

	EnsureCoordinator();
	EnsureSequentialModeForFunctionDDL();
	QualifyTreeNode((Node *) stmt);
	const char *sql = DeparseTreeNode((Node *) stmt);

	List *commands = list_make3(DISABLE_DDL_PROPAGATION,
								(void *) sql,
								ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(ALL_WORKERS, commands);
}


/*
 * PreprocessAlterFunctionSchemaStmt is executed before the statement is applied to the local
 * postgres instance.
 *
 * In this stage we can prepare the commands that need to be run on all workers.
 */
List *
PreprocessAlterFunctionSchemaStmt(Node *node, const char *queryString)
{
	AlterObjectSchemaStmt *stmt = castNode(AlterObjectSchemaStmt, node);
	AssertObjectTypeIsFunctional(stmt->objectType);

	ObjectAddress address = GetObjectAddressFromParseTree((Node *) stmt, false);
	if (!ShouldPropagateAlterFunction(&address))
	{
		return NIL;
	}

	EnsureCoordinator();
	EnsureSequentialModeForFunctionDDL();
	QualifyTreeNode((Node *) stmt);
	const char *sql = DeparseTreeNode((Node *) stmt);

	List *commands = list_make3(DISABLE_DDL_PROPAGATION,
								(void *) sql,
								ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(ALL_WORKERS, commands);
}


/*
 * PreprocessAlterTypeOwnerStmt is called for change of owner ship of functions before the owner
 * ship is changed on the local instance.
 *
 * If the function for which the owner is changed is distributed we execute the change on
 * all the workers to keep the type in sync across the cluster.
 */
List *
PreprocessAlterFunctionOwnerStmt(Node *node, const char *queryString)
{
	AlterOwnerStmt *stmt = castNode(AlterOwnerStmt, node);
	AssertObjectTypeIsFunctional(stmt->objectType);

	ObjectAddress address = GetObjectAddressFromParseTree((Node *) stmt, false);
	if (!ShouldPropagateAlterFunction(&address))
	{
		return NIL;
	}

	EnsureCoordinator();
	EnsureSequentialModeForFunctionDDL();
	QualifyTreeNode((Node *) stmt);
	const char *sql = DeparseTreeNode((Node *) stmt);

	List *commands = list_make3(DISABLE_DDL_PROPAGATION,
								(void *) sql,
								ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(ALL_WORKERS, commands);
}


/*
 * PreprocessDropFunctionStmt gets called during the planning phase of a DROP FUNCTION statement
 * and returns a list of DDLJob's that will drop any distributed functions from the
 * workers.
 *
 * The DropStmt could have multiple objects to drop, the list of objects will be filtered
 * to only keep the distributed functions for deletion on the workers. Non-distributed
 * functions will still be dropped locally but not on the workers.
 */
List *
PreprocessDropFunctionStmt(Node *node, const char *queryString)
{
	DropStmt *stmt = castNode(DropStmt, node);
	List *deletingObjectWithArgsList = stmt->objects;
	List *distributedObjectWithArgsList = NIL;
	List *distributedFunctionAddresses = NIL;

	AssertObjectTypeIsFunctional(stmt->removeType);

	if (creating_extension)
	{
		/*
		 * extensions should be created separately on the workers, types cascading from an
		 * extension should therefor not be propagated here.
		 */
		return NIL;
	}

	if (!EnableDependencyCreation)
	{
		/*
		 * we are configured to disable object propagation, should not propagate anything
		 */
		return NIL;
	}


	/*
	 * Our statements need to be fully qualified so we can drop them from the right schema
	 * on the workers
	 */
	QualifyTreeNode((Node *) stmt);

	/*
	 * iterate over all functions to be dropped and filter to keep only distributed
	 * functions.
	 */
	ObjectWithArgs *func = NULL;
	foreach_ptr(func, deletingObjectWithArgsList)
	{
		ObjectAddress address = FunctionToObjectAddress(stmt->removeType, func,
														stmt->missing_ok);

		if (!IsObjectDistributed(&address))
		{
			continue;
		}

		/* collect information for all distributed functions */
		ObjectAddress *addressp = palloc(sizeof(ObjectAddress));
		*addressp = address;
		distributedFunctionAddresses = lappend(distributedFunctionAddresses, addressp);
		distributedObjectWithArgsList = lappend(distributedObjectWithArgsList, func);
	}

	if (list_length(distributedObjectWithArgsList) <= 0)
	{
		/* no distributed functions to drop */
		return NIL;
	}

	/*
	 * managing types can only be done on the coordinator if ddl propagation is on. when
	 * it is off we will never get here. MX workers don't have a notion of distributed
	 * types, so we block the call.
	 */
	EnsureCoordinator();
	EnsureSequentialModeForFunctionDDL();

	/* remove the entries for the distributed objects on dropping */
	ObjectAddress *address = NULL;
	foreach_ptr(address, distributedFunctionAddresses)
	{
		UnmarkObjectDistributed(address);
	}

	/*
	 * Swap the list of objects before deparsing and restore the old list after. This
	 * ensures we only have distributed functions in the deparsed drop statement.
	 */
	DropStmt *stmtCopy = copyObject(stmt);
	stmtCopy->objects = distributedObjectWithArgsList;
	const char *dropStmtSql = DeparseTreeNode((Node *) stmtCopy);

	List *commands = list_make3(DISABLE_DDL_PROPAGATION,
								(void *) dropStmtSql,
								ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(ALL_WORKERS, commands);
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
PreprocessAlterFunctionDependsStmt(Node *node, const char *queryString)
{
	AlterObjectDependsStmt *stmt = castNode(AlterObjectDependsStmt, node);
	AssertObjectTypeIsFunctional(stmt->objectType);

	if (creating_extension)
	{
		/*
		 * extensions should be created separately on the workers, types cascading from an
		 * extension should therefor not be propagated here.
		 */
		return NIL;
	}

	if (!EnableDependencyCreation)
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

	const char *functionName = getObjectIdentity(&address);
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
 * PostprocessAlterFunctionSchemaStmt is executed after the change has been applied locally,
 * we can now use the new dependencies of the function to ensure all its dependencies
 * exist on the workers before we apply the commands remotely.
 */
List *
PostprocessAlterFunctionSchemaStmt(Node *node, const char *queryString)
{
	AlterObjectSchemaStmt *stmt = castNode(AlterObjectSchemaStmt, node);
	AssertObjectTypeIsFunctional(stmt->objectType);

	ObjectAddress address = GetObjectAddressFromParseTree((Node *) stmt, false);
	if (!ShouldPropagateAlterFunction(&address))
	{
		return NIL;
	}

	/* dependencies have changed (schema) let's ensure they exist */
	EnsureDependenciesExistOnAllNodes(&address);

	return NIL;
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
		Value *funcNameStr = lfirst(list_tail(names));
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
	Value *namespace = makeString(get_namespace_name(get_func_namespace(
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
		FuncCandidateList clist = FuncnameGetCandidates(newProcName, numargs, NIL, false,
														false, true);
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
		if (argModes == NULL ||
			argModes[i] != PROARGMODE_OUT || argModes[i] != PROARGMODE_TABLE)
		{
			objargs = lappend(objargs, makeTypeNameFromOid(argTypes[i], -1));
		}
	}
	objectWithArgs->objargs = objargs;

	ReleaseSysCache(proctup);

	return objectWithArgs;
}


/*
 * FunctionToObjectAddress returns the ObjectAddress of a Function or Procedure based on
 * its type and ObjectWithArgs describing the Function/Procedure. If missing_ok is set to
 * false an error will be raised by postgres explaining the Function/Procedure could not
 * be found.
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


/*
 * ErrorIfFunctionDependsOnExtension functions depending on extensions should raise an
 * error informing the user why they can't be distributed.
 */
static void
ErrorIfFunctionDependsOnExtension(const ObjectAddress *functionAddress)
{
	/* captures the extension address during lookup */
	ObjectAddress extensionAddress = { 0 };

	if (IsObjectAddressOwnedByExtension(functionAddress, &extensionAddress))
	{
		char *functionName = getObjectIdentity(functionAddress);
		char *extensionName = getObjectIdentity(&extensionAddress);
		ereport(ERROR, (errmsg("unable to create a distributed function from functions "
							   "owned by an extension"),
						errdetail("Function \"%s\" has a dependency on extension \"%s\". "
								  "Functions depending on an extension cannot be "
								  "distributed. Create the function by creating the "
								  "extension on the workers.", functionName,
								  extensionName)));
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
