/*
 * procedure_metadata.c
 *   Functions to load metadata on distributed stored procedures.
 */
#include "postgres.h"
#include "miscadmin.h"
#include "funcapi.h"


#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup.h"
#include "access/htup_details.h"
#include "access/skey.h"
#include "access/tupmacs.h"
#include "access/xact.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_proc.h"
#include "commands/sequence.h"
#include "distributed/citus_procedures.h"
#include "distributed/colocation_utils.h"
#include "distributed/master_protocol.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_join_order.h"
#include "distributed/procedure_metadata.h"
#include "distributed/reference_table_utils.h"
#include "distributed/worker_transaction.h"
#include "storage/bufmgr.h"
#include "storage/lmgr.h"
#include "storage/lock.h"
#include "storage/fd.h"
#include "utils/builtins.h"
#include "utils/catcache.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/rel.h"
#include "utils/relcache.h"


static const int DistributedProcedureCacheId = -102008;
static CatCache *DistributedProcedureCache = NULL;
static bool DistributedProcedureCacheInitialized = false;


static int IndexOfArgumentName(HeapTuple procedureTuple, char *argumentName);
static uint32 ColocationIdForFunction(Oid procedureId, Oid distributionArgumentType,
									  char *colocateWithTableName);
static void InsertDistributedProcedureRecord(DistributedProcedureRecord *record);
static void CreateFunctionOnAllNodes(Oid procedureId);
static Oid CitusInternalNamespaceId(void);
static Oid CitusProceduresRelationId(void);
static Oid CitusProceduresFunctionIdIndexId(void);


PG_FUNCTION_INFO_V1(distribute_procedure);


Datum
distribute_procedure(PG_FUNCTION_ARGS)
{
	Oid procedureId = PG_GETARG_OID(0);
	text *distributionColumnText = PG_GETARG_TEXT_P(1);
	text *colocateWithTableNameText = PG_GETARG_TEXT_P(2);

	int distributionArgIndex = -1;
	Oid distributionArgType = InvalidOid;
	char *colocateWithTableName = NULL;
	HeapTuple procedureTuple = NULL;
	Form_pg_proc procedureStruct;
	int colocationId = INVALID_COLOCATION_ID;
	DistributedProcedureRecord *procedureRecord = NULL;

	CheckCitusVersion(ERROR);
	EnsureCoordinator();

	procedureTuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(procedureId));
	if (!HeapTupleIsValid(procedureTuple))
	{
		elog(ERROR, "cache lookup failed for function %u", procedureId);
	}

	procedureStruct = (Form_pg_proc) GETSTRUCT(procedureTuple);

	if (!PG_ARGISNULL(1))
	{
		char *distributionColumnName = text_to_cstring(distributionColumnText);
		distributionArgIndex = IndexOfArgumentName(procedureTuple,
												   distributionColumnName);
		if (distributionArgIndex < 0)
		{
			ereport(ERROR, (errmsg("function does not have an argument named \"%s\"",
								   distributionColumnName)));
		}

		distributionArgType = procedureStruct->proargtypes.values[distributionArgIndex];
	}

	colocateWithTableName = text_to_cstring(colocateWithTableNameText);

	colocationId = ColocationIdForFunction(procedureId, distributionArgType,
										   colocateWithTableName);

	procedureRecord =
		(DistributedProcedureRecord *) palloc0(sizeof(DistributedProcedureRecord));
	procedureRecord->procedureId = procedureId;
	procedureRecord->distributionArgumentIndex = distributionArgIndex;
	procedureRecord->colocationId = colocationId;

	InsertDistributedProcedureRecord(procedureRecord);
	CreateFunctionOnAllNodes(procedureId);

	ReleaseSysCache(procedureTuple);

	PG_RETURN_BOOL(true);
}


/*
 * IndexOfArgumentName finds the index of a function argument with the given
 * name, or -1 if it cannot be found.
 */
static int
IndexOfArgumentName(HeapTuple procedureTuple, char *argumentName)
{
	Datum proargnames = 0;
	Datum proargmodes = 0;
	int numberOfArguments = 0;
	bool isNull = false;
	char **argumentNames = NULL;
	int argumentIndex = 0;

	proargnames = SysCacheGetAttr(PROCNAMEARGSNSP, procedureTuple,
								  Anum_pg_proc_proargnames, &isNull);
	if (isNull)
	{
		return -1;
	}

	proargmodes = SysCacheGetAttr(PROCNAMEARGSNSP, procedureTuple,
								  Anum_pg_proc_proargmodes, &isNull);

	numberOfArguments = get_func_input_arg_names(proargnames, proargmodes,
												 &argumentNames);
	for (argumentIndex = 0; argumentIndex < numberOfArguments; argumentIndex++)
	{
		char *currentArgumentName = argumentNames[argumentIndex];

		if (strncmp(currentArgumentName, argumentName, NAMEDATALEN) == 0)
		{
			return argumentIndex;
		}
	}

	return -1;
}


/*
 * ColocationIdForFunction
 */
static uint32
ColocationIdForFunction(Oid procedureId, Oid distributionArgumentType,
						char *colocateWithTableName)
{
	uint32 colocationId = INVALID_COLOCATION_ID;

	/*
	 * Get an exclusive lock on the colocation system catalog. Therefore, we
	 * can be sure that there will no modifications on the colocation table
	 * until this transaction is committed.
	 */
	Relation pgDistColocation = heap_open(DistColocationRelationId(), ExclusiveLock);

	bool createdColocationGroup = false;

	if (distributionArgumentType == InvalidOid)
	{
		return CreateReferenceTableColocationId();
	}
	if (pg_strncasecmp(colocateWithTableName, "default", NAMEDATALEN) == 0)
	{
		/* check for default colocation group */
		colocationId = ColocationId(ShardCount, ShardReplicationFactor,
									distributionArgumentType);

		if (colocationId == INVALID_COLOCATION_ID)
		{
			colocationId = CreateColocationGroup(ShardCount, ShardReplicationFactor,
												 distributionArgumentType);
			createdColocationGroup = true;
		}
	}
	else
	{
		text *colocateWithTableNameText = cstring_to_text(colocateWithTableName);
		Oid sourceRelationId = ResolveRelationId(colocateWithTableNameText, false);
		Var *sourceDistributionColumn = DistPartitionKey(sourceRelationId);

		if (distributionArgumentType != sourceDistributionColumn->vartype)
		{
			char *functionName = get_func_name(procedureId);
			char *relationName = get_rel_name(sourceRelationId);

			ereport(ERROR, (errmsg("cannot colocate function %s with table %s",
								   functionName, relationName),
							errdetail("Distribution column types don't match for "
									  "%s and %s.", functionName, relationName)));
		}

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
 * InsertDistributedProcedureRecord inserts a record into citus.procedures recording
 * that the function should be distributed and
 */
static void
InsertDistributedProcedureRecord(DistributedProcedureRecord *record)
{
	Relation citusProcedures = NULL;
	TupleDesc tupleDescriptor = NULL;
	HeapTuple procedureTuple = NULL;
	Form_pg_proc procedureStruct;
	HeapTuple distributedProcedureTuple = NULL;
	Datum values[Natts_citus_procedures];
	bool isNulls[Natts_citus_procedures];
	Oid schemaId = InvalidOid;
	Name procedureName = NULL;

	procedureTuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(record->procedureId));
	if (!HeapTupleIsValid(procedureTuple))
	{
		elog(ERROR, "cache lookup failed for function %u", record->procedureId);
	}

	procedureStruct = (Form_pg_proc) GETSTRUCT(procedureTuple);
	schemaId = procedureStruct->pronamespace;
	procedureName = &procedureStruct->proname;

	/* form new shard tuple */
	memset(values, 0, sizeof(values));
	memset(isNulls, false, sizeof(isNulls));

	values[Anum_citus_procedures_function_id - 1] = ObjectIdGetDatum(record->procedureId);
	values[Anum_citus_procedures_schema_name - 1] = ObjectIdGetDatum(schemaId);
	values[Anum_citus_procedures_procedure_name - 1] = NameGetDatum(procedureName);
	values[Anum_citus_procedures_distribution_arg - 1] =
		Int32GetDatum(record->distributionArgumentIndex);
	values[Anum_citus_procedures_colocation_id - 1] = Int32GetDatum(record->colocationId);

	citusProcedures = heap_open(CitusProceduresRelationId(), RowExclusiveLock);

	tupleDescriptor = RelationGetDescr(citusProcedures);
	distributedProcedureTuple = heap_form_tuple(tupleDescriptor, values, isNulls);

	CatalogTupleInsert(citusProcedures, distributedProcedureTuple);

	CitusInvalidateRelcacheByRelid(CitusProceduresRelationId());

	/* increment the counter so that next command can see the row */
	CommandCounterIncrement();

	/* close relation */
	heap_close(citusProcedures, NoLock);

	ReleaseSysCache(procedureTuple);
}


/*
 * CreateFunctionOnAllNodes creates a given function on all nodes.
 */
static void
CreateFunctionOnAllNodes(Oid procedureId)
{
	char *userName = NULL;
	char *command = CreateFunctionCommand(procedureId);
	int parameterCount = 0;

	SendCommandToWorkersParams(WORKERS_WITH_METADATA, userName, command, parameterCount,
							   NULL, NULL);
}


/*
 * CreateFunctionCommand
 */
char *
CreateFunctionCommand(Oid procedureId)
{
	Datum procedureIdDatum = ObjectIdGetDatum(procedureId);
	Datum commandDatum = DirectFunctionCall1(pg_get_functiondef, procedureIdDatum);

	return TextDatumGetCString(commandDatum);
}


/*
 * CitusInternalNamespaceId returns the OID of the citus_internal namespace.
 *
 * TODO: caching
 */
static Oid
CitusInternalNamespaceId(void)
{
	bool missingOK = false;

	return get_namespace_oid("citus_internal", missingOK);
}


/*
 * CitusProceduresRelationId returns the OID of citus.procedures.
 *
 * TODO: caching
 */
static Oid
CitusProceduresRelationId(void)
{
	return get_relname_relid("procedures", CitusInternalNamespaceId());
}


/*
 * CitusProceduresFunctionIdIndexId returns the OID of citus.procedures.
 *
 * TODO: caching
 */
static Oid
CitusProceduresFunctionIdIndexId(void)
{
	return get_relname_relid("procedures_function_id_idx", CitusInternalNamespaceId());
}


/*
 * LoadDistributedProcedureRecord loads a record from citus.procedures.
 *
 * TODO: caching
 */
DistributedProcedureRecord *
LoadDistributedProcedureRecord(Oid procedureId)
{
	HeapTuple heapTuple = NULL;
	DistributedProcedureRecord *record = NULL;

	if (!DistributedProcedureCacheInitialized)
	{
		int keyColumnCount = 1;
		int keyColumnArray[] = { Anum_citus_procedures_function_id, 0, 0, 0 };

		DistributedProcedureCache = InitCatCache(DistributedProcedureCacheId,
												 CitusProceduresRelationId(),
												 CitusProceduresFunctionIdIndexId(),
												 keyColumnCount,
												 keyColumnArray,
												 16);

		/* load tuple descriptor */
		InitCatCachePhase2(DistributedProcedureCache, false);
												 
		DistributedProcedureCacheInitialized = true;
	}

	heapTuple = SearchCatCache1(DistributedProcedureCache, ObjectIdGetDatum(procedureId));

	if (HeapTupleIsValid(heapTuple))
	{
		Datum datumArray[Natts_citus_procedures];
		bool isNullArray[Natts_citus_procedures];

		heap_deform_tuple(heapTuple, DistributedProcedureCache->cc_tupdesc, datumArray,
						  isNullArray);

		record =
			(DistributedProcedureRecord *) palloc0(sizeof(DistributedProcedureRecord));
		record->procedureId =
			DatumGetUInt32(datumArray[Anum_citus_procedures_function_id - 1]);
		record->distributionArgumentIndex =
			DatumGetUInt32(datumArray[Anum_citus_procedures_distribution_arg - 1]);
		record->colocationId =
			DatumGetInt32(datumArray[Anum_citus_procedures_colocation_id - 1]);

		ReleaseCatCache(heapTuple);
	}

	return record;
}
