/*-------------------------------------------------------------------------
 *
 * alter_table.c
 *	  Routines related to the altering of tables.
 *
 *		There are three UDFs defined in this file:
 *		undistribute_table:
 *			Turns a distributed table to a local table
 *		alter_distributed_table:
 *			Alters distribution_column, shard_count or colocate_with
 *			properties of a distributed table
 *		alter_table_set_access_method:
 *			Changes the access method of a table
 *
 *		All three methods work in similar steps:
 *			- Create a new table the required way (with a different
 *			  shard count, distribution column, colocate with value,
 *			  access method or local)
 *			- Move everything to the new table
 *			- Drop the old one
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "fmgr.h"

#include "access/hash.h"
#include "access/xact.h"
#include "catalog/dependency.h"
#include "catalog/pg_am.h"
#include "catalog/pg_depend.h"
#include "catalog/pg_rewrite_d.h"
#include "commands/defrem.h"
#include "executor/spi.h"
#include "nodes/pg_list.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

#include "columnar/columnar.h"
#include "columnar/columnar_tableam.h"

#include "distributed/colocation_utils.h"
#include "distributed/commands.h"
#include "distributed/commands/utility_hook.h"
#include "distributed/coordinator_protocol.h"
#include "distributed/deparser.h"
#include "distributed/distribution_column.h"
#include "distributed/hash_helpers.h"
#include "distributed/listutils.h"
#include "distributed/local_executor.h"
#include "distributed/metadata/dependency.h"
#include "distributed/metadata/distobject.h"
#include "distributed/metadata_cache.h"
#include "distributed/metadata_sync.h"
#include "distributed/multi_executor.h"
#include "distributed/multi_logical_planner.h"
#include "distributed/multi_partitioning_utils.h"
#include "distributed/namespace_utils.h"
#include "distributed/reference_table_utils.h"
#include "distributed/relation_access_tracking.h"
#include "distributed/replication_origin_session_utils.h"
#include "distributed/shard_utils.h"
#include "distributed/shared_library_init.h"
#include "distributed/tenant_schema_metadata.h"
#include "distributed/worker_protocol.h"
#include "distributed/worker_transaction.h"


/* Table Conversion Types */
#define UNDISTRIBUTE_TABLE 'u'
#define ALTER_DISTRIBUTED_TABLE 'a'
#define ALTER_TABLE_SET_ACCESS_METHOD 'm'

#define UNDISTRIBUTE_TABLE_CASCADE_HINT \
	"Use cascade option to undistribute all the relations involved in " \
	"a foreign key relationship with %s by executing SELECT " \
	"undistribute_table($$%s$$, cascade_via_foreign_keys=>true)"


typedef TableConversionReturn *(*TableConversionFunction)(struct
														  TableConversionParameters *);


/*
 * TableConversionState objects are used for table conversion functions:
 * UndistributeTable, AlterDistributedTable, AlterTableSetAccessMethod.
 *
 * They can be created using TableConversionParameters objects with
 * CreateTableConversion function.
 *
 * TableConversionState objects include everything TableConversionParameters
 * objects do and some extra to be used in the conversion process.
 */
typedef struct TableConversionState
{
	/*
	 * Determines type of conversion: UNDISTRIBUTE_TABLE,
	 * ALTER_DISTRIBUTED_TABLE, ALTER_TABLE_SET_ACCESS_METHOD.
	 */
	char conversionType;

	/* Oid of the table to do conversion on */
	Oid relationId;

	/*
	 * Options to do conversions on the table
	 * distributionColumn is the name of the new distribution column,
	 * shardCountIsNull is if the shardCount variable is not given
	 * shardCount is the new shard count,
	 * colocateWith is the name of the table to colocate with, 'none', or
	 * 'default'
	 * accessMethod is the name of the new accessMethod for the table
	 */
	char *distributionColumn;
	bool shardCountIsNull;
	int shardCount;
	char *colocateWith;
	char *accessMethod;

	/*
	 * cascadeToColocated determines whether the shardCount and
	 * colocateWith will be cascaded to the currently colocated tables
	 */
	CascadeToColocatedOption cascadeToColocated;

	/*
	 * cascadeViaForeignKeys determines if the conversion operation
	 * will be cascaded to the graph connected with foreign keys
	 * to the table
	 */
	bool cascadeViaForeignKeys;


	/* schema of the table */
	char *schemaName;
	Oid schemaId;

	/* name of the table */
	char *relationName;

	/* new relation oid after the conversion */
	Oid newRelationId;

	/* temporary name for intermediate table */
	char *tempName;

	/*hash that is appended to the name to create tempName */
	uint32 hashOfName;

	/* shard count of the table before conversion */
	int originalShardCount;

	/* list of the table oids of tables colocated with the table before conversion */
	List *colocatedTableList;

	/* new distribution key, if distributionColumn variable is given */
	Var *distributionKey;

	/* distribution key of the table before conversion */
	Var *originalDistributionKey;

	/* access method name of the table before conversion */
	char *originalAccessMethod;

	/*
	 * The function that will be used for the conversion
	 * Must comply with conversionType
	 * UNDISTRIBUTE_TABLE -> UndistributeTable
	 * ALTER_DISTRIBUTED_TABLE -> AlterDistributedTable
	 * ALTER_TABLE_SET_ACCESS_METHOD -> AlterTableSetAccessMethod
	 */
	TableConversionFunction function;

	/*
	 * suppressNoticeMessages determines if we want to suppress NOTICE
	 * messages that we explicitly issue
	 */
	bool suppressNoticeMessages;
} TableConversionState;


static TableConversionReturn * AlterDistributedTable(TableConversionParameters *params);
static TableConversionReturn * AlterTableSetAccessMethod(
	TableConversionParameters *params);
static TableConversionReturn * ConvertTable(TableConversionState *con);
static TableConversionReturn * ConvertTableInternal(TableConversionState *con);
static bool SwitchToSequentialAndLocalExecutionIfShardNameTooLong(char *relationName,
																  char *longestShardName);
static void DropIndexesNotSupportedByColumnar(Oid relationId,
											  bool suppressNoticeMessages);
static char * GetIndexAccessMethodName(Oid indexId);
static void DropConstraintRestrict(Oid relationId, Oid constraintId);
static void DropIndexRestrict(Oid indexId);
static void EnsureTableNotReferencing(Oid relationId, char conversionType);
static void EnsureTableNotReferenced(Oid relationId, char conversionType);
static void EnsureTableNotForeign(Oid relationId);
static void EnsureTableNotPartition(Oid relationId);
static void ErrorIfColocateWithTenantTable(char *colocateWith);
static TableConversionState * CreateTableConversion(TableConversionParameters *params);
static void CreateDistributedTableLike(TableConversionState *con);
static void CreateCitusTableLike(TableConversionState *con);
static void ReplaceTable(Oid sourceId, Oid targetId, List *justBeforeDropCommands,
						 bool suppressNoticeMessages);
static bool HasAnyGeneratedStoredColumns(Oid relationId);
static List * GetNonGeneratedStoredColumnNameList(Oid relationId);
static void CheckAlterDistributedTableConversionParameters(TableConversionState *con);
static char * CreateWorkerChangeSequenceDependencyCommand(char *qualifiedSequeceName,
														  char *qualifiedSourceName,
														  char *qualifiedTargetName);
static void ErrorIfMatViewSizeExceedsTheLimit(Oid matViewOid);
static char * CreateMaterializedViewDDLCommand(Oid matViewOid);
static char * GetAccessMethodForMatViewIfExists(Oid viewOid);
static bool WillRecreateForeignKeyToReferenceTable(Oid relationId,
												   CascadeToColocatedOption cascadeOption);
static void WarningsForDroppingForeignKeysWithDistributedTables(Oid relationId);
static void ErrorIfUnsupportedCascadeObjects(Oid relationId);
static List * WrapTableDDLCommands(List *commandStrings);
static bool DoesCascadeDropUnsupportedObject(Oid classId, Oid id, HTAB *nodeMap);
static TableConversionReturn * CopyTableConversionReturnIntoCurrentContext(
	TableConversionReturn *tableConversionReturn);

PG_FUNCTION_INFO_V1(undistribute_table);
PG_FUNCTION_INFO_V1(alter_distributed_table);
PG_FUNCTION_INFO_V1(alter_table_set_access_method);
PG_FUNCTION_INFO_V1(worker_change_sequence_dependency);

/* global variable keeping track of whether we are in a table type conversion function */
bool InTableTypeConversionFunctionCall = false;

/* controlled by GUC, in MB */
int MaxMatViewSizeToAutoRecreate = 1024;

/*
 * undistribute_table gets a distributed table name and
 * udistributes it.
 */
Datum
undistribute_table(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	Oid relationId = PG_GETARG_OID(0);
	bool cascadeViaForeignKeys = PG_GETARG_BOOL(1);

	TableConversionParameters params = {
		.relationId = relationId,
		.cascadeViaForeignKeys = cascadeViaForeignKeys,
		.bypassTenantCheck = false
	};

	UndistributeTable(&params);

	PG_RETURN_VOID();
}


/*
 * alter_distributed_table gets a distributed table and some other
 * parameters and alters some properties of the table according to
 * the parameters.
 */
Datum
alter_distributed_table(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	Oid relationId = PG_GETARG_OID(0);

	char *distributionColumn = NULL;
	if (!PG_ARGISNULL(1))
	{
		text *distributionColumnText = PG_GETARG_TEXT_P(1);
		distributionColumn = text_to_cstring(distributionColumnText);
	}

	int shardCount = 0;
	bool shardCountIsNull = true;
	if (!PG_ARGISNULL(2))
	{
		shardCount = PG_GETARG_INT32(2);
		shardCountIsNull = false;
	}

	char *colocateWith = NULL;
	if (!PG_ARGISNULL(3))
	{
		text *colocateWithText = PG_GETARG_TEXT_P(3);
		colocateWith = text_to_cstring(colocateWithText);
	}

	CascadeToColocatedOption cascadeToColocated = CASCADE_TO_COLOCATED_UNSPECIFIED;
	if (!PG_ARGISNULL(4))
	{
		if (PG_GETARG_BOOL(4) == true)
		{
			cascadeToColocated = CASCADE_TO_COLOCATED_YES;
		}
		else
		{
			cascadeToColocated = CASCADE_TO_COLOCATED_NO;
		}
	}

	TableConversionParameters params = {
		.relationId = relationId,
		.distributionColumn = distributionColumn,
		.shardCountIsNull = shardCountIsNull,
		.shardCount = shardCount,
		.colocateWith = colocateWith,
		.cascadeToColocated = cascadeToColocated
	};

	AlterDistributedTable(&params);

	PG_RETURN_VOID();
}


/*
 * alter_table_set_access_method gets a distributed table and an access
 * method and changes table's access method into that.
 */
Datum
alter_table_set_access_method(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	Oid relationId = PG_GETARG_OID(0);

	text *accessMethodText = PG_GETARG_TEXT_P(1);
	char *accessMethod = text_to_cstring(accessMethodText);

	TableConversionParameters params = {
		.relationId = relationId,
		.accessMethod = accessMethod
	};

	AlterTableSetAccessMethod(&params);

	PG_RETURN_VOID();
}


/*
 * worker_change_sequence_dependency is a wrapper UDF for
 * changeDependencyFor function
 */
Datum
worker_change_sequence_dependency(PG_FUNCTION_ARGS)
{
	Oid sequenceOid = PG_GETARG_OID(0);
	Oid sourceRelationOid = PG_GETARG_OID(1);
	Oid targetRelationOid = PG_GETARG_OID(2);

	changeDependencyFor(RelationRelationId, sequenceOid,
						RelationRelationId, sourceRelationOid, targetRelationOid);
	PG_RETURN_VOID();
}


/*
 * DropFKeysAndUndistributeTable drops all foreign keys that relation with
 * relationId is involved then undistributes it.
 * Note that as UndistributeTable changes relationId of relation, this
 * function also returns new relationId of relation.
 * Also note that callers are responsible for storing & recreating foreign
 * keys to be dropped if needed.
 */
Oid
DropFKeysAndUndistributeTable(Oid relationId)
{
	DropFKeysRelationInvolvedWithTableType(relationId, INCLUDE_ALL_TABLE_TYPES);

	/* store them before calling UndistributeTable as it changes relationId */
	char *relationName = get_rel_name(relationId);
	Oid schemaId = get_rel_namespace(relationId);

	/* suppress notices messages not to be too verbose */
	TableConversionParameters params = {
		.relationId = relationId,
		.cascadeViaForeignKeys = false,
		.suppressNoticeMessages = true
	};
	UndistributeTable(&params);

	Oid newRelationId = get_relname_relid(relationName, schemaId);

	/*
	 * We don't expect this to happen but to be on the safe side let's error
	 * out here.
	 */
	EnsureRelationExists(newRelationId);

	return newRelationId;
}


/*
 * UndistributeTables undistributes given relations. It first collects all foreign keys
 * to recreate them after the undistribution. Then, drops the foreign keys and
 * undistributes the relations. Finally, it recreates foreign keys.
 */
void
UndistributeTables(List *relationIdList)
{
	/*
	 * Collect foreign keys for recreation and then drop fkeys and undistribute
	 * tables.
	 */
	List *originalForeignKeyRecreationCommands = NIL;
	Oid relationId = InvalidOid;
	foreach_declared_oid(relationId, relationIdList)
	{
		List *fkeyCommandsForRelation =
			GetFKeyCreationCommandsRelationInvolvedWithTableType(relationId,
																 INCLUDE_ALL_TABLE_TYPES);
		originalForeignKeyRecreationCommands = list_concat(
			originalForeignKeyRecreationCommands, fkeyCommandsForRelation);
		DropFKeysAndUndistributeTable(relationId);
	}

	/* We can skip foreign key validations as we are sure about them at start */
	bool skip_validation = true;
	ExecuteForeignKeyCreateCommandList(originalForeignKeyRecreationCommands,
									   skip_validation);
}


/*
 * EnsureUndistributeTenantTableSafe ensures that it is safe to undistribute a tenant table.
 */
void
EnsureUndistributeTenantTableSafe(Oid relationId, const char *operationName)
{
	Oid schemaId = get_rel_namespace(relationId);
	Assert(IsTenantSchema(schemaId));

	/* We only allow undistribute while altering schema */
	if (strcmp(operationName, TenantOperationNames[TENANT_SET_SCHEMA]) != 0)
	{
		ErrorIfTenantTable(relationId, operationName);
	}

	char *tableName = get_rel_name(relationId);
	char *schemaName = get_namespace_name(schemaId);

	/*
	 * Partition table cannot be undistributed. Otherwise, its parent table would still
	 * be a tenant table whereas partition table would be a local table.
	 */
	if (PartitionTable(relationId))
	{
		ereport(ERROR, (errmsg("%s is not allowed for partition table %s in distributed "
							   "schema %s", operationName, tableName, schemaName),
						errdetail("partition table should be under the same distributed "
								  "schema as its parent and be a "
								  "distributed schema table.")));
	}

	/*
	 * When table is referenced by or referencing to a table in the same tenant
	 * schema, we should disallow undistributing the table since we do not allow
	 * foreign keys from/to Citus local or Postgres local table to/from distributed
	 * schema.
	 */
	List *fkeyCommandsWithSingleShardTables =
		GetFKeyCreationCommandsRelationInvolvedWithTableType(
			relationId, INCLUDE_SINGLE_SHARD_TABLES);
	if (fkeyCommandsWithSingleShardTables != NIL)
	{
		ereport(ERROR, (errmsg("%s is not allowed for table %s in distributed schema %s",
							   operationName, tableName, schemaName),
						errdetail("distributed schemas cannot have foreign keys from/to "
								  "local tables or different schema")));
	}
}


/*
 * UndistributeTable undistributes the given table. It uses ConvertTable function to
 * create a new local table and move everything to that table.
 *
 * The local tables, tables with references, partition tables and foreign tables are
 * not supported. The function gives errors in these cases.
 */
TableConversionReturn *
UndistributeTable(TableConversionParameters *params)
{
	EnsureCoordinator();
	EnsureRelationExists(params->relationId);
	EnsureTableOwner(params->relationId);

	if (!IsCitusTable(params->relationId))
	{
		ereport(ERROR, (errmsg("cannot undistribute table "
							   "because the table is not distributed")));
	}

	Oid schemaId = get_rel_namespace(params->relationId);
	if (!params->bypassTenantCheck && IsTenantSchema(schemaId) &&
		IsCitusTableType(params->relationId, SINGLE_SHARD_DISTRIBUTED))
	{
		EnsureUndistributeTenantTableSafe(params->relationId,
										  TenantOperationNames[TENANT_UNDISTRIBUTE_TABLE]);
	}

	if (!params->cascadeViaForeignKeys)
	{
		EnsureTableNotReferencing(params->relationId, UNDISTRIBUTE_TABLE);
		EnsureTableNotReferenced(params->relationId, UNDISTRIBUTE_TABLE);
	}

	EnsureTableNotPartition(params->relationId);

	if (PartitionedTable(params->relationId))
	{
		List *partitionList = PartitionList(params->relationId);

		/*
		 * This is a less common pattern where foreing key is directly from/to
		 * the partition relation as we already handled inherited foreign keys
		 * on partitions either by erroring out or cascading via foreign keys.
		 * It seems an acceptable limitation for now to ask users to drop such
		 * foreign keys manually.
		 */
		ErrorIfAnyPartitionRelationInvolvedInNonInheritedFKey(partitionList);
	}

	ErrorIfUnsupportedCascadeObjects(params->relationId);

	params->conversionType = UNDISTRIBUTE_TABLE;
	params->shardCountIsNull = true;
	TableConversionState *con = CreateTableConversion(params);

	SetupReplicationOriginLocalSession();
	TableConversionReturn *conv = ConvertTable(con);
	ResetReplicationOriginLocalSession();
	return conv;
}


/*
 * AlterDistributedTable changes some properties of the given table. It uses
 * ConvertTable function to create a new local table and move everything to that table.
 *
 * The local and reference tables, tables with references, partition tables and foreign
 * tables are not supported. The function gives errors in these cases.
 */
TableConversionReturn *
AlterDistributedTable(TableConversionParameters *params)
{
	EnsureCoordinator();
	EnsureRelationExists(params->relationId);
	EnsureTableOwner(params->relationId);

	if (!IsCitusTableType(params->relationId, DISTRIBUTED_TABLE))
	{
		ereport(ERROR, (errmsg("cannot alter table because the table "
							   "is not distributed")));
	}

	ErrorIfTenantTable(params->relationId, TenantOperationNames[TENANT_ALTER_TABLE]);
	ErrorIfColocateWithTenantTable(params->colocateWith);

	EnsureTableNotForeign(params->relationId);
	EnsureTableNotPartition(params->relationId);
	EnsureHashDistributedTable(params->relationId);

	ErrorIfUnsupportedCascadeObjects(params->relationId);

	params->conversionType = ALTER_DISTRIBUTED_TABLE;
	TableConversionState *con = CreateTableConversion(params);
	CheckAlterDistributedTableConversionParameters(con);

	if (WillRecreateForeignKeyToReferenceTable(con->relationId, con->cascadeToColocated))
	{
		ereport(DEBUG1, (errmsg("setting multi shard modify mode to sequential")));
		SetLocalMultiShardModifyModeToSequential();
	}

	return ConvertTable(con);
}


/*
 * AlterTableSetAccessMethod changes the access method of the given table. It uses
 * ConvertTable function to create a new table with the access method and move everything
 * to that table.
 *
 * The local and references tables, tables with references, partition tables and foreign
 * tables are not supported. The function gives errors in these cases.
 */
TableConversionReturn *
AlterTableSetAccessMethod(TableConversionParameters *params)
{
	EnsureRelationExists(params->relationId);
	EnsureTableOwner(params->relationId);

	if (IsCitusTable(params->relationId))
	{
		EnsureCoordinator();
	}

	EnsureTableNotReferencing(params->relationId, ALTER_TABLE_SET_ACCESS_METHOD);
	EnsureTableNotReferenced(params->relationId, ALTER_TABLE_SET_ACCESS_METHOD);
	EnsureTableNotForeign(params->relationId);

	if (!IsCitusTableType(params->relationId, SINGLE_SHARD_DISTRIBUTED) &&
		IsCitusTableType(params->relationId, DISTRIBUTED_TABLE))
	{
		/* we do not support non-hash distributed tables, except single shard tables */
		EnsureHashDistributedTable(params->relationId);
	}

	if (PartitionedTable(params->relationId))
	{
		ereport(ERROR, (errmsg("you cannot alter access method of a partitioned table")));
	}

	if (get_rel_relkind(params->relationId) == RELKIND_VIEW)
	{
		ereport(ERROR, (errmsg("you cannot alter access method of a view")));
	}

	if (PartitionTable(params->relationId) &&
		IsCitusTableType(params->relationId, DISTRIBUTED_TABLE))
	{
		Oid parentRelationId = PartitionParentOid(params->relationId);
		if (HasForeignKeyToReferenceTable(parentRelationId))
		{
			ereport(DEBUG1, (errmsg("setting multi shard modify mode to sequential")));
			SetLocalMultiShardModifyModeToSequential();
		}
	}

	ErrorIfUnsupportedCascadeObjects(params->relationId);

	params->conversionType = ALTER_TABLE_SET_ACCESS_METHOD;
	params->shardCountIsNull = true;
	TableConversionState *con = CreateTableConversion(params);

	if (strcmp(con->originalAccessMethod, con->accessMethod) == 0)
	{
		ereport(ERROR, (errmsg("the access method of %s is already %s",
							   generate_qualified_relation_name(con->relationId),
							   con->accessMethod)));
	}

	return ConvertTable(con);
}


/*
 * ConvertTableInternal is used for converting a table into a new table with different
 * properties. The conversion is done by creating a new table, moving everything to the
 * new table and dropping the old one. So the oid of the table is not preserved.
 *
 * The new table will have the same name, columns and rows. It will also have partitions,
 * views, sequences of the old table. Finally it will have everything created by
 * GetPostLoadTableCreationCommands function, which include indexes. These will be
 * re-created during conversion, so their oids are not preserved either (except for
 * sequences). However, their names are preserved.
 *
 * The dropping of old table is done with CASCADE. Anything not mentioned here will
 * be dropped.
 *
 * The function returns a TableConversionReturn object that can stores variables that
 * can be used at the caller operations.
 *
 * To be able to provide more meaningful messages while converting a table type,
 * Citus keeps InTableTypeConversionFunctionCall flag. Don't forget to set it properly
 * in case you add a new way to return from this function.
 */
TableConversionReturn *
ConvertTableInternal(TableConversionState *con)
{
	InTableTypeConversionFunctionCall = true;

	/*
	 * We undistribute citus local tables that are not chained with any reference
	 * tables via foreign keys at the end of the utility hook.
	 * Here we temporarily set the related GUC to off to disable the logic for
	 * internally executed DDL's that might invoke this mechanism unnecessarily.
	 */
	bool oldEnableLocalReferenceForeignKeys = EnableLocalReferenceForeignKeys;
	SetLocalEnableLocalReferenceForeignKeys(false);

	/* switch to sequential execution if shard names will be too long */
	SwitchToSequentialAndLocalExecutionIfRelationNameTooLong(con->relationId,
															 con->relationName);

	if (con->conversionType == UNDISTRIBUTE_TABLE && con->cascadeViaForeignKeys &&
		(TableReferencing(con->relationId) || TableReferenced(con->relationId)))
	{
		/*
		 * Acquire ExclusiveLock as UndistributeTable does in order to
		 * make sure that no modifications happen on the relations.
		 */
		CascadeOperationForFkeyConnectedRelations(con->relationId, ExclusiveLock,
												  CASCADE_FKEY_UNDISTRIBUTE_TABLE);

		/*
		 * Undistributed every foreign key connected relation in our foreign key
		 * subgraph including itself, so return here.
		 */
		SetLocalEnableLocalReferenceForeignKeys(oldEnableLocalReferenceForeignKeys);
		InTableTypeConversionFunctionCall = false;
		return NULL;
	}
	char *newAccessMethod = con->accessMethod ? con->accessMethod :
							con->originalAccessMethod;
	IncludeSequenceDefaults includeSequenceDefaults = NEXTVAL_SEQUENCE_DEFAULTS;
	IncludeIdentities includeIdentity = INCLUDE_IDENTITY;

	List *preLoadCommands = GetPreLoadTableCreationCommands(con->relationId,
															includeSequenceDefaults,
															includeIdentity,
															newAccessMethod);

	if (con->accessMethod && strcmp(con->accessMethod, "columnar") == 0)
	{
		DropIndexesNotSupportedByColumnar(con->relationId,
										  con->suppressNoticeMessages);
	}

	/*
	 * Since we already dropped unsupported indexes, we can safely pass
	 * includeIndexes to be true.
	 */
	bool includeIndexes = true;
	bool includeReplicaIdentity = true;
	List *postLoadCommands = GetPostLoadTableCreationCommands(con->relationId,
															  includeIndexes,
															  includeReplicaIdentity);
	List *justBeforeDropCommands = NIL;
	List *attachPartitionCommands = NIL;

	List *createViewCommands = GetViewCreationCommandsOfTable(con->relationId);

	postLoadCommands = list_concat(postLoadCommands,
								   WrapTableDDLCommands(createViewCommands));

	/* need to add back to publications after dropping the original table */
	bool isAdd = true;
	List *alterPublicationCommands =
		GetAlterPublicationDDLCommandsForTable(con->relationId, isAdd);

	postLoadCommands = list_concat(postLoadCommands,
								   WrapTableDDLCommands(alterPublicationCommands));

	List *foreignKeyCommands = NIL;
	if (con->conversionType == ALTER_DISTRIBUTED_TABLE)
	{
		foreignKeyCommands = GetForeignConstraintToReferenceTablesCommands(
			con->relationId);
		if (con->cascadeToColocated == CASCADE_TO_COLOCATED_YES ||
			con->cascadeToColocated == CASCADE_TO_COLOCATED_NO_ALREADY_CASCADED)
		{
			List *foreignKeyToDistributedTableCommands =
				GetForeignConstraintToDistributedTablesCommands(con->relationId);
			foreignKeyCommands = list_concat(foreignKeyCommands,
											 foreignKeyToDistributedTableCommands);

			List *foreignKeyFromDistributedTableCommands =
				GetForeignConstraintFromDistributedTablesCommands(con->relationId);
			foreignKeyCommands = list_concat(foreignKeyCommands,
											 foreignKeyFromDistributedTableCommands);
		}
		else
		{
			WarningsForDroppingForeignKeysWithDistributedTables(con->relationId);
		}
	}

	bool isPartitionTable = false;
	char *attachToParentCommand = NULL;
	if (PartitionTable(con->relationId))
	{
		isPartitionTable = true;
		char *detachFromParentCommand = GenerateDetachPartitionCommand(con->relationId);
		attachToParentCommand = GenerateAlterTableAttachPartitionCommand(con->relationId);

		justBeforeDropCommands = lappend(justBeforeDropCommands, detachFromParentCommand);
	}

	char *qualifiedRelationName = quote_qualified_identifier(con->schemaName,
															 con->relationName);

	if (PartitionedTable(con->relationId))
	{
		if (!con->suppressNoticeMessages)
		{
			ereport(NOTICE, (errmsg("converting the partitions of %s",
									qualifiedRelationName)));
		}

		List *partitionList = PartitionList(con->relationId);

		Oid partitionRelationId = InvalidOid;
		foreach_declared_oid(partitionRelationId, partitionList)
		{
			char *tableQualifiedName = generate_qualified_relation_name(
				partitionRelationId);
			char *detachPartitionCommand = GenerateDetachPartitionCommand(
				partitionRelationId);
			char *attachPartitionCommand = GenerateAlterTableAttachPartitionCommand(
				partitionRelationId);

			/*
			 * We first detach the partitions to be able to convert them separately.
			 * After this they are no longer partitions, so they will not be caught by
			 * the checks.
			 */
			ExecuteQueryViaSPI(detachPartitionCommand, SPI_OK_UTILITY);
			attachPartitionCommands = lappend(attachPartitionCommands,
											  attachPartitionCommand);

			CascadeToColocatedOption cascadeOption = CASCADE_TO_COLOCATED_NO;
			if (con->cascadeToColocated == CASCADE_TO_COLOCATED_YES ||
				con->cascadeToColocated == CASCADE_TO_COLOCATED_NO_ALREADY_CASCADED)
			{
				cascadeOption = CASCADE_TO_COLOCATED_NO_ALREADY_CASCADED;
			}

			TableConversionParameters partitionParam = {
				.relationId = partitionRelationId,
				.distributionColumn = con->distributionColumn,
				.shardCountIsNull = con->shardCountIsNull,
				.shardCount = con->shardCount,
				.cascadeToColocated = cascadeOption,
				.colocateWith = con->colocateWith,
				.suppressNoticeMessages = con->suppressNoticeMessages,

				/*
				 * Even if we called UndistributeTable with cascade option, we
				 * shouldn't cascade via foreign keys on partitions. Otherwise,
				 * we might try to undistribute partitions of other tables in
				 * our foreign key subgraph more than once.
				 */
				.cascadeViaForeignKeys = false
			};

			TableConversionReturn *partitionReturn = con->function(&partitionParam);
			if (cascadeOption == CASCADE_TO_COLOCATED_NO_ALREADY_CASCADED)
			{
				foreignKeyCommands = list_concat(foreignKeyCommands,
												 partitionReturn->foreignKeyCommands);
			}


			/*
			 * If we are altering a partitioned distributed table by
			 * colocateWith:none, we override con->colocationWith parameter
			 * with the first newly created partition table to share the
			 * same colocation group for rest of partitions and partitioned
			 * table.
			 */
			if (con->colocateWith != NULL && IsColocateWithNone(con->colocateWith))
			{
				con->colocateWith = tableQualifiedName;
			}
		}
	}

	if (!con->suppressNoticeMessages)
	{
		ereport(NOTICE, (errmsg("creating a new table for %s", qualifiedRelationName)));
	}

	TableDDLCommand *tableCreationCommand = NULL;
	foreach_declared_ptr(tableCreationCommand, preLoadCommands)
	{
		Assert(CitusIsA(tableCreationCommand, TableDDLCommand));

		char *tableCreationSql = GetTableDDLCommand(tableCreationCommand);
		Node *parseTree = ParseTreeNode(tableCreationSql);

		RelayEventExtendNames(parseTree, con->schemaName, con->hashOfName);
		ProcessUtilityParseTree(parseTree, tableCreationSql, PROCESS_UTILITY_QUERY,
								NULL, None_Receiver, NULL);
	}

	/* set columnar options */
	if (con->accessMethod == NULL && con->originalAccessMethod &&
		strcmp(con->originalAccessMethod, "columnar") == 0)
	{
		ColumnarOptions options = { 0 };
		extern_ReadColumnarOptions(con->relationId, &options);

		ColumnarTableDDLContext *context = (ColumnarTableDDLContext *) palloc0(
			sizeof(ColumnarTableDDLContext));

		/* build the context */
		context->schemaName = con->schemaName;
		context->relationName = con->relationName;
		context->options = options;

		char *columnarOptionsSql = GetShardedTableDDLCommandColumnar(con->hashOfName,
																	 context);

		ExecuteQueryViaSPI(columnarOptionsSql, SPI_OK_UTILITY);
	}

	con->newRelationId = get_relname_relid(con->tempName, con->schemaId);

	if (con->conversionType == ALTER_DISTRIBUTED_TABLE)
	{
		CreateDistributedTableLike(con);
	}
	else if (con->conversionType == ALTER_TABLE_SET_ACCESS_METHOD)
	{
		CreateCitusTableLike(con);
	}

	/* preserve colocation with procedures/functions */
	if (con->conversionType == ALTER_DISTRIBUTED_TABLE)
	{
		/*
		 * Updating the colocationId of functions is always desirable for
		 * the following scenario:
		 *    we have shardCount or colocateWith change
		 *    AND  entire co-location group is altered
		 * The reason for the second condition is because we currently don't
		 * remember the original table specified in the colocateWith when
		 * distributing the function. We only remember the colocationId in
		 * pg_dist_object table.
		 */
		if ((!con->shardCountIsNull || con->colocateWith != NULL) &&
			(con->cascadeToColocated == CASCADE_TO_COLOCATED_YES || list_length(
				 con->colocatedTableList) == 1) && con->distributionColumn == NULL)
		{
			/*
			 * Update the colocationId from the one of the old relation to the one
			 * of the new relation for all tuples in citus.pg_dist_object
			 */
			UpdateDistributedObjectColocationId(TableColocationId(con->relationId),
												TableColocationId(con->newRelationId));
		}
	}

	ReplaceTable(con->relationId, con->newRelationId, justBeforeDropCommands,
				 con->suppressNoticeMessages);

	TableDDLCommand *tableConstructionCommand = NULL;
	foreach_declared_ptr(tableConstructionCommand, postLoadCommands)
	{
		Assert(CitusIsA(tableConstructionCommand, TableDDLCommand));
		char *tableConstructionSQL = GetTableDDLCommand(tableConstructionCommand);
		ExecuteQueryViaSPI(tableConstructionSQL, SPI_OK_UTILITY);
	}

	/*
	 * when there are many partitions, each call to ProcessUtilityParseTree
	 * accumulates used memory. Free context after each call.
	 */
	MemoryContext citusPerPartitionContext =
		AllocSetContextCreate(CurrentMemoryContext,
							  "citus_per_partition_context",
							  ALLOCSET_DEFAULT_SIZES);
	MemoryContext oldContext = MemoryContextSwitchTo(citusPerPartitionContext);

	char *attachPartitionCommand = NULL;
	foreach_declared_ptr(attachPartitionCommand, attachPartitionCommands)
	{
		MemoryContextReset(citusPerPartitionContext);

		Node *parseTree = ParseTreeNode(attachPartitionCommand);

		ProcessUtilityParseTree(parseTree, attachPartitionCommand,
								PROCESS_UTILITY_QUERY,
								NULL, None_Receiver, NULL);
	}

	MemoryContextSwitchTo(oldContext);
	MemoryContextDelete(citusPerPartitionContext);

	if (isPartitionTable)
	{
		ExecuteQueryViaSPI(attachToParentCommand, SPI_OK_UTILITY);
	}

	if (con->cascadeToColocated == CASCADE_TO_COLOCATED_YES)
	{
		Oid colocatedTableId = InvalidOid;

		/* For now we only support cascade to colocation for alter_distributed_table UDF */
		Assert(con->conversionType == ALTER_DISTRIBUTED_TABLE);
		foreach_declared_oid(colocatedTableId, con->colocatedTableList)
		{
			if (colocatedTableId == con->relationId)
			{
				continue;
			}

			TableConversionParameters cascadeParam = {
				.relationId = colocatedTableId,
				.shardCountIsNull = con->shardCountIsNull,
				.shardCount = con->shardCount,
				.colocateWith = qualifiedRelationName,
				.cascadeToColocated = CASCADE_TO_COLOCATED_NO_ALREADY_CASCADED,
				.suppressNoticeMessages = con->suppressNoticeMessages
			};
			TableConversionReturn *colocatedReturn = con->function(&cascadeParam);
			foreignKeyCommands = list_concat(foreignKeyCommands,
											 colocatedReturn->foreignKeyCommands);
		}
	}

	/* recreate foreign keys */
	TableConversionReturn *ret = NULL;
	if (con->conversionType == ALTER_DISTRIBUTED_TABLE)
	{
		if (con->cascadeToColocated != CASCADE_TO_COLOCATED_NO_ALREADY_CASCADED)
		{
			char *foreignKeyCommand = NULL;
			foreach_declared_ptr(foreignKeyCommand, foreignKeyCommands)
			{
				ExecuteQueryViaSPI(foreignKeyCommand, SPI_OK_UTILITY);
			}
		}
		else
		{
			ret = palloc0(sizeof(TableConversionReturn));
			ret->foreignKeyCommands = foreignKeyCommands;
		}
	}

	/* increment command counter so that next command can see the new table */
	CommandCounterIncrement();

	SetLocalEnableLocalReferenceForeignKeys(oldEnableLocalReferenceForeignKeys);

	InTableTypeConversionFunctionCall = false;

	return ret;
}


/*
 * CopyTableConversionReturnIntoCurrentContext copies given tableConversionReturn
 * into CurrentMemoryContext.
 */
static TableConversionReturn *
CopyTableConversionReturnIntoCurrentContext(TableConversionReturn *tableConversionReturn)
{
	TableConversionReturn *tableConversionReturnCopy = NULL;
	if (tableConversionReturn)
	{
		tableConversionReturnCopy = palloc0(sizeof(TableConversionReturn));
		List *copyForeignKeyCommands = NIL;
		char *foreignKeyCommand = NULL;
		foreach_declared_ptr(foreignKeyCommand, tableConversionReturn->foreignKeyCommands)
		{
			char *copyForeignKeyCommand = MemoryContextStrdup(CurrentMemoryContext,
															  foreignKeyCommand);
			copyForeignKeyCommands = lappend(copyForeignKeyCommands,
											 copyForeignKeyCommand);
		}
		tableConversionReturnCopy->foreignKeyCommands = copyForeignKeyCommands;
	}

	return tableConversionReturnCopy;
}


/*
 * ConvertTable is a wrapper for ConvertTableInternal to persist only
 * TableConversionReturn and delete all other allocations.
 */
static TableConversionReturn *
ConvertTable(TableConversionState *con)
{
	/*
	 * We do not allow alter_distributed_table and undistribute_table operations
	 * for tables with identity columns. This is because we do not have a proper way
	 * of keeping sequence states consistent across the cluster.
	 */
	ErrorIfTableHasIdentityColumn(con->relationId);

	/*
	 * when there are many partitions or colocated tables, memory usage is
	 * accumulated. Free context for each call to ConvertTable.
	 */
	MemoryContext convertTableContext =
		AllocSetContextCreate(CurrentMemoryContext,
							  "citus_convert_table_context",
							  ALLOCSET_DEFAULT_SIZES);
	MemoryContext oldContext = MemoryContextSwitchTo(convertTableContext);

	TableConversionReturn *tableConversionReturn = ConvertTableInternal(con);

	MemoryContextSwitchTo(oldContext);

	/* persist TableConversionReturn in oldContext */
	TableConversionReturn *tableConversionReturnCopy =
		CopyTableConversionReturnIntoCurrentContext(tableConversionReturn);

	/* delete convertTableContext */
	MemoryContextDelete(convertTableContext);

	return tableConversionReturnCopy;
}


/*
 * DropIndexesNotSupportedByColumnar is a helper function used during accces
 * method conversion to drop the indexes that are not supported by columnarAM.
 */
static void
DropIndexesNotSupportedByColumnar(Oid relationId, bool suppressNoticeMessages)
{
	Relation columnarRelation = RelationIdGetRelation(relationId);
	if (!RelationIsValid(columnarRelation))
	{
		ereport(ERROR, (errmsg("could not open relation with OID %u", relationId)));
	}

	List *indexIdList = RelationGetIndexList(columnarRelation);

	/*
	 * Immediately close the relation since we might execute ALTER TABLE
	 * for that relation.
	 */
	RelationClose(columnarRelation);

	Oid indexId = InvalidOid;
	foreach_declared_oid(indexId, indexIdList)
	{
		char *indexAmName = GetIndexAccessMethodName(indexId);
		if (extern_ColumnarSupportsIndexAM(indexAmName))
		{
			continue;
		}

		if (!suppressNoticeMessages)
		{
			ereport(NOTICE, (errmsg("unsupported access method for index %s "
									"on columnar table %s, given index and "
									"the constraint depending on the index "
									"(if any) will be dropped",
									get_rel_name(indexId),
									generate_qualified_relation_name(relationId))));
		}

		Oid constraintId = get_index_constraint(indexId);
		if (OidIsValid(constraintId))
		{
			/* index is implied by a constraint, so drop the constraint itself */
			DropConstraintRestrict(relationId, constraintId);
		}
		else
		{
			DropIndexRestrict(indexId);
		}
	}
}


/*
 * GetIndexAccessMethodName returns access method name of index with indexId.
 * If there is no such index, then errors out.
 */
static char *
GetIndexAccessMethodName(Oid indexId)
{
	/* fetch pg_class tuple of the index relation */
	HeapTuple indexTuple = SearchSysCache1(RELOID, ObjectIdGetDatum(indexId));
	if (!HeapTupleIsValid(indexTuple))
	{
		ereport(ERROR, (errmsg("index with oid %u does not exist", indexId)));
	}

	Form_pg_class indexForm = (Form_pg_class) GETSTRUCT(indexTuple);
	Oid indexAMId = indexForm->relam;
	ReleaseSysCache(indexTuple);

	char *indexAmName = get_am_name(indexAMId);
	if (!indexAmName)
	{
		ereport(ERROR, (errmsg("access method with oid %u does not exist", indexAMId)));
	}

	return indexAmName;
}


/*
 * DropConstraintRestrict drops the constraint with constraintId by using spi.
 */
static void
DropConstraintRestrict(Oid relationId, Oid constraintId)
{
	char *qualifiedRelationName = generate_qualified_relation_name(relationId);
	char *constraintName = get_constraint_name(constraintId);
	const char *quotedConstraintName = quote_identifier(constraintName);
	StringInfo dropConstraintCommand = makeStringInfo();
	appendStringInfo(dropConstraintCommand, "ALTER TABLE %s DROP CONSTRAINT %s RESTRICT;",
					 qualifiedRelationName, quotedConstraintName);
	ExecuteQueryViaSPI(dropConstraintCommand->data, SPI_OK_UTILITY);
}


/*
 * DropIndexRestrict drops the index with indexId by using spi.
 */
static void
DropIndexRestrict(Oid indexId)
{
	char *qualifiedIndexName = generate_qualified_relation_name(indexId);
	StringInfo dropIndexCommand = makeStringInfo();
	appendStringInfo(dropIndexCommand, "DROP INDEX %s RESTRICT;", qualifiedIndexName);
	ExecuteQueryViaSPI(dropIndexCommand->data, SPI_OK_UTILITY);
}


/*
 * EnsureTableNotReferencing checks if the table has a reference to another
 * table and errors if it is.
 */
void
EnsureTableNotReferencing(Oid relationId, char conversionType)
{
	if (TableReferencing(relationId))
	{
		if (conversionType == UNDISTRIBUTE_TABLE)
		{
			char *qualifiedRelationName = generate_qualified_relation_name(relationId);
			ereport(ERROR, (errmsg("cannot complete operation "
								   "because table %s has a foreign key",
								   get_rel_name(relationId)),
							errhint(UNDISTRIBUTE_TABLE_CASCADE_HINT,
									qualifiedRelationName,
									qualifiedRelationName)));
		}
		else
		{
			ereport(ERROR, (errmsg("cannot complete operation "
								   "because table %s has a foreign key",
								   get_rel_name(relationId))));
		}
	}
}


/*
 * EnsureTableNotReferenced checks if the table is referenced by another
 * table and errors if it is.
 */
void
EnsureTableNotReferenced(Oid relationId, char conversionType)
{
	if (TableReferenced(relationId))
	{
		if (conversionType == UNDISTRIBUTE_TABLE)
		{
			char *qualifiedRelationName = generate_qualified_relation_name(relationId);
			ereport(ERROR, (errmsg("cannot complete operation "
								   "because table %s is referenced by a foreign key",
								   get_rel_name(relationId)),
							errhint(UNDISTRIBUTE_TABLE_CASCADE_HINT,
									qualifiedRelationName,
									qualifiedRelationName)));
		}
		else
		{
			ereport(ERROR, (errmsg("cannot complete operation "
								   "because table %s is referenced by a foreign key",
								   get_rel_name(relationId))));
		}
	}
}


/*
 * EnsureTableNotForeign checks if the table is a foreign table and errors
 * if it is.
 */
void
EnsureTableNotForeign(Oid relationId)
{
	if (IsForeignTable(relationId))
	{
		ereport(ERROR, (errmsg("cannot complete operation "
							   "because it is a foreign table")));
	}
}


/*
 * EnsureTableNotPartition checks if the table is a partition of another
 * table and errors if it is.
 */
void
EnsureTableNotPartition(Oid relationId)
{
	if (PartitionTable(relationId))
	{
		Oid parentRelationId = PartitionParentOid(relationId);
		char *parentRelationName = get_rel_name(parentRelationId);
		ereport(ERROR, (errmsg("cannot complete operation "
							   "because table is a partition"),
						errhint("the parent table is \"%s\"",
								parentRelationName)));
	}
}


/*
 * ErrorIfColocateWithTenantTable errors out if given colocateWith text refers to
 * a tenant table.
 */
void
ErrorIfColocateWithTenantTable(char *colocateWith)
{
	if (colocateWith != NULL &&
		!IsColocateWithDefault(colocateWith) &&
		!IsColocateWithNone(colocateWith))
	{
		text *colocateWithTableNameText = cstring_to_text(colocateWith);
		Oid colocateWithTableId = ResolveRelationId(colocateWithTableNameText, false);
		ErrorIfTenantTable(colocateWithTableId,
						   TenantOperationNames[TENANT_COLOCATE_WITH]);
	}
}


TableConversionState *
CreateTableConversion(TableConversionParameters *params)
{
	TableConversionState *con = palloc0(sizeof(TableConversionState));

	con->conversionType = params->conversionType;
	con->relationId = params->relationId;
	con->distributionColumn = params->distributionColumn;
	con->shardCountIsNull = params->shardCountIsNull;
	con->shardCount = params->shardCount;
	con->colocateWith = params->colocateWith;
	con->accessMethod = params->accessMethod;
	con->cascadeToColocated = params->cascadeToColocated;
	con->cascadeViaForeignKeys = params->cascadeViaForeignKeys;
	con->suppressNoticeMessages = params->suppressNoticeMessages;

	Relation relation = try_relation_open(con->relationId, ExclusiveLock);
	if (relation == NULL)
	{
		ereport(ERROR, (errmsg("cannot complete operation "
							   "because no such table exists")));
	}


	relation_close(relation, NoLock);
	con->distributionKey =
		BuildDistributionKeyFromColumnName(con->relationId, con->distributionColumn,
										   NoLock);

	con->originalAccessMethod = NULL;
	if (!PartitionedTable(con->relationId) && !IsForeignTable(con->relationId))
	{
		HeapTuple amTuple = SearchSysCache1(AMOID, ObjectIdGetDatum(
												relation->rd_rel->relam));
		if (!HeapTupleIsValid(amTuple))
		{
			ereport(ERROR, (errmsg("cache lookup failed for access method %d",
								   relation->rd_rel->relam)));
		}
		Form_pg_am amForm = (Form_pg_am) GETSTRUCT(amTuple);
		con->originalAccessMethod = NameStr(amForm->amname);
		ReleaseSysCache(amTuple);
	}


	con->colocatedTableList = NIL;
	if (IsCitusTableType(con->relationId, DISTRIBUTED_TABLE))
	{
		con->originalDistributionKey = DistPartitionKey(con->relationId);

		CitusTableCacheEntry *cacheEntry = GetCitusTableCacheEntry(con->relationId);
		con->originalShardCount = cacheEntry->shardIntervalArrayLength;

		List *colocatedTableList = ColocatedTableList(con->relationId);

		/*
		 * we will not add partition tables to the colocatedTableList
		 * since they will be handled separately.
		 */
		Oid colocatedTableId = InvalidOid;
		foreach_declared_oid(colocatedTableId, colocatedTableList)
		{
			if (PartitionTable(colocatedTableId))
			{
				continue;
			}
			con->colocatedTableList = lappend_oid(con->colocatedTableList,
												  colocatedTableId);
		}

		/* sort the oids to avoid deadlock */
		con->colocatedTableList = SortList(con->colocatedTableList, CompareOids);
	}

	/* find relation and schema names */
	con->relationName = get_rel_name(con->relationId);
	con->schemaId = get_rel_namespace(con->relationId);
	con->schemaName = get_namespace_name(con->schemaId);

	/* calculate a temp name for the new table */
	con->tempName = pstrdup(con->relationName);
	con->hashOfName = hash_any((unsigned char *) con->tempName, strlen(con->tempName));
	AppendShardIdToName(&con->tempName, con->hashOfName);

	if (con->conversionType == UNDISTRIBUTE_TABLE)
	{
		con->function = &UndistributeTable;
	}
	else if (con->conversionType == ALTER_DISTRIBUTED_TABLE)
	{
		con->function = &AlterDistributedTable;
	}
	else if (con->conversionType == ALTER_TABLE_SET_ACCESS_METHOD)
	{
		con->function = &AlterTableSetAccessMethod;
	}

	return con;
}


/*
 * CreateDistributedTableLike distributes the new table in con parameter
 * like the old one. It checks the distribution column, colocation and
 * shard count and if they are not changed sets them to the old table's values.
 */
void
CreateDistributedTableLike(TableConversionState *con)
{
	Var *newDistributionKey =
		con->distributionColumn ? con->distributionKey : con->originalDistributionKey;

	char *newColocateWith = con->colocateWith;
	if (con->colocateWith == NULL)
	{
		/*
		 * If the new distribution column and the old one have the same data type
		 * and the shard_count parameter is null (which means shard count will not
		 * change) we can create the new table in the same colocation as the old one.
		 * In this case we set the new table's colocate_with value as the old table
		 * so we don't even change the colocation id of the table during conversion.
		 */
		if (con->originalDistributionKey->vartype == newDistributionKey->vartype &&
			con->shardCountIsNull)
		{
			newColocateWith =
				quote_qualified_identifier(con->schemaName, con->relationName);
		}
		else
		{
			newColocateWith = "default";
		}
	}
	int newShardCount = 0;
	if (con->shardCountIsNull)
	{
		newShardCount = con->originalShardCount;
	}
	else
	{
		newShardCount = con->shardCount;
	}

	/*
	 * To get the correct column name, we use the original relation id, not the
	 * new relation id. The reason is that the cached attributes of the original
	 * and newly created tables are not the same if the original table has
	 * dropped columns (dropped columns are still present in the attribute cache)
	 * Detailed example in https://github.com/citusdata/citus/pull/6387
	 */
	char *distributionColumnName =
		ColumnToColumnName(con->relationId, (Node *) newDistributionKey);

	Oid originalRelationId = con->relationId;
	if (con->originalDistributionKey != NULL && PartitionTable(originalRelationId))
	{
		/*
		 * Due to dropped columns, the partition tables might have different
		 * distribution keys than their parents, see issue #5123 for details.
		 *
		 * At this point, we get the partitioning information from the
		 * originalRelationId, but we get the distribution key for newRelationId.
		 *
		 * We have to do this, because the newRelationId is just a placeholder
		 * at this moment, but that's going to be the table in pg_dist_partition.
		 */
		Oid parentRelationId = PartitionParentOid(originalRelationId);
		Var *parentDistKey = DistPartitionKeyOrError(parentRelationId);
		distributionColumnName =
			ColumnToColumnName(parentRelationId, (Node *) parentDistKey);
	}

	char partitionMethod = PartitionMethod(con->relationId);

	CreateDistributedTable(con->newRelationId, distributionColumnName, partitionMethod,
						   newShardCount, true, newColocateWith);
}


/*
 * CreateCitusTableLike converts the new table to the Citus table type
 * of the old table.
 */
void
CreateCitusTableLike(TableConversionState *con)
{
	if (IsCitusTableType(con->relationId, DISTRIBUTED_TABLE))
	{
		if (IsCitusTableType(con->relationId, SINGLE_SHARD_DISTRIBUTED))
		{
			ColocationParam colocationParam = {
				.colocationParamType = COLOCATE_WITH_TABLE_LIKE_OPT,
				.colocateWithTableName = quote_qualified_identifier(con->schemaName,
																	con->relationName)
			};
			CreateSingleShardTable(con->newRelationId, colocationParam);
		}
		else
		{
			CreateDistributedTableLike(con);
		}
	}
	else if (IsCitusTableType(con->relationId, REFERENCE_TABLE))
	{
		CreateReferenceTable(con->newRelationId);
	}
	else if (IsCitusTableType(con->relationId, CITUS_LOCAL_TABLE))
	{
		CitusTableCacheEntry *entry = GetCitusTableCacheEntry(con->relationId);
		bool autoConverted = entry->autoConverted;
		bool cascade = false;
		CreateCitusLocalTable(con->newRelationId, cascade, autoConverted);

		/*
		 * creating Citus local table adds a shell table on top
		 * so we need its oid now
		 */
		con->newRelationId = get_relname_relid(con->tempName, con->schemaId);
	}
}


/*
 * ErrorIfUnsupportedCascadeObjects gets oid of a relation, finds the objects
 * that dropping this relation cascades into and errors if there are any extensions
 * that would be dropped.
 */
static void
ErrorIfUnsupportedCascadeObjects(Oid relationId)
{
	HTAB *nodeMap = CreateSimpleHashSetWithName(Oid, "object dependency map (oid)");

	bool unsupportedObjectInDepGraph =
		DoesCascadeDropUnsupportedObject(RelationRelationId, relationId, nodeMap);

	if (unsupportedObjectInDepGraph)
	{
		ereport(ERROR, (errmsg("cannot alter table because an extension depends on it")));
	}
}


/*
 * DoesCascadeDropUnsupportedObject walks through the objects that depend on the
 * object with object id and returns true if it finds any unsupported objects.
 *
 * This function only checks extensions as unsupported objects.
 *
 * Extension dependency is different than the rest. If an object depends on an extension
 * dropping the object would drop the extension too.
 * So we check with IsAnyObjectAddressOwnedByExtension function.
 */
static bool
DoesCascadeDropUnsupportedObject(Oid classId, Oid objectId, HTAB *nodeMap)
{
	bool found = false;
	hash_search(nodeMap, &objectId, HASH_ENTER, &found);

	if (found)
	{
		return false;
	}

	ObjectAddress *objectAddress = palloc0(sizeof(ObjectAddress));
	ObjectAddressSet(*objectAddress, classId, objectId);
	if (IsAnyObjectAddressOwnedByExtension(list_make1(objectAddress), NULL))
	{
		return true;
	}

	Oid targetObjectClassId = classId;
	Oid targetObjectId = objectId;
	List *dependencyTupleList = GetPgDependTuplesForDependingObjects(targetObjectClassId,
																	 targetObjectId);

	HeapTuple depTup = NULL;
	foreach_declared_ptr(depTup, dependencyTupleList)
	{
		Form_pg_depend pg_depend = (Form_pg_depend) GETSTRUCT(depTup);

		Oid dependingOid = InvalidOid;
		Oid dependingClassId = InvalidOid;

		if (pg_depend->classid == RewriteRelationId)
		{
			dependingOid = GetDependingView(pg_depend);
			dependingClassId = RelationRelationId;
		}
		else
		{
			dependingOid = pg_depend->objid;
			dependingClassId = pg_depend->classid;
		}

		if (DoesCascadeDropUnsupportedObject(dependingClassId, dependingOid, nodeMap))
		{
			return true;
		}
	}
	return false;
}


/*
 * GetViewCreationCommandsOfTable takes a table oid generates the CREATE VIEW
 * commands for views that depend to the given table. This includes the views
 * that recursively depend on the table too.
 */
List *
GetViewCreationCommandsOfTable(Oid relationId)
{
	List *views = GetDependingViews(relationId);

	List *commands = NIL;

	Oid viewOid = InvalidOid;
	foreach_declared_oid(viewOid, views)
	{
		StringInfo query = makeStringInfo();

		/* See comments on CreateMaterializedViewDDLCommand for its limitations */
		if (get_rel_relkind(viewOid) == RELKIND_MATVIEW)
		{
			ErrorIfMatViewSizeExceedsTheLimit(viewOid);

			char *matViewCreateCommands = CreateMaterializedViewDDLCommand(viewOid);
			appendStringInfoString(query, matViewCreateCommands);
		}
		else
		{
			char *viewCreateCommand = CreateViewDDLCommand(viewOid);
			appendStringInfoString(query, viewCreateCommand);
		}

		char *alterViewCommmand = AlterViewOwnerCommand(viewOid);
		appendStringInfoString(query, alterViewCommmand);

		commands = lappend(commands, query->data);
	}

	return commands;
}


/*
 * WrapTableDDLCommands takes a list of command strings and wraps them
 * in TableDDLCommand structs.
 */
static List *
WrapTableDDLCommands(List *commandStrings)
{
	List *tableDDLCommands = NIL;

	char *command = NULL;
	foreach_declared_ptr(command, commandStrings)
	{
		tableDDLCommands = lappend(tableDDLCommands, makeTableDDLCommandString(command));
	}

	return tableDDLCommands;
}


/*
 * ErrorIfMatViewSizeExceedsTheLimit takes the oid of a materialized view and errors
 * out if the size of the matview exceeds the limit set by the GUC
 * citus.max_matview_size_to_auto_recreate.
 */
static void
ErrorIfMatViewSizeExceedsTheLimit(Oid matViewOid)
{
	if (MaxMatViewSizeToAutoRecreate >= 0)
	{
		/* if it's below 0, it means the user has removed the limit */
		Datum relSizeDatum = DirectFunctionCall1(pg_total_relation_size,
												 ObjectIdGetDatum(matViewOid));
		uint64 matViewSize = DatumGetInt64(relSizeDatum);

		/* convert from MB to bytes */
		uint64 limitSizeInBytes = MaxMatViewSizeToAutoRecreate * 1024L * 1024L;

		if (matViewSize > limitSizeInBytes)
		{
			ereport(ERROR, (errmsg("size of the materialized view %s exceeds "
								   "citus.max_matview_size_to_auto_recreate "
								   "(currently %d MB)", get_rel_name(matViewOid),
								   MaxMatViewSizeToAutoRecreate),
							errdetail("Citus restricts automatically recreating "
									  "materialized views that are larger than the "
									  "limit, because it could take too long."),
							errhint(
								"Consider increasing the size limit by setting "
								"citus.max_matview_size_to_auto_recreate; "
								"or you can remove the limit by setting it to -1")));
		}
	}
}


/*
 * CreateMaterializedViewDDLCommand creates the command to create materialized view.
 * Note that this function doesn't support
 * - Aliases
 * - Storage parameters
 * - Tablespace
 * - WITH [NO] DATA
 * options for the given materialized view. Parser functions for materialized views
 * should be added to handle them.
 *
 * Related issue: https://github.com/citusdata/citus/issues/5968
 */
static char *
CreateMaterializedViewDDLCommand(Oid matViewOid)
{
	StringInfo query = makeStringInfo();

	char *qualifiedViewName = generate_qualified_relation_name(matViewOid);

	/* here we need to get the access method of the view to recreate it */
	char *accessMethodName = GetAccessMethodForMatViewIfExists(matViewOid);

	appendStringInfo(query, "CREATE MATERIALIZED VIEW %s ", qualifiedViewName);

	if (accessMethodName)
	{
		appendStringInfo(query, "USING %s ", accessMethodName);
	}

	/*
	 * Set search_path to NIL so that all objects outside of pg_catalog will be
	 * schema-prefixed.
	 */
	int saveNestLevel = PushEmptySearchPath();

	/*
	 * Push the transaction snapshot to be able to get vief definition with pg_get_viewdef
	 */
	PushActiveSnapshot(GetTransactionSnapshot());

	Datum viewDefinitionDatum = DirectFunctionCall1(pg_get_viewdef,
													ObjectIdGetDatum(matViewOid));
	char *viewDefinition = TextDatumGetCString(viewDefinitionDatum);

	PopActiveSnapshot();
	PopEmptySearchPath(saveNestLevel);

	appendStringInfo(query, "AS %s", viewDefinition);

	return query->data;
}


/*
 * ReplaceTable replaces the source table with the target table.
 * It moves all the rows of the source table to target table with INSERT SELECT.
 * Changes the dependencies of the sequences owned by source table to target table.
 * Then drops the source table and renames the target table to source tables name.
 *
 * Source and target tables need to be in the same schema and have the same columns.
 */
void
ReplaceTable(Oid sourceId, Oid targetId, List *justBeforeDropCommands,
			 bool suppressNoticeMessages)
{
	char *sourceName = get_rel_name(sourceId);
	char *qualifiedSourceName = generate_qualified_relation_name(sourceId);
	char *qualifiedTargetName = generate_qualified_relation_name(targetId);

	StringInfo query = makeStringInfo();

	if (!PartitionedTable(sourceId) && !IsForeignTable(sourceId))
	{
		if (!suppressNoticeMessages)
		{
			ereport(NOTICE, (errmsg("moving the data of %s", qualifiedSourceName)));
		}

		if (!HasAnyGeneratedStoredColumns(sourceId))
		{
			/*
			 * Relation has no GENERATED STORED columns, copy the table via plain
			 * "INSERT INTO .. SELECT *"".
			 */
			appendStringInfo(query, "INSERT INTO %s SELECT * FROM %s",
							 qualifiedTargetName, qualifiedSourceName);
		}
		else
		{
			/*
			 * Skip columns having GENERATED ALWAYS AS (...) STORED expressions
			 * since Postgres doesn't allow inserting into such columns.
			 * This is not bad since Postgres would already generate such columns.
			 * Note that here we intentionally don't skip columns having DEFAULT
			 * expressions since user might have inserted non-default values.
			 */
			List *nonStoredColumnNameList = GetNonGeneratedStoredColumnNameList(sourceId);
			char *insertColumnString = StringJoin(nonStoredColumnNameList, ',');
			appendStringInfo(query,
							 "INSERT INTO %s (%s) OVERRIDING SYSTEM VALUE SELECT %s FROM %s",
							 qualifiedTargetName, insertColumnString,
							 insertColumnString, qualifiedSourceName);
		}

		ExecuteQueryViaSPI(query->data, SPI_OK_INSERT);
	}

	/*
	 * Modify regular sequence dependencies (sequences marked as DEPENDENCY_AUTO)
	 */
	List *ownedSequences = getOwnedSequences_internal(sourceId, 0, DEPENDENCY_AUTO);
	Oid sequenceOid = InvalidOid;
	foreach_declared_oid(sequenceOid, ownedSequences)
	{
		changeDependencyFor(RelationRelationId, sequenceOid,
							RelationRelationId, sourceId, targetId);

		/*
		 * Skip if we cannot sync metadata for target table.
		 * Checking only for the target table is sufficient since we will
		 * anyway drop the source table even if it was a Citus table that
		 * has metadata on MX workers.
		 */
		if (ShouldSyncTableMetadata(targetId))
		{
			char *qualifiedSequenceName = generate_qualified_relation_name(sequenceOid);
			char *workerChangeSequenceDependencyCommand =
				CreateWorkerChangeSequenceDependencyCommand(qualifiedSequenceName,
															qualifiedSourceName,
															qualifiedTargetName);
			SendCommandToWorkersWithMetadata(workerChangeSequenceDependencyCommand);
		}
		else if (ShouldSyncTableMetadata(sourceId))
		{
			/*
			 * We are converting a citus local table to a distributed/reference table,
			 * so we should prevent dropping the sequence on the table. Otherwise, we'd
			 * lose track of the previous changes in the sequence.
			 */
			char *command = WorkerDropSequenceDependencyCommand(sourceId);
			SendCommandToWorkersWithMetadata(command);
		}
	}

	char *justBeforeDropCommand = NULL;
	foreach_declared_ptr(justBeforeDropCommand, justBeforeDropCommands)
	{
		ExecuteQueryViaSPI(justBeforeDropCommand, SPI_OK_UTILITY);
	}

	if (!suppressNoticeMessages)
	{
		ereport(NOTICE, (errmsg("dropping the old %s", qualifiedSourceName)));
	}

	resetStringInfo(query);
	appendStringInfo(query, "DROP %sTABLE %s CASCADE",
					 IsForeignTable(sourceId) ? "FOREIGN " : "",
					 qualifiedSourceName);
	ExecuteQueryViaSPI(query->data, SPI_OK_UTILITY);

	if (!suppressNoticeMessages)
	{
		ereport(NOTICE, (errmsg("renaming the new table to %s", qualifiedSourceName)));
	}

	resetStringInfo(query);
	appendStringInfo(query, "ALTER TABLE %s RENAME TO %s",
					 qualifiedTargetName,
					 quote_identifier(sourceName));
	ExecuteQueryViaSPI(query->data, SPI_OK_UTILITY);
}


/*
 * HasAnyGeneratedStoredColumns decides if relation has any columns that we
 * might need to copy the data of when replacing table.
 */
static bool
HasAnyGeneratedStoredColumns(Oid relationId)
{
	return list_length(GetNonGeneratedStoredColumnNameList(relationId)) > 0;
}


/*
 * GetNonGeneratedStoredColumnNameList returns a list of column names for
 * columns not having GENERATED ALWAYS AS (...) STORED expressions.
 */
static List *
GetNonGeneratedStoredColumnNameList(Oid relationId)
{
	List *nonStoredColumnNameList = NIL;

	Relation relation = relation_open(relationId, AccessShareLock);
	TupleDesc tupleDescriptor = RelationGetDescr(relation);
	for (int columnIndex = 0; columnIndex < tupleDescriptor->natts; columnIndex++)
	{
		Form_pg_attribute currentColumn = TupleDescAttr(tupleDescriptor, columnIndex);
		if (currentColumn->attisdropped)
		{
			/* skip dropped columns */
			continue;
		}

		if (currentColumn->attgenerated == ATTRIBUTE_GENERATED_STORED)
		{
			continue;
		}

		const char *quotedColumnName = quote_identifier(NameStr(currentColumn->attname));
		nonStoredColumnNameList = lappend(nonStoredColumnNameList,
										  pstrdup(quotedColumnName));
	}

	relation_close(relation, NoLock);

	return nonStoredColumnNameList;
}


/*
 * CheckAlterDistributedTableConversionParameters errors for the cases where
 * alter_distributed_table UDF wouldn't work.
 */
void
CheckAlterDistributedTableConversionParameters(TableConversionState *con)
{
	/* Changing nothing is not allowed */
	if (con->distributionColumn == NULL && con->shardCountIsNull &&
		con->colocateWith == NULL && con->cascadeToColocated != CASCADE_TO_COLOCATED_YES)
	{
		ereport(ERROR, (errmsg("you have to specify at least one of the "
							   "distribution_column, shard_count or "
							   "colocate_with parameters")));
	}

	/* check if the parameters in this conversion are given and same with table's properties */
	bool sameDistColumn = false;
	if (con->distributionColumn != NULL &&
		equal(con->distributionKey, con->originalDistributionKey))
	{
		sameDistColumn = true;
	}

	bool sameShardCount = false;
	if (!con->shardCountIsNull && con->originalShardCount == con->shardCount)
	{
		sameShardCount = true;
	}

	bool sameColocateWith = false;
	if (con->colocateWith != NULL && strcmp(con->colocateWith, "default") != 0 &&
		strcmp(con->colocateWith, "none") != 0)
	{
		/* check if already colocated with colocate_with */
		Oid colocatedTableOid = InvalidOid;
		text *colocateWithText = cstring_to_text(con->colocateWith);
		Oid colocateWithTableOid = ResolveRelationId(colocateWithText, false);
		foreach_declared_oid(colocatedTableOid, con->colocatedTableList)
		{
			if (colocateWithTableOid == colocatedTableOid)
			{
				sameColocateWith = true;
				break;
			}
		}

		/*
		 * already found colocateWithTableOid so let's check if
		 * colocate_with table is a distributed table
		 */
		if (!IsCitusTableType(colocateWithTableOid, DISTRIBUTED_TABLE))
		{
			ereport(ERROR, (errmsg("cannot colocate with %s because "
								   "it is not a distributed table",
								   con->colocateWith)));
		}
		else if (IsCitusTableType(colocateWithTableOid, SINGLE_SHARD_DISTRIBUTED))
		{
			ereport(ERROR, (errmsg("cannot colocate with %s because "
								   "it is a single shard distributed table",
								   con->colocateWith)));
		}
	}

	/* shard_count:=0 is not allowed */
	if (!con->shardCountIsNull && con->shardCount == 0)
	{
		ereport(ERROR, (errmsg("shard_count cannot be 0"),
						errhint("if you no longer want this to be a "
								"distributed table you can try "
								"undistribute_table() function")));
	}

	if (con->cascadeToColocated == CASCADE_TO_COLOCATED_YES &&
		con->distributionColumn != NULL)
	{
		ereport(ERROR, (errmsg("distribution_column cannot be "
							   "cascaded to colocated tables")));
	}
	if (con->cascadeToColocated == CASCADE_TO_COLOCATED_YES && con->shardCountIsNull &&
		con->colocateWith == NULL)
	{
		ereport(ERROR, (errmsg("shard_count or colocate_with is necessary "
							   "for cascading to colocated tables")));
	}

	/*
	 * if every parameter is either not given or already the
	 * same then give error
	 */
	if ((con->distributionColumn == NULL || sameDistColumn) &&
		(con->shardCountIsNull || sameShardCount) &&
		(con->colocateWith == NULL || sameColocateWith))
	{
		ereport(ERROR, (errmsg("this call doesn't change any properties of the table"),
						errhint("check citus_tables view to see current "
								"properties of the table")));
	}
	if (con->cascadeToColocated == CASCADE_TO_COLOCATED_YES &&
		con->colocateWith != NULL &&
		strcmp(con->colocateWith, "none") == 0)
	{
		ereport(ERROR, (errmsg("colocate_with := 'none' cannot be "
							   "cascaded to colocated tables")));
	}

	int colocatedTableCount = list_length(con->colocatedTableList) - 1;
	if (colocatedTableCount > 0 && !con->shardCountIsNull && !sameShardCount &&
		con->cascadeToColocated == CASCADE_TO_COLOCATED_UNSPECIFIED)
	{
		ereport(ERROR, (errmsg("cascade_to_colocated parameter is necessary"),
						errdetail("this table is colocated with some other tables"),
						errhint("cascade_to_colocated := false will break the "
								"current colocation, cascade_to_colocated := true "
								"will change the shard count of colocated tables "
								"too.")));
	}

	if (con->colocateWith != NULL && strcmp(con->colocateWith, "default") != 0 &&
		strcmp(con->colocateWith, "none") != 0)
	{
		text *colocateWithText = cstring_to_text(con->colocateWith);
		Oid colocateWithTableOid = ResolveRelationId(colocateWithText, false);
		CitusTableCacheEntry *colocateWithTableCacheEntry =
			GetCitusTableCacheEntry(colocateWithTableOid);
		int colocateWithTableShardCount =
			colocateWithTableCacheEntry->shardIntervalArrayLength;

		if (!con->shardCountIsNull && con->shardCount != colocateWithTableShardCount)
		{
			ereport(ERROR, (errmsg("shard_count cannot be different than the shard "
								   "count of the table in colocate_with"),
							errhint("if no shard_count is specified shard count "
									"will be same with colocate_with table's")));
		}

		if (colocateWithTableShardCount != con->originalShardCount)
		{
			/*
			 * shardCount is either 0 or already same with colocateWith table's
			 * It's ok to set shardCountIsNull to false because we assume giving a table
			 * to colocate with and no shard count is the same with giving colocate_with
			 * table's shard count if it is different than the original.
			 * So it is almost like the shard_count parameter was given by the user.
			 */
			con->shardCount = colocateWithTableShardCount;
			con->shardCountIsNull = false;
		}

		Var *colocateWithPartKey = DistPartitionKey(colocateWithTableOid);

		if (colocateWithPartKey == NULL)
		{
			/* this should never happen */
			ereport(ERROR, (errmsg("cannot colocate %s with %s because %s doesn't have a "
								   "distribution column",
								   con->relationName, con->colocateWith,
								   con->colocateWith)));
		}
		else if (con->distributionColumn &&
				 colocateWithPartKey->vartype != con->distributionKey->vartype)
		{
			ereport(ERROR, (errmsg("cannot colocate with %s and change distribution "
								   "column to %s because data type of column %s is "
								   "different then the distribution column of the %s",
								   con->colocateWith, con->distributionColumn,
								   con->distributionColumn, con->colocateWith)));
		}
		else if (!con->distributionColumn &&
				 colocateWithPartKey->vartype != con->originalDistributionKey->vartype)
		{
			ereport(ERROR, (errmsg("cannot colocate with %s because data type of its "
								   "distribution column is different than %s",
								   con->colocateWith, con->relationName)));
		}
	}

	if (!con->suppressNoticeMessages)
	{
		/* Notices for no operation UDF calls */
		if (sameDistColumn)
		{
			ereport(NOTICE, (errmsg("table is already distributed by %s",
									con->distributionColumn)));
		}

		if (sameShardCount)
		{
			ereport(NOTICE, (errmsg("shard count of the table is already %d",
									con->shardCount)));
		}

		if (sameColocateWith)
		{
			ereport(NOTICE, (errmsg("table is already colocated with %s",
									con->colocateWith)));
		}
	}
}


/*
 * CreateWorkerChangeSequenceDependencyCommand creates and returns a
 * worker_change_sequence_dependency query with the parameters.
 */
static char *
CreateWorkerChangeSequenceDependencyCommand(char *qualifiedSequeceName,
											char *qualifiedSourceName,
											char *qualifiedTargetName)
{
	StringInfo query = makeStringInfo();
	appendStringInfo(query, "SELECT worker_change_sequence_dependency(%s, %s, %s)",
					 quote_literal_cstr(qualifiedSequeceName),
					 quote_literal_cstr(qualifiedSourceName),
					 quote_literal_cstr(qualifiedTargetName));

	return query->data;
}


/*
 * GetAccessMethodForMatViewIfExists returns if there's an access method
 * set to the view with the given oid. Returns NULL otherwise.
 */
static char *
GetAccessMethodForMatViewIfExists(Oid viewOid)
{
	char *accessMethodName = NULL;
	Relation relation = try_relation_open(viewOid, AccessShareLock);
	if (relation == NULL)
	{
		ereport(ERROR, (errmsg("cannot complete operation "
							   "because no such view exists")));
	}

	Oid accessMethodOid = relation->rd_rel->relam;
	if (OidIsValid(accessMethodOid))
	{
		accessMethodName = get_am_name(accessMethodOid);
	}
	relation_close(relation, NoLock);

	return accessMethodName;
}


/*
 * WillRecreateForeignKeyToReferenceTable checks if the table of relationId has any foreign
 * key to a reference table, if conversion will be cascaded to colocated table this function
 * also checks if any of the colocated tables have a foreign key to a reference table too
 */
bool
WillRecreateForeignKeyToReferenceTable(Oid relationId,
									   CascadeToColocatedOption cascadeOption)
{
	if (cascadeOption == CASCADE_TO_COLOCATED_NO ||
		cascadeOption == CASCADE_TO_COLOCATED_UNSPECIFIED)
	{
		return HasForeignKeyToReferenceTable(relationId);
	}
	else if (cascadeOption == CASCADE_TO_COLOCATED_YES)
	{
		List *colocatedTableList = ColocatedTableList(relationId);
		Oid colocatedTableOid = InvalidOid;
		foreach_declared_oid(colocatedTableOid, colocatedTableList)
		{
			if (HasForeignKeyToReferenceTable(colocatedTableOid))
			{
				return true;
			}
		}
	}
	return false;
}


/*
 * WarningsForDroppingForeignKeysWithDistributedTables gives warnings for the
 * foreign keys that will be dropped because formerly colocated distributed tables
 * are not colocated.
 */
void
WarningsForDroppingForeignKeysWithDistributedTables(Oid relationId)
{
	int flags = INCLUDE_REFERENCING_CONSTRAINTS | INCLUDE_DISTRIBUTED_TABLES;
	List *referencingForeingKeys = GetForeignKeyOids(relationId, flags);
	flags = INCLUDE_REFERENCED_CONSTRAINTS | INCLUDE_DISTRIBUTED_TABLES;
	List *referencedForeignKeys = GetForeignKeyOids(relationId, flags);

	List *foreignKeys = list_concat(referencingForeingKeys, referencedForeignKeys);

	Oid foreignKeyOid = InvalidOid;
	foreach_declared_oid(foreignKeyOid, foreignKeys)
	{
		ereport(WARNING, (errmsg("foreign key %s will be dropped",
								 get_constraint_name(foreignKeyOid))));
	}
}


/*
 * ExecuteQueryViaSPI connects to SPI, executes the query and checks if it
 * returned the OK value and finishes the SPI connection
 */
void
ExecuteQueryViaSPI(char *query, int SPIOK)
{
	int spiResult = SPI_connect();
	if (spiResult != SPI_OK_CONNECT)
	{
		ereport(ERROR, (errmsg("could not connect to SPI manager")));
	}

	spiResult = SPI_execute(query, false, 0);
	if (spiResult != SPIOK)
	{
		ereport(ERROR, (errmsg("could not run SPI query")));
	}

	spiResult = SPI_finish();
	if (spiResult != SPI_OK_FINISH)
	{
		ereport(ERROR, (errmsg("could not finish SPI connection")));
	}
}


/*
 * ExecuteAndLogQueryViaSPI is a wrapper around ExecuteQueryViaSPI, that logs
 * the query to be executed, with the given log level.
 */
void
ExecuteAndLogQueryViaSPI(char *query, int SPIOK, int logLevel)
{
	ereport(logLevel, (errmsg("executing \"%s\"", query)));

	ExecuteQueryViaSPI(query, SPIOK);
}


/*
 * SwitchToSequentialAndLocalExecutionIfRelationNameTooLong generates the longest shard name
 * on the shards of a distributed table, and if exceeds the limit switches to sequential and
 * local execution to prevent self-deadlocks.
 *
 * In case of a RENAME, the relation name parameter should store the new table name, so
 * that the function can generate shard names of the renamed relations
 */
void
SwitchToSequentialAndLocalExecutionIfRelationNameTooLong(Oid relationId,
														 char *finalRelationName)
{
	if (!IsCitusTable(relationId))
	{
		return;
	}

	if (ShardIntervalCount(relationId) == 0)
	{
		/*
		 * Relation has no shards, so we cannot run into "long shard relation
		 * name" issue.
		 */
		return;
	}

	char *longestShardName = GetLongestShardName(relationId, finalRelationName);
	bool switchedToSequentialAndLocalExecution =
		SwitchToSequentialAndLocalExecutionIfShardNameTooLong(finalRelationName,
															  longestShardName);

	if (switchedToSequentialAndLocalExecution)
	{
		return;
	}

	if (PartitionedTable(relationId))
	{
		Oid longestNamePartitionId = PartitionWithLongestNameRelationId(relationId);
		if (!OidIsValid(longestNamePartitionId))
		{
			/* no partitions have been created yet */
			return;
		}

		char *longestPartitionName = get_rel_name(longestNamePartitionId);
		char *longestPartitionShardName = NULL;

		/*
		 * Use the shardId values of the partition if it is distributed, otherwise use
		 * hypothetical values
		 */
		if (IsCitusTable(longestNamePartitionId) &&
			ShardIntervalCount(longestNamePartitionId) > 0)
		{
			longestPartitionShardName =
				GetLongestShardName(longestNamePartitionId, longestPartitionName);
		}
		else
		{
			longestPartitionShardName =
				GetLongestShardNameForLocalPartition(relationId, longestPartitionName);
		}

		SwitchToSequentialAndLocalExecutionIfShardNameTooLong(longestPartitionName,
															  longestPartitionShardName);
	}
}


/*
 * SwitchToSequentialAndLocalExecutionIfShardNameTooLong switches to sequential and local
 * execution if the shard name is too long.
 *
 * returns true if switched to sequential and local execution.
 */
static bool
SwitchToSequentialAndLocalExecutionIfShardNameTooLong(char *relationName,
													  char *longestShardName)
{
	if (strlen(longestShardName) >= NAMEDATALEN - 1)
	{
		if (ParallelQueryExecutedInTransaction())
		{
			/*
			 * If there has already been a parallel query executed, the sequential mode
			 * would still use the already opened parallel connections to the workers,
			 * thus contradicting our purpose of using sequential mode.
			 */
			ereport(ERROR, (errmsg(
								"Shard name (%s) for table (%s) is too long and could "
								"lead to deadlocks when executed in a transaction "
								"block after a parallel query", longestShardName,
								relationName),
							errhint("Try re-running the transaction with "
									"\"SET LOCAL citus.multi_shard_modify_mode TO "
									"\'sequential\';\"")));
		}
		else
		{
			elog(DEBUG1, "the name of the shard (%s) for relation (%s) is too long, "
						 "switching to sequential and local execution mode to prevent "
						 "self deadlocks",
				 longestShardName, relationName);

			SetLocalMultiShardModifyModeToSequential();
			SetLocalExecutionStatus(LOCAL_EXECUTION_REQUIRED);

			return true;
		}
	}

	return false;
}


/*
 * SwitchToSequentialAndLocalExecutionIfPartitionNameTooLong is a wrapper for new
 * partitions that will be distributed after attaching to a distributed partitioned table
 */
void
SwitchToSequentialAndLocalExecutionIfPartitionNameTooLong(Oid parentRelationId,
														  Oid partitionRelationId)
{
	SwitchToSequentialAndLocalExecutionIfRelationNameTooLong(
		parentRelationId, get_rel_name(partitionRelationId));
}
