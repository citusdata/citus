/*-------------------------------------------------------------------------
 * schema_based_sharding.c
 *
 *	  Routines for schema-based sharding.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "miscadmin.h"

#include "access/genam.h"
#include "catalog/catalog.h"
#include "catalog/pg_namespace_d.h"
#include "commands/extension.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

#include "distributed/argutils.h"
#include "distributed/backend_data.h"
#include "distributed/colocation_utils.h"
#include "distributed/commands.h"
#include "distributed/listutils.h"
#include "distributed/metadata/distobject.h"
#include "distributed/metadata_sync.h"
#include "distributed/multi_partitioning_utils.h"
#include "distributed/shard_transfer.h"
#include "distributed/tenant_schema_metadata.h"
#include "distributed/worker_shard_visibility.h"


/* return value of CreateCitusMoveSchemaParams() */
typedef struct
{
	uint64 anchorShardId;
	uint32 sourceNodeId;
	char *sourceNodeName;
	uint32 sourceNodePort;
} CitusMoveSchemaParams;


static void UnregisterTenantSchemaGlobally(Oid schemaId, char *schemaName);
static List * SchemaGetNonShardTableIdList(Oid schemaId);
static void EnsureSchemaCanBeDistributed(Oid schemaId, List *schemaTableIdList);
static void EnsureTenantSchemaNameAllowed(Oid schemaId);
static void EnsureTableKindSupportedForTenantSchema(Oid relationId);
static void EnsureFKeysForTenantTable(Oid relationId);
static void EnsureSchemaExist(Oid schemaId);
static CitusMoveSchemaParams * CreateCitusMoveSchemaParams(Oid schemaId);
static uint64 TenantSchemaPickAnchorShardId(Oid schemaId);


/* controlled via citus.enable_schema_based_sharding GUC */
bool EnableSchemaBasedSharding = false;


const char *TenantOperationNames[TOTAL_TENANT_OPERATION] = {
	"undistribute_table",
	"alter_distributed_table",
	"colocate_with",
	"update_distributed_table_colocation",
	"set schema",
};


PG_FUNCTION_INFO_V1(citus_internal_unregister_tenant_schema_globally);
PG_FUNCTION_INFO_V1(citus_schema_distribute);
PG_FUNCTION_INFO_V1(citus_schema_undistribute);
PG_FUNCTION_INFO_V1(citus_schema_move);
PG_FUNCTION_INFO_V1(citus_schema_move_with_nodeid);

/*
 * ShouldUseSchemaBasedSharding returns true if schema given name should be
 * used as a tenant schema.
 */
bool
ShouldUseSchemaBasedSharding(char *schemaName)
{
	if (!EnableSchemaBasedSharding)
	{
		return false;
	}

	if (IsBinaryUpgrade)
	{
		return false;
	}

	/*
	 * Citus utility hook skips processing CREATE SCHEMA commands while an
	 * extension is being created. For this reason, we don't expect to get
	 * here while an extension is being created.
	 */
	Assert(!creating_extension);

	/*
	 * CREATE SCHEMA commands issued by internal backends are not meant to
	 * create tenant schemas but to sync metadata.
	 *
	 * On workers, Citus utility hook skips processing CREATE SCHEMA commands
	 * because we temporarily disable DDL propagation on workers when sending
	 * CREATE SCHEMA commands. For this reason, right now this check is a bit
	 * redundant but we prefer to keep it here to be on the safe side.
	 */
	if (IsCitusInternalBackend() || IsRebalancerInternalBackend())
	{
		return false;
	}

	/*
	 * Not do an oid comparison based on PG_PUBLIC_NAMESPACE because
	 * we want to treat "public" schema in the same way even if it's
	 * recreated.
	 */
	if (strcmp(schemaName, "public") == 0)
	{
		return false;
	}

	return true;
}


/*
 * ShouldCreateTenantSchemaTable returns true if we should create a tenant
 * schema table for given relationId.
 */
bool
ShouldCreateTenantSchemaTable(Oid relationId)
{
	if (IsBinaryUpgrade)
	{
		return false;
	}

	/*
	 * CREATE TABLE commands issued by internal backends are not meant to
	 * create tenant tables but to sync metadata.
	 */
	if (IsCitusInternalBackend() || IsRebalancerInternalBackend())
	{
		return false;
	}

	Oid schemaId = get_rel_namespace(relationId);
	return IsTenantSchema(schemaId);
}


/*
 * EnsureTableKindSupportedForTenantSchema ensures that given table's kind is
 * supported by a tenant schema.
 */
static void
EnsureTableKindSupportedForTenantSchema(Oid relationId)
{
	if (IsForeignTable(relationId))
	{
		ereport(ERROR, (errmsg("cannot create a foreign table in a distributed "
							   "schema")));
	}

	if (PartitionTable(relationId))
	{
		ErrorIfIllegalPartitioningInTenantSchema(PartitionParentOid(relationId),
												 relationId);
	}

	if (PartitionedTable(relationId))
	{
		List *partitionList = PartitionList(relationId);

		Oid partitionRelationId = InvalidOid;
		foreach_declared_oid(partitionRelationId, partitionList)
		{
			ErrorIfIllegalPartitioningInTenantSchema(relationId, partitionRelationId);
		}
	}

	if (IsChildTable(relationId) || IsParentTable(relationId))
	{
		ereport(ERROR, (errmsg("tables in a distributed schema cannot inherit or "
							   "be inherited")));
	}
}


/*
 * EnsureFKeysForTenantTable ensures that all referencing and referenced foreign
 * keys are allowed for given table.
 */
static void
EnsureFKeysForTenantTable(Oid relationId)
{
	Oid tenantSchemaId = get_rel_namespace(relationId);
	int fKeyReferencingFlags = INCLUDE_REFERENCING_CONSTRAINTS | INCLUDE_ALL_TABLE_TYPES;
	List *referencingForeignKeys = GetForeignKeyOids(relationId, fKeyReferencingFlags);
	Oid foreignKeyId = InvalidOid;
	foreach_declared_oid(foreignKeyId, referencingForeignKeys)
	{
		Oid referencingTableId = GetReferencingTableId(foreignKeyId);
		Oid referencedTableId = GetReferencedTableId(foreignKeyId);
		Oid referencedTableSchemaId = get_rel_namespace(referencedTableId);

		/* We allow foreign keys to a table in the same schema */
		if (tenantSchemaId == referencedTableSchemaId)
		{
			continue;
		}

		/*
		 * Allow foreign keys to the other schema only if the referenced table is
		 * a reference table.
		 */
		if (!IsCitusTable(referencedTableId) ||
			!IsCitusTableType(referencedTableId, REFERENCE_TABLE))
		{
			ereport(ERROR, (errmsg("foreign keys from distributed schemas can only "
								   "point to the same distributed schema or reference "
								   "tables in regular schemas"),
							errdetail("\"%s\" references \"%s\" via foreign key "
									  "constraint \"%s\"",
									  generate_qualified_relation_name(
										  referencingTableId),
									  generate_qualified_relation_name(referencedTableId),
									  get_constraint_name(foreignKeyId))));
		}
	}

	int fKeyReferencedFlags = INCLUDE_REFERENCED_CONSTRAINTS | INCLUDE_ALL_TABLE_TYPES;
	List *referencedForeignKeys = GetForeignKeyOids(relationId, fKeyReferencedFlags);
	foreach_declared_oid(foreignKeyId, referencedForeignKeys)
	{
		Oid referencingTableId = GetReferencingTableId(foreignKeyId);
		Oid referencedTableId = GetReferencedTableId(foreignKeyId);
		Oid referencingTableSchemaId = get_rel_namespace(referencingTableId);

		/* We allow foreign keys from a table in the same schema */
		if (tenantSchemaId == referencingTableSchemaId)
		{
			continue;
		}

		/* Not allow any foreign keys from the other schema */
		ereport(ERROR, (errmsg("cannot create foreign keys to tables in a distributed "
							   "schema from another schema"),
						errdetail("\"%s\" references \"%s\" via foreign key "
								  "constraint \"%s\"",
								  generate_qualified_relation_name(referencingTableId),
								  generate_qualified_relation_name(referencedTableId),
								  get_constraint_name(foreignKeyId))));
	}
}


/*
 * CreateTenantSchemaTable creates a tenant table with given relationId.
 *
 * This means creating a single shard distributed table without a shard
 * key and colocating it with the other tables in its schema.
 */
void
CreateTenantSchemaTable(Oid relationId)
{
	if (!IsCoordinator())
	{
		/*
		 * We don't support creating tenant tables from workers. We could
		 * let ShouldCreateTenantSchemaTable() to return false to allow users
		 * to create a local table as usual but that would be confusing because
		 * it might sound like we allow creating tenant tables from workers.
		 * For this reason, we prefer to throw an error instead.
		 *
		 * Indeed, CreateSingleShardTable() would already do so but we
		 * prefer to throw an error with a more meaningful message, rather
		 * than saying "operation is not allowed on this node".
		 */
		ereport(ERROR, (errmsg("cannot create tables in a distributed schema from "
							   "a worker node"),
						errhint("Connect to the coordinator node and try again.")));
	}

	EnsureTableKindSupportedForTenantSchema(relationId);

	/*
	 * We don't expect this to happen because ShouldCreateTenantSchemaTable()
	 * should've already verified that; but better to check.
	 */
	Oid schemaId = get_rel_namespace(relationId);
	uint32 colocationId = SchemaIdGetTenantColocationId(schemaId);
	if (colocationId == INVALID_COLOCATION_ID)
	{
		ereport(ERROR, (errmsg("schema \"%s\" is not distributed",
							   get_namespace_name(schemaId))));
	}

	ColocationParam colocationParam = {
		.colocationParamType = COLOCATE_WITH_COLOCATION_ID,
		.colocationId = colocationId,
	};
	CreateSingleShardTable(relationId, colocationParam);
}


/*
 * ErrorIfIllegalPartitioningInTenantSchema throws an error if the
 * partitioning relationship between the parent and the child is illegal
 * because they are in different schemas while one of them is a tenant table.
 *
 * This function assumes that either the parent or the child are in a tenant
 * schema.
 */
void
ErrorIfIllegalPartitioningInTenantSchema(Oid parentRelationId, Oid partitionRelationId)
{
	if (get_rel_namespace(partitionRelationId) != get_rel_namespace(parentRelationId))
	{
		ereport(ERROR, (errmsg("partitioning within a distributed schema is not "
							   "supported when the parent and the child "
							   "are in different schemas")));
	}
}


/*
 * CreateTenantSchemaColocationId returns new colocation id for a tenant schema.
 */
uint32
CreateTenantSchemaColocationId(void)
{
	int shardCount = 1;
	int replicationFactor = 1;
	Oid distributionColumnType = InvalidOid;
	Oid distributionColumnCollation = InvalidOid;
	uint32 schemaColocationId = CreateColocationGroup(
		shardCount, replicationFactor, distributionColumnType,
		distributionColumnCollation);
	return schemaColocationId;
}


/*
 * SchemaGetNonShardTableIdList returns all nonshard relation ids
 * inside given schema.
 */
static List *
SchemaGetNonShardTableIdList(Oid schemaId)
{
	List *relationIdList = NIL;

	/* scan all relations in pg_class and return all tables inside given schema */
	Relation relationRelation = relation_open(RelationRelationId, AccessShareLock);

	ScanKeyData scanKey[1] = { 0 };
	ScanKeyInit(&scanKey[0], Anum_pg_class_relnamespace, BTEqualStrategyNumber,
				F_OIDEQ, ObjectIdGetDatum(schemaId));
	SysScanDesc scanDescriptor = systable_beginscan(relationRelation, ClassNameNspIndexId,
													true, NULL, 1, scanKey);

	HeapTuple heapTuple = NULL;
	while (HeapTupleIsValid(heapTuple = systable_getnext(scanDescriptor)))
	{
		Form_pg_class relationForm = (Form_pg_class) GETSTRUCT(heapTuple);
		char *relationName = NameStr(relationForm->relname);
		Oid relationId = get_relname_relid(relationName, schemaId);

		if (!OidIsValid(relationId))
		{
			ereport(ERROR, errmsg("table %s is dropped by a concurrent operation",
								  relationName));
		}

		/* skip shards */
		if (RelationIsAKnownShard(relationId))
		{
			continue;
		}

		if (RegularTable(relationId) || PartitionTable(relationId) ||
			IsForeignTable(relationId))
		{
			relationIdList = lappend_oid(relationIdList, relationId);
		}
	}

	systable_endscan(scanDescriptor);
	relation_close(relationRelation, AccessShareLock);

	return relationIdList;
}


/*
 * EnsureSchemaCanBeDistributed ensures the schema can be distributed.
 * Caller should take required the lock on relations and the schema.
 *
 * It checks:
 *  - Schema name is in the allowed-list,
 *  - Schema does not depend on an extension (created by extension),
 *  - No extension depends on the schema (CREATE EXTENSION <ext> SCHEMA <schema>),
 *	- Some checks for the table for being a valid tenant table.
 */
static void
EnsureSchemaCanBeDistributed(Oid schemaId, List *schemaTableIdList)
{
	/* Ensure schema name is allowed */
	EnsureTenantSchemaNameAllowed(schemaId);

	/* Any schema owned by extension is not allowed */
	char *schemaName = get_namespace_name(schemaId);
	ObjectAddress *schemaAddress = palloc0(sizeof(ObjectAddress));
	ObjectAddressSet(*schemaAddress, NamespaceRelationId, schemaId);
	if (IsAnyObjectAddressOwnedByExtension(list_make1(schemaAddress), NULL))
	{
		ereport(ERROR, (errmsg("schema %s, which is owned by an extension, cannot "
							   "be distributed", schemaName)));
	}

	/* Extension schemas are not allowed */
	ObjectAddress *extensionAddress = FirstExtensionWithSchema(schemaId);
	if (extensionAddress)
	{
		char *extensionName = get_extension_name(extensionAddress->objectId);
		ereport(ERROR, (errmsg("schema %s cannot be distributed since it is the schema "
							   "of extension %s", schemaName, extensionName)));
	}

	Oid relationId = InvalidOid;
	foreach_declared_oid(relationId, schemaTableIdList)
	{
		EnsureTenantTable(relationId, "citus_schema_distribute");
	}
}


/*
 * EnsureTenantTable ensures the table can be a valid tenant table.
 *  - Current user should be the owner of table,
 *  - Table kind is supported,
 *  - Referencing and referenced foreign keys for the table are supported,
 *	- Table is not owned by an extension,
 *  - Table should be Citus local or Postgres local table.
 */
void
EnsureTenantTable(Oid relationId, char *operationName)
{
	/* Ensure table owner */
	EnsureTableOwner(relationId);

	/* Check relation kind */
	EnsureTableKindSupportedForTenantSchema(relationId);

	/* Check foreign keys */
	EnsureFKeysForTenantTable(relationId);

	/* Check table not owned by an extension */
	ObjectAddress *tableAddress = palloc0(sizeof(ObjectAddress));
	ObjectAddressSet(*tableAddress, RelationRelationId, relationId);
	if (IsAnyObjectAddressOwnedByExtension(list_make1(tableAddress), NULL))
	{
		Oid schemaId = get_rel_namespace(relationId);
		char *tableName = get_namespace_name(schemaId);
		ereport(ERROR, (errmsg("schema cannot be distributed since it has "
							   "table %s which is owned by an extension",
							   tableName)));
	}

	/* Postgres local tables are allowed */
	if (!IsCitusTable(relationId))
	{
		return;
	}

	/* Only Citus local tables, amongst Citus table types, are allowed */
	if (!IsCitusTableType(relationId, CITUS_LOCAL_TABLE))
	{
		ereport(ERROR, (errmsg("distributed schema cannot have distributed tables"),
						errhint("Undistribute distributed tables before "
								"'%s'.", operationName)));
	}
}


/*
 * EnsureTenantSchemaNameAllowed ensures if given schema is applicable for registering
 * as a tenant schema.
 */
static void
EnsureTenantSchemaNameAllowed(Oid schemaId)
{
	char *schemaName = get_namespace_name(schemaId);

	/* public schema is not allowed */
	if (strcmp(schemaName, "public") == 0)
	{
		ereport(ERROR, (errmsg("public schema cannot be distributed")));
	}

	/* information_schema schema is not allowed */
	if (strcmp(schemaName, "information_schema") == 0)
	{
		ereport(ERROR, (errmsg("information_schema schema cannot be distributed")));
	}

	/* pg_temp_xx and pg_toast_temp_xx schemas are not allowed */
	if (isAnyTempNamespace(schemaId))
	{
		ereport(ERROR, (errmsg("temporary schema cannot be distributed")));
	}

	/* pg_catalog schema is not allowed */
	if (IsCatalogNamespace(schemaId))
	{
		ereport(ERROR, (errmsg("pg_catalog schema cannot be distributed")));
	}

	/* pg_toast schema is not allowed */
	if (IsToastNamespace(schemaId))
	{
		ereport(ERROR, (errmsg("pg_toast schema cannot be distributed")));
	}
}


/*
 * EnsureSchemaExist ensures that schema exists. Caller is responsible to take
 * the required lock on the schema.
 */
static void
EnsureSchemaExist(Oid schemaId)
{
	if (!SearchSysCacheExists1(NAMESPACEOID, ObjectIdGetDatum(schemaId)))
	{
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_SCHEMA),
						errmsg("schema with OID %u does not exist", schemaId)));
	}
}


/*
 * UnregisterTenantSchemaGlobally removes given schema from the tenant schema
 * metadata table, deletes the colocation group of the schema and sends the
 * command to do the same on the workers.
 */
static void
UnregisterTenantSchemaGlobally(Oid schemaId, char *schemaName)
{
	uint32 tenantSchemaColocationId = SchemaIdGetTenantColocationId(schemaId);

	DeleteTenantSchemaLocally(schemaId);
	if (EnableMetadataSync)
	{
		SendCommandToWorkersWithMetadata(TenantSchemaDeleteCommand(schemaName));
	}

	DeleteColocationGroup(tenantSchemaColocationId);
}


/*
 * citus_internal_unregister_tenant_schema_globally, called by Citus drop hook,
 * unregisters the schema when a tenant schema is dropped.
 *
 * NOTE: We need to pass schema_name as an argument. We cannot use schema id
 * to obtain schema name since the schema would have already been dropped when this
 * udf is called by the drop hook.
 */
Datum
citus_internal_unregister_tenant_schema_globally(PG_FUNCTION_ARGS)
{
	PG_ENSURE_ARGNOTNULL(0, "schema_id");
	Oid schemaId = PG_GETARG_OID(0);

	PG_ENSURE_ARGNOTNULL(1, "schema_name");
	text *schemaName = PG_GETARG_TEXT_PP(1);
	char *schemaNameStr = text_to_cstring(schemaName);

	/*
	 * Skip on workers because we expect this to be called from the coordinator
	 * only via drop hook.
	 */
	if (!IsCoordinator())
	{
		PG_RETURN_VOID();
	}

	/* make sure that the schema is dropped already */
	HeapTuple namespaceTuple = SearchSysCache1(NAMESPACEOID, ObjectIdGetDatum(schemaId));
	if (HeapTupleIsValid(namespaceTuple))
	{
		ReleaseSysCache(namespaceTuple);
		ereport(ERROR, (errmsg("schema is expected to be already dropped "
							   "because this function is only expected to "
							   "be called from Citus drop hook")));
	}
	UnregisterTenantSchemaGlobally(schemaId, schemaNameStr);
	PG_RETURN_VOID();
}


/*
 * citus_schema_distribute gets a regular schema name, then converts it to a tenant
 * schema.
 */
Datum
citus_schema_distribute(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);
	EnsureCoordinator();

	Oid schemaId = PG_GETARG_OID(0);
	EnsureSchemaExist(schemaId);
	EnsureSchemaOwner(schemaId);

	/* Prevent concurrent table creation under the schema */
	LockDatabaseObject(NamespaceRelationId, schemaId, 0, AccessExclusiveLock);

	/*
	 * We should ensure the existence of the schema after taking the lock since
	 * the schema could have been dropped before we acquired the lock.
	 */
	EnsureSchemaExist(schemaId);
	EnsureSchemaOwner(schemaId);

	/* Return if the schema is already a tenant schema */
	char *schemaName = get_namespace_name(schemaId);
	if (IsTenantSchema(schemaId))
	{
		ereport(NOTICE, (errmsg("schema %s is already distributed", schemaName)));
		PG_RETURN_VOID();
	}

	/* Take lock on the relations and filter out partition tables */
	List *tableIdListInSchema = SchemaGetNonShardTableIdList(schemaId);
	List *tableIdListToConvert = NIL;
	Oid relationId = InvalidOid;
	foreach_declared_oid(relationId, tableIdListInSchema)
	{
		/* prevent concurrent drop of the relation */
		LockRelationOid(relationId, AccessShareLock);
		EnsureRelationExists(relationId);

		/*
		 * Skip partitions as they would be distributed by the parent table.
		 *
		 * We should filter out partitions here before distributing the schema.
		 * Otherwise, converted partitioned table would change oid of partitions and its
		 * partition tables would fail with oid not exist.
		 */
		if (PartitionTable(relationId))
		{
			continue;
		}

		tableIdListToConvert = lappend_oid(tableIdListToConvert, relationId);
	}

	/* Makes sure the schema can be distributed. */
	EnsureSchemaCanBeDistributed(schemaId, tableIdListInSchema);

	ereport(NOTICE, (errmsg("distributing the schema %s", schemaName)));

	/* Create colocation id and then single shard tables with the colocation id */
	uint32 colocationId = CreateTenantSchemaColocationId();
	ColocationParam colocationParam = {
		.colocationParamType = COLOCATE_WITH_COLOCATION_ID,
		.colocationId = colocationId,
	};

	/*
	 * Collect foreign keys for recreation and then drop fkeys and create single shard
	 * tables.
	 */
	List *originalForeignKeyRecreationCommands = NIL;
	foreach_declared_oid(relationId, tableIdListToConvert)
	{
		List *fkeyCommandsForRelation =
			GetFKeyCreationCommandsRelationInvolvedWithTableType(relationId,
																 INCLUDE_ALL_TABLE_TYPES);
		originalForeignKeyRecreationCommands = list_concat(
			originalForeignKeyRecreationCommands, fkeyCommandsForRelation);

		DropFKeysRelationInvolvedWithTableType(relationId, INCLUDE_ALL_TABLE_TYPES);
		CreateSingleShardTable(relationId, colocationParam);
	}

	/* We can skip foreign key validations as we are sure about them at start */
	bool skip_validation = true;
	ExecuteForeignKeyCreateCommandList(originalForeignKeyRecreationCommands,
									   skip_validation);

	/* Register the schema locally and sync it to workers */
	InsertTenantSchemaLocally(schemaId, colocationId);
	char *registerSchemaCommand = TenantSchemaInsertCommand(schemaId, colocationId);
	if (EnableMetadataSync)
	{
		SendCommandToWorkersWithMetadata(registerSchemaCommand);
	}

	PG_RETURN_VOID();
}


/*
 * citus_schema_undistribute gets a tenant schema name, then converts it to a regular
 * schema by undistributing all tables under it.
 */
Datum
citus_schema_undistribute(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);
	EnsureCoordinator();

	Oid schemaId = PG_GETARG_OID(0);
	EnsureSchemaExist(schemaId);
	EnsureSchemaOwner(schemaId);

	/* Prevent concurrent table creation under the schema */
	LockDatabaseObject(NamespaceRelationId, schemaId, 0, AccessExclusiveLock);

	/*
	 * We should ensure the existence of the schema after taking the lock since
	 * the schema could have been dropped before we acquired the lock.
	 */
	EnsureSchemaExist(schemaId);
	EnsureSchemaOwner(schemaId);

	/* The schema should be a tenant schema */
	char *schemaName = get_namespace_name(schemaId);
	if (!IsTenantSchema(schemaId))
	{
		ereport(ERROR, (errmsg("schema %s is not distributed", schemaName)));
	}

	ereport(NOTICE, (errmsg("undistributing schema %s", schemaName)));

	/* Take lock on the relations and filter out partition tables */
	List *tableIdListInSchema = SchemaGetNonShardTableIdList(schemaId);
	List *tableIdListToConvert = NIL;
	Oid relationId = InvalidOid;
	foreach_declared_oid(relationId, tableIdListInSchema)
	{
		/* prevent concurrent drop of the relation */
		LockRelationOid(relationId, AccessShareLock);
		EnsureRelationExists(relationId);

		/*
		 * Skip partitions as they would be undistributed by the parent table.
		 *
		 * We should filter out partitions here before undistributing the schema.
		 * Otherwise, converted partitioned table would change oid of partitions and its
		 * partition tables would fail with oid not exist.
		 */
		if (PartitionTable(relationId))
		{
			continue;
		}

		tableIdListToConvert = lappend_oid(tableIdListToConvert, relationId);

		/* Only single shard tables are expected during the undistribution of the schema */
		Assert(IsCitusTableType(relationId, SINGLE_SHARD_DISTRIBUTED));
	}

	/*
	 * First, we need to delete schema metadata and sync it to workers. Otherwise,
	 * we would get error from `ErrorIfTenantTable` while undistributing the tables.
	 */
	UnregisterTenantSchemaGlobally(schemaId, schemaName);
	UndistributeTables(tableIdListToConvert);

	PG_RETURN_VOID();
}


/*
 * citus_schema_move moves the shards that belong to given distributed tenant
 * schema from one node to the other node by using citus_move_shard_placement().
 */
Datum
citus_schema_move(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);
	EnsureCoordinator();

	Oid schemaId = PG_GETARG_OID(0);
	CitusMoveSchemaParams *params = CreateCitusMoveSchemaParams(schemaId);

	DirectFunctionCall6(citus_move_shard_placement,
						UInt64GetDatum(params->anchorShardId),
						CStringGetTextDatum(params->sourceNodeName),
						UInt32GetDatum(params->sourceNodePort),
						PG_GETARG_DATUM(1),
						PG_GETARG_DATUM(2),
						PG_GETARG_DATUM(3));
	PG_RETURN_VOID();
}


/*
 * citus_schema_move_with_nodeid does the same as citus_schema_move(), but
 * accepts node id as parameter instead of hostname and port, hence uses
 * citus_move_shard_placement_with_nodeid().
 */
Datum
citus_schema_move_with_nodeid(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);
	EnsureCoordinator();

	Oid schemaId = PG_GETARG_OID(0);
	CitusMoveSchemaParams *params = CreateCitusMoveSchemaParams(schemaId);

	DirectFunctionCall4(citus_move_shard_placement_with_nodeid,
						UInt64GetDatum(params->anchorShardId),
						UInt32GetDatum(params->sourceNodeId),
						PG_GETARG_DATUM(1),
						PG_GETARG_DATUM(2));
	PG_RETURN_VOID();
}


/*
 * CreateCitusMoveSchemaParams is a helper function for
 * citus_schema_move() and citus_schema_move_with_nodeid()
 * that validates input schema and returns the parameters to be used in underlying
 * shard transfer functions.
 */
static CitusMoveSchemaParams *
CreateCitusMoveSchemaParams(Oid schemaId)
{
	EnsureSchemaExist(schemaId);
	EnsureSchemaOwner(schemaId);

	if (!IsTenantSchema(schemaId))
	{
		ereport(ERROR, (errmsg("schema %s is not a distributed schema",
							   get_namespace_name(schemaId))));
	}

	uint64 anchorShardId = TenantSchemaPickAnchorShardId(schemaId);
	if (anchorShardId == INVALID_SHARD_ID)
	{
		ereport(ERROR, (errmsg("cannot move distributed schema %s because it is empty",
							   get_namespace_name(schemaId))));
	}

	uint32 colocationId = SchemaIdGetTenantColocationId(schemaId);
	uint32 sourceNodeId = SingleShardTableColocationNodeId(colocationId);

	bool missingOk = false;
	WorkerNode *sourceNode = FindNodeWithNodeId(sourceNodeId, missingOk);

	CitusMoveSchemaParams *params = palloc0(sizeof(CitusMoveSchemaParams));
	params->anchorShardId = anchorShardId;
	params->sourceNodeId = sourceNodeId;
	params->sourceNodeName = sourceNode->workerName;
	params->sourceNodePort = sourceNode->workerPort;
	return params;
}


/*
 * TenantSchemaPickAnchorShardId returns the id of one of the shards
 * created in given tenant schema.
 *
 * Returns INVALID_SHARD_ID if the schema was initially empty or if it's not
 * a tenant schema.
 *
 * Throws an error if all the tables in the schema are concurrently dropped.
 */
static uint64
TenantSchemaPickAnchorShardId(Oid schemaId)
{
	uint32 colocationId = SchemaIdGetTenantColocationId(schemaId);
	List *tablesInSchema = ColocationGroupTableList(colocationId, 0);
	if (list_length(tablesInSchema) == 0)
	{
		return INVALID_SHARD_ID;
	}

	Oid relationId = InvalidOid;
	foreach_declared_oid(relationId, tablesInSchema)
	{
		/*
		 * Make sure the relation isn't dropped for the remainder of
		 * the transaction.
		 */
		LockRelationOid(relationId, AccessShareLock);

		/*
		 * The relation might have been dropped just before we locked it.
		 * Let's look it up.
		 */
		Relation relation = RelationIdGetRelation(relationId);
		if (RelationIsValid(relation))
		{
			/* relation still exists, we can use it */
			RelationClose(relation);
			return GetFirstShardId(relationId);
		}
	}

	ereport(ERROR, (errmsg("tables in schema %s are concurrently dropped",
						   get_namespace_name(schemaId))));
}


/*
 * ErrorIfTenantTable errors out with the given operation name,
 * if the given relation is a tenant table.
 */
void
ErrorIfTenantTable(Oid relationId, const char *operationName)
{
	if (IsTenantSchema(get_rel_namespace(relationId)))
	{
		ereport(ERROR, (errmsg("%s is not allowed for %s because it belongs to "
							   "a distributed schema",
							   generate_qualified_relation_name(relationId),
							   operationName)));
	}
}
