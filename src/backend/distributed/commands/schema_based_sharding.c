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
#include "distributed/argutils.h"
#include "distributed/backend_data.h"
#include "distributed/colocation_utils.h"
#include "distributed/commands.h"
#include "distributed/listutils.h"
#include "distributed/metadata_sync.h"
#include "distributed/metadata/distobject.h"
#include "distributed/multi_partitioning_utils.h"
#include "distributed/tenant_schema_metadata.h"
#include "distributed/worker_shard_visibility.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"


static List * GetRegularAndForeignCitusTablesInSchema(Oid schemaId);
static void EnsureSchemaCanBeDistributed(Oid schemaId, List *schemaTableIdList);
static void EnsureTenantSchemaNameAllowed(Oid schemaId);
static void EnsureTableKindSupportedForTenantSchema(Oid relationId);
static void EnsureSchemaExist(Oid schemaId);

/* controlled via citus.enable_schema_based_sharding GUC */
bool EnableSchemaBasedSharding = false;


PG_FUNCTION_INFO_V1(citus_internal_unregister_tenant_schema_globally);
PG_FUNCTION_INFO_V1(citus_schema_distribute);
PG_FUNCTION_INFO_V1(citus_schema_undistribute);

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
	else if (PartitionTable(relationId))
	{
		ErrorIfIllegalPartitioningInTenantSchema(PartitionParentOid(relationId),
												 relationId);
	}
	else if (IsChildTable(relationId) || IsParentTable(relationId))
	{
		ereport(ERROR, (errmsg("tables in a distributed schema cannot inherit or "
							   "be inherited")));
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
 */
void
ErrorIfIllegalPartitioningInTenantSchema(Oid parentRelationId, Oid partitionRelationId)
{
	Oid partitionSchemaId = get_rel_namespace(partitionRelationId);
	Oid parentSchemaId = get_rel_namespace(parentRelationId);

	bool partitionIsTenantTable = IsTenantSchema(partitionSchemaId);
	bool parentIsTenantTable = IsTenantSchema(parentSchemaId);

	bool illegalPartitioning = false;
	if (partitionIsTenantTable != parentIsTenantTable)
	{
		illegalPartitioning = true;
	}
	else if (partitionIsTenantTable && parentIsTenantTable)
	{
		illegalPartitioning = (parentSchemaId != partitionSchemaId);
	}

	if (illegalPartitioning)
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
 * GetRegularAndForeignCitusTablesInSchema returns all nonshard relation ids,
 * with relkind = relation or partitioned or foreign, inside given schema.
 */
static List *
GetRegularAndForeignCitusTablesInSchema(Oid schemaId)
{
	List *relationIdList = NIL;

	/* scan all relations in pg_class and return all tables inside given schema */
	Relation relationRelation = relation_open(RelationRelationId, RowExclusiveLock);

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

		if (RegularTable(relationId) || IsForeignTable(relationId))
		{
			relationIdList = lappend_oid(relationIdList, relationId);
		}
	}

	systable_endscan(scanDescriptor);
	relation_close(relationRelation, RowExclusiveLock);

	return relationIdList;
}


/*
 * EnsureSchemaCanBeDistributed ensures the schema can be distributed.
 * Caller should take required the lock on relations and the schema.
 *
 * It checks:
 *  - Schema name is in the allowed-list,
 *  - Only Citus local and Postgres tables exist under the schema,
 *  - Table kinds are supported.
 */
static void
EnsureSchemaCanBeDistributed(Oid schemaId, List *schemaTableIdList)
{
	/* Ensure schema name is allowed */
	EnsureTenantSchemaNameAllowed(schemaId);

	Oid relationId = InvalidOid;
	foreach_oid(relationId, schemaTableIdList)
	{
		/* Check relation kind */
		EnsureTableKindSupportedForTenantSchema(relationId);

		/* Postgres local tables are allowed */
		if (!IsCitusTable(relationId))
		{
			continue;
		}

		/* Only Citus local tables, amongst Citus table types, are allowed */
		CitusTableCacheEntry *tableEntry = GetCitusTableCacheEntry(relationId);
		CitusTableType citusTableType = GetCitusTableTypeCacheEntry(tableEntry);
		if (citusTableType != CITUS_LOCAL_TABLE)
		{
			ereport(ERROR, (errmsg("schema already has distributed tables"),
							errhint("Undistribute distributed tables under "
									"the schema before distributing the schema.")));
		}
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

	/* any schema owned by extension is not allowed */
	ObjectAddress *schemaAddress = palloc0(sizeof(ObjectAddress));
	ObjectAddressSet(*schemaAddress, NamespaceRelationId, schemaId);
	if (IsAnyObjectAddressOwnedByExtension(list_make1(schemaAddress), NULL))
	{
		ereport(ERROR, (errmsg("schema %s, which is owned by an extension, cannot "
							   "be distributed", schemaName)));
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
						errmsg("concurrent operation removed the schema")));
	}
}


/*
 * citus_internal_unregister_tenant_schema_globally removes given schema from
 * the tenant schema metadata table, deletes the colocation group of the schema
 * and sends the command to do the same on the workers.
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
	uint32 tenantSchemaColocationId = SchemaIdGetTenantColocationId(schemaId);

	DeleteTenantSchemaLocally(schemaId);
	SendCommandToWorkersWithMetadata(TenantSchemaDeleteCommand(schemaNameStr));

	DeleteColocationGroup(tenantSchemaColocationId);
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

	/* Collect all tables under the schema */
	List *tableIdListInSchema = GetRegularAndForeignCitusTablesInSchema(schemaId);

	/* Take lock on the relations and filter out partition tables */
	List *tableIdListToConvert = NIL;
	Oid relationId = InvalidOid;
	foreach_oid(relationId, tableIdListInSchema)
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

	/* Create colocation id for single shard tables */
	uint32 colocationId = CreateTenantSchemaColocationId();
	ColocationParam colocationParam = {
		.colocationParamType = COLOCATE_WITH_COLOCATION_ID,
		.colocationId = colocationId,
	};

	foreach_oid(relationId, tableIdListToConvert)
	{
		/*
		 * We cannot simply call CreateTenantSchemaTable since we need to register
		 * the schema before calling it. And during the table creation, if we have
		 * any Citus local table under the schema, it would first undistributed and
		 * then converted to a single shard. It would throw an error from
		 * `ErrorIfTenantTable` while undistributing the Citus local table. That is
		 * why we first create single shards before registering the schema.
		 */
		CreateSingleShardTable(relationId, colocationParam);
	}

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

	/* Collect all tables under the schema */
	List *tableIdListInSchema = GetRegularAndForeignCitusTablesInSchema(schemaId);

	/* Take lock on the relations and filter out partition tables */
	List *tableIdListToConvert = NIL;
	Oid relationId = InvalidOid;
	foreach_oid(relationId, tableIdListInSchema)
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
	char *unregisterSchemaCommand = TenantSchemaDeleteCommand(schemaName);
	DeleteTenantSchemaLocally(schemaId);
	if (EnableMetadataSync)
	{
		SendCommandToWorkersWithMetadata(unregisterSchemaCommand);
	}

	/*
	 * No need to drop colocation entries explicitly here, since colocation entry would be
	 * automatically dropped when we undistribute the last table in the schema.
	 */
	UndistributeTables(tableIdListToConvert);

	PG_RETURN_VOID();
}


/*
 * ErrorIfTenantTable errors out with the given operation name,
 * if the given relation is a tenant table.
 */
void
ErrorIfTenantTable(Oid relationId, char *operationName)
{
	if (IsTenantSchema(get_rel_namespace(relationId)))
	{
		ereport(ERROR, (errmsg("%s is not allowed for %s because it belongs to "
							   "a distributed schema",
							   generate_qualified_relation_name(relationId),
							   operationName)));
	}
}


/*
 * ErrorIfTenantSchema errors out with the given operation name,
 * if the given schema is a tenant schema.
 */
void
ErrorIfTenantSchema(Oid nspOid, char *operationName)
{
	if (IsTenantSchema(nspOid))
	{
		ereport(ERROR, (errmsg(
							"%s is not allowed for %s because it is a distributed schema",
							get_namespace_name(nspOid),
							operationName)));
	}
}
