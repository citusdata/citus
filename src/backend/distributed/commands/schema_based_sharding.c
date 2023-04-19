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
#include "distributed/metadata_utility.h"
#include "distributed/multi_partitioning_utils.h"
#include "distributed/tenant_schema_metadata.h"
#include "distributed/worker_shard_visibility.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"


/*
 * SchemaTables is used to classify different table types under a schema
 */
typedef struct SchemaTables
{
	Oid schemaId;
	List *hashDistributedRelationIds;
	List *rangeDistributedRelationIds;
	List *appendDistributedRelationIds;
	List *referenceRelationIds;
	List *singleShardRelationIds;
	List *citusLocalRelationIds;
	List *localRelationIds;
} SchemaTables;

static bool SchemaHasCitusTableType(SchemaTables *schemaTables, List *citusTableTypes);
static SchemaTables * TablesUnderSchema(Oid schemaId);
static List * GetRelationIdsInSchema(Oid schemaId);
static void EnsureTenantSchemaAllowed(Oid schemaId);
static List * DisAllowedTableTypesDuringSchemaSet(void);
static List * DisAllowedTableTypesDuringSchemaUnset(void);
static void EnsureTableKindsSupportedForTenantSchema(List *relationIds);

/* controlled via citus.enable_schema_based_sharding GUC */
bool EnableSchemaBasedSharding = false;


PG_FUNCTION_INFO_V1(citus_internal_unregister_tenant_schema_globally);
PG_FUNCTION_INFO_V1(citus_schema_tenant_set);
PG_FUNCTION_INFO_V1(citus_schema_tenant_unset);

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
 * EnsureTableKindsSupportedForTenantSchema ensures that given tables' kinds are
 * supported by a tenant schema.
 */
static void
EnsureTableKindsSupportedForTenantSchema(List *relationIds)
{
	Oid relationId = InvalidOid;
	foreach_oid(relationId, relationIds)
	{
		if (IsForeignTable(relationId))
		{
			/* throw an error that is nicer than the one CreateSingleShardTable() would throw */
			ereport(ERROR, (errmsg("cannot create a tenant table from a foreign table")));
		}
		else if (PartitionTable(relationId))
		{
			ErrorIfIllegalPartitioningInTenantSchema(PartitionParentOid(relationId),
													 relationId);
		}
		else if (IsChildTable(relationId) || IsParentTable(relationId))
		{
			ereport(ERROR, (errmsg("tenant tables cannot inherit or be inherited")));
		}
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
		ereport(ERROR, (errmsg("cannot create a tenant table from a worker node"),
						errhint("Connect to the coordinator node and try again.")));
	}

	EnsureTableKindsSupportedForTenantSchema(list_make1_oid(relationId));

	/*
	 * We don't expect this to happen because ShouldCreateTenantSchemaTable()
	 * should've already verified that; but better to check.
	 */
	Oid schemaId = get_rel_namespace(relationId);
	uint32 colocationId = SchemaIdGetTenantColocationId(schemaId);
	if (colocationId == INVALID_COLOCATION_ID)
	{
		ereport(ERROR, (errmsg("schema \"%s\" is not a tenant schema",
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
		ereport(ERROR, (errmsg("partitioning with tenant tables is not "
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
 * GetRelationIdsInSchema returns all nonshard relation ids, with relkind = relation or
 * partitioned or foreign, inside given schema.
 */
static List *
GetRelationIdsInSchema(Oid schemaId)
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
			continue;
		}

		/* skip shards */
		if (RelationIsAKnownShard(relationId))
		{
			continue;
		}

		/*
		 * Collect only the relkinds of 'relation' and 'partitioned' since others are
		 * created as dependency. e.g. index, sequence, type, view, matview
		 *
		 * We also collect foreign tables here since we will apply error checks for them
		 * later.
		 */
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
 * TablesUnderSchema filters all Citus tables, given in the schema,
 * inside the relations. Returns SchemaTables which contains separate list
 * fir different table types.
 */
static SchemaTables *
TablesUnderSchema(Oid schemaId)
{
	List *relationIdsInSchema = GetRelationIdsInSchema(schemaId);

	SchemaTables *schemaTables = palloc0(sizeof(SchemaTables));
	schemaTables->schemaId = schemaId;

	Oid relationId = InvalidOid;
	foreach_oid(relationId, relationIdsInSchema)
	{
		CitusTableCacheEntry *tableEntry = LookupCitusTableCacheEntry(relationId);
		if (tableEntry == NULL)
		{
			schemaTables->localRelationIds = lappend_oid(schemaTables->localRelationIds,
														 relationId);
		}
		else if (IsCitusTableType(relationId, HASH_DISTRIBUTED))
		{
			schemaTables->hashDistributedRelationIds = lappend_oid(
				schemaTables->hashDistributedRelationIds, relationId);
		}
		else if (IsCitusTableType(relationId, RANGE_DISTRIBUTED))
		{
			schemaTables->rangeDistributedRelationIds = lappend_oid(
				schemaTables->rangeDistributedRelationIds, relationId);
		}
		else if (IsCitusTableType(relationId, APPEND_DISTRIBUTED))
		{
			schemaTables->appendDistributedRelationIds = lappend_oid(
				schemaTables->appendDistributedRelationIds, relationId);
		}
		else if (IsCitusTableType(relationId, SINGLE_SHARD_DISTRIBUTED))
		{
			schemaTables->singleShardRelationIds = lappend_oid(
				schemaTables->singleShardRelationIds, relationId);
		}
		else if (IsCitusTableType(relationId, REFERENCE_TABLE))
		{
			schemaTables->referenceRelationIds = lappend_oid(
				schemaTables->referenceRelationIds, relationId);
		}
		else if (IsCitusTableType(relationId, CITUS_LOCAL_TABLE))
		{
			schemaTables->citusLocalRelationIds = lappend_oid(
				schemaTables->citusLocalRelationIds, relationId);
		}
	}
	return schemaTables;
}


static bool
SchemaHasCitusTableType(SchemaTables *schemaTables, List *citusTableTypes)
{
	CitusTableType citusTableType = 0;
	foreach_int(citusTableType, citusTableTypes)
	{
		switch (citusTableType)
		{
			case HASH_DISTRIBUTED:
			{
				if (schemaTables->hashDistributedRelationIds)
				{
					return true;
				}
				break;
			}

			case RANGE_DISTRIBUTED:
			{
				if (schemaTables->rangeDistributedRelationIds)
				{
					return true;
				}
				break;
			}

			case APPEND_DISTRIBUTED:
			{
				if (schemaTables->appendDistributedRelationIds)
				{
					return true;
				}
				break;
			}

			case SINGLE_SHARD_DISTRIBUTED:
			{
				if (schemaTables->singleShardRelationIds)
				{
					return true;
				}
				break;
			}

			case REFERENCE_TABLE:
			{
				if (schemaTables->referenceRelationIds)
				{
					return true;
				}
				break;
			}

			case CITUS_LOCAL_TABLE:
			{
				if (schemaTables->citusLocalRelationIds)
				{
					return true;
				}
				break;
			}

			default:
			{
				break;
			}
		}
	}
	return false;
}


/*
 * DisAllowedTableTypesDuringSchemaSet returns Citus table tables which are
 * not allowed inside a schema to be set as a tenant schema.
 */
static List *
DisAllowedTableTypesDuringSchemaSet(void)
{
	List *disallowedCitusTableTypes = NIL;
	disallowedCitusTableTypes = lappend_int(disallowedCitusTableTypes, HASH_DISTRIBUTED);
	disallowedCitusTableTypes = lappend_int(disallowedCitusTableTypes, RANGE_DISTRIBUTED);
	disallowedCitusTableTypes = lappend_int(disallowedCitusTableTypes,
											APPEND_DISTRIBUTED);
	disallowedCitusTableTypes = lappend_int(disallowedCitusTableTypes, REFERENCE_TABLE);
	return disallowedCitusTableTypes;
}


/*
 * DisAllowedTableTypesDuringSchemaUnset returns Citus table tables which are
 * not allowed inside a schema during unset.
 */
static List *
DisAllowedTableTypesDuringSchemaUnset(void)
{
	List *disallowedCitusTableTypes = NIL;
	disallowedCitusTableTypes = lappend_int(disallowedCitusTableTypes, HASH_DISTRIBUTED);
	disallowedCitusTableTypes = lappend_int(disallowedCitusTableTypes, RANGE_DISTRIBUTED);
	disallowedCitusTableTypes = lappend_int(disallowedCitusTableTypes,
											APPEND_DISTRIBUTED);
	disallowedCitusTableTypes = lappend_int(disallowedCitusTableTypes, REFERENCE_TABLE);
	disallowedCitusTableTypes = lappend_int(disallowedCitusTableTypes, CITUS_LOCAL_TABLE);
	return disallowedCitusTableTypes;
}


/*
 * EnsureTenantSchemaAllowed ensures if given schema is applicable for registering
 * as a tenant schema.
 */
static void
EnsureTenantSchemaAllowed(Oid schemaId)
{
	char *schemaName = get_namespace_name(schemaId);

	/* public schema is not allowed */
	if (strcmp(schemaName, "public") == 0)
	{
		ereport(ERROR, (errmsg("public schema cannot be set as a tenant schema")));
	}

	/* information_schema schema is not allowed */
	if (strcmp(schemaName, "information_schema") == 0)
	{
		ereport(ERROR, (errmsg("information_schema schema cannot be set as a tenant "
							   "schema")));
	}

	/* pg_temp_xx and pg_toast_temp_xx schemas are not allowed */
	if (isAnyTempNamespace(schemaId))
	{
		ereport(ERROR, (errmsg("temporary schema cannot be set as a tenant schema")));
	}

	/* pg_catalog schema is not allowed */
	if (IsCatalogNamespace(schemaId))
	{
		ereport(ERROR, (errmsg("pg_catalog schema cannot be set as a tenant schema")));
	}

	/* pg_toast schema is not allowed */
	if (IsToastNamespace(schemaId))
	{
		ereport(ERROR, (errmsg("pg_toast schemacannot be set as a tenant schema")));
	}

	/* any schema owned by extension is not allowed */
	ObjectAddress *schemaAddress = palloc0(sizeof(ObjectAddress));
	ObjectAddressSet(*schemaAddress, NamespaceRelationId, schemaId);
	if (IsAnyObjectAddressOwnedByExtension(list_make1(schemaAddress), NULL))
	{
		ereport(ERROR, (errmsg(
							"the schema %s, which is owned by an extension, cannot be set as a tenant schema",
							schemaName)));
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
 * citus_schema_tenant_set gets a regular schema name, then converts it to a tenant
 * schema.
 */
Datum
citus_schema_tenant_set(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	Oid schemaId = PG_GETARG_OID(0);
	char *schemaName = get_namespace_name(schemaId);
	EnsureTenantSchemaAllowed(schemaId);
	EnsureSchemaOwner(schemaId);
	EnsureCoordinator();

	/* Prevent any update to the schema */
	LockDatabaseObject(NamespaceRelationId, schemaId, 0, AccessExclusiveLock);

	/* Return if the schema is already a tenant schema */
	if (IsTenantSchema(schemaId))
	{
		ereport(NOTICE, (errmsg("schema %s is already a tenant schema", schemaName)));
		PG_RETURN_VOID();
	}
	else
	{
		ereport(NOTICE, (errmsg("schema %s is converted to a tenant schema",
								schemaName)));
	}

	/* Collect all tables by their types in the schema */
	SchemaTables *schemaTables = TablesUnderSchema(schemaId);

	/* Allow only (Citus) local tables and single shard tables in the schema */
	List *disallowedCitusTableTypes = DisAllowedTableTypesDuringSchemaSet();
	if (SchemaHasCitusTableType(schemaTables, disallowedCitusTableTypes))
	{
		ereport(ERROR, (errmsg("schema already has distributed tables"),
						errhint("Undistribute hash distributed and reference "
								"tables under the schema before setting it.")));
	}

	List *singleShardRelationIds = schemaTables->singleShardRelationIds;
	List *localRelationIds = schemaTables->localRelationIds;
	localRelationIds = list_concat(localRelationIds, schemaTables->citusLocalRelationIds);

	/* We check that all table relation kinds are supported by a tenant schema */
	EnsureTableKindsSupportedForTenantSchema(singleShardRelationIds);
	EnsureTableKindsSupportedForTenantSchema(localRelationIds);

	/* All single shard tables should be colocated in the schema */
	if (!AllTablesColocated(singleShardRelationIds))
	{
		ereport(ERROR, (errmsg("schema %s has non-colocated single shard "
							   "tables", schemaName)));
	}

	/*
	 * When we have colocated single shard tables, we want to use their colocation id
	 * during the schema registration. Otherwise, register he schema with a new
	 * colocation id.
	 */
	uint32 colocationId = INVALID_COLOCATION_ID;
	if (singleShardRelationIds != NIL)
	{
		Oid firstSingleShardRelationId = linitial_oid(singleShardRelationIds);
		colocationId = ColocationIdViaCatalog(firstSingleShardRelationId);
	}
	else
	{
		colocationId = CreateTenantSchemaColocationId();
	}

	/* Register the schema locally and sync it to workers */
	InsertTenantSchemaLocally(schemaId, colocationId);
	char *registerSchemaCommand = TenantSchemaInsertCommand(schemaId, colocationId);
	if (EnableMetadataSync)
	{
		SendCommandToWorkersWithMetadata(registerSchemaCommand);
	}

	/*
	 * Now we can convert all local relations to single shard relation. Note that we
	 * already verified that all single tables are colocated and all tables' kind are
	 * supported by a tenant schema.
	 */
	Oid relationId = InvalidOid;
	List *tablesToConvert = NIL;
	foreach_oid(relationId, localRelationIds)
	{
		/*
		 * Skip partitions as they would be distributed/undistibuted by partitioned table.
		 *
		 * We should filter out partitions here before calling CreateTenantSchemaTable.
		 * Otherwise, converted partitioned table would change oid of partitions and its
		 * partition tables would fail wih oid not exist.
		 */
		if (PartitionTable(relationId))
		{
			continue;
		}
		tablesToConvert = lappend_oid(tablesToConvert, relationId);
	}
	foreach_oid(relationId, tablesToConvert)
	{
		CreateTenantSchemaTable(relationId);
	}

	PG_RETURN_VOID();
}


/*
 * citus_schema_tenant_unset gets a tenant schema name, then converts it to a regular
 * schema.
 */
Datum
citus_schema_tenant_unset(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	EnsureCoordinator();

	Oid schemaId = PG_GETARG_OID(0);
	char *schemaName = get_namespace_name(schemaId);
	EnsureTenantSchemaAllowed(schemaId);
	EnsureSchemaOwner(schemaId);

	/* Prevent any update to the schema */
	LockDatabaseObject(NamespaceRelationId, schemaId, 0, AccessExclusiveLock);

	/* The schema should be a tenant schema */
	if (!IsTenantSchema(schemaId))
	{
		ereport(ERROR, (errmsg("schema %s is not a tenant schema", schemaName)));
	}

	ereport(NOTICE, (errmsg("schema %s is converted back to a regular schema",
							schemaName)));

	/* Only single shard tables are expected during unsetting the tenant schema */
	List *disallowedTableTypesForUnset PG_USED_FOR_ASSERTS_ONLY =
		DisAllowedTableTypesDuringSchemaUnset();
	SchemaTables *schemaTables PG_USED_FOR_ASSERTS_ONLY = TablesUnderSchema(schemaId);
	Assert(!SchemaHasCitusTableType(schemaTables, disallowedTableTypesForUnset));

	/*
	 * Unregister schema without removing colocation metadata. Let the user decide
	 * to convert single shard tables into (Citus) local ones later.
	 */
	DeleteTenantSchemaLocally(schemaId);
	char *unregisterSchemaCommand = TenantSchemaDeleteCommand(schemaName);
	if (EnableMetadataSync)
	{
		SendCommandToWorkersWithMetadata(unregisterSchemaCommand);
	}

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
		ereport(ERROR, (errmsg("%s is not allowed for %s because it is a tenant table",
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
