/*-------------------------------------------------------------------------
 * schema_based_sharding.c
 *
 *	  Routines for schema-based sharding.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "miscadmin.h"
#include "catalog/pg_namespace_d.h"
#include "commands/extension.h"
#include "distributed/argutils.h"
#include "distributed/backend_data.h"
#include "distributed/colocation_utils.h"
#include "distributed/commands.h"
#include "distributed/metadata_sync.h"
#include "distributed/multi_partitioning_utils.h"
#include "distributed/tenant_schema_metadata.h"
#include "distributed/metadata/distobject.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"


PG_FUNCTION_INFO_V1(citus_internal_unregister_tenant_schema_globally);


/* controlled via citus.enable_schema_based_sharding GUC */
bool EnableSchemaBasedSharding = false;


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
 * ErrorIfTenantTable errors out with the given operation name,
 * if the given relation is a tenant table.
 */
void
ErrorIfTenantTable(Oid relationId, char *operationName)
{
	if (IsTenantSchema(get_rel_namespace(relationId)))
	{
		ereport(ERROR, (errmsg("%s is not allowed for %s because it is a tenant table",
			get_rel_name(relationId), operationName)));
	}
}
