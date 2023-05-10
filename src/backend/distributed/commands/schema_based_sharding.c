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
#include "distributed/backend_data.h"
#include "distributed/colocation_utils.h"
#include "distributed/commands.h"
#include "distributed/metadata_cache.h"
#include "distributed/metadata_sync.h"
#include "distributed/tenant_schema_metadata.h"
#include "distributed/metadata/distobject.h"
#include "utils/lsyscache.h"


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
 * IsTenantSchema returns true if there is a tenant schema with given schemaId.
 */
bool
IsTenantSchema(Oid schemaId)
{
	/*
	 * We don't allow creating tenant schemas when there is a version
	 * mismatch. Even more, SchemaIdGetTenantColocationId() would throw an
	 * error if the underlying pg_dist_tenant_schema metadata table has not
	 * been created yet, which is the case in older versions. For this reason,
	 * it's safe to assume that it cannot be a tenant schema when there is a
	 * version mismatch.
	 *
	 * But it's a bit tricky that we do the same when version checks are
	 * disabled because then CheckCitusVersion() returns true even if there
	 * is a version mismatch. And in that case, the test that tries to create
	 * a distributed table (in multi_extension.sql) in an older version would
	 * fail when deciding whether we're trying to colocate given distributed
	 * table with a tenant table.
	 *
	 * The downside of doing so is that, for example, we will skip deleting
	 * the tenant schema entry from pg_dist_tenant_schema when dropping a
	 * tenant schema while the version checks are disabled even if there was
	 * no version mismatch. But we're okay with that because we don't expect
	 * users to disable version checks anyway.
	 */
	if (!EnableVersionChecks || !CheckCitusVersion(DEBUG4))
	{
		return false;
	}

	return SchemaIdGetTenantColocationId(schemaId) != INVALID_COLOCATION_ID;
}


/*
 * CreateTenantTable creates a tenant table with given relationId.
 *
 * This means creating a single shard distributed table without a shard
 * key and colocating it with the other tables in its schema.
 */
void
CreateTenantTable(Oid relationId)
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
 * RegisterTenantSchema registers given schema as a tenant schema locally and
 * returns the command to do the same on the workers.
 */
char *
RegisterTenantSchema(Oid schemaId)
{
	CheckCitusVersion(ERROR);

	int shardCount = 1;
	int replicationFactor = 1;
	Oid distributionColumnType = InvalidOid;
	Oid distributionColumnCollation = InvalidOid;
	uint32 colocationId = CreateColocationGroup(
		shardCount, replicationFactor, distributionColumnType,
		distributionColumnCollation);

	InsertTenantSchemaLocally(schemaId, colocationId);

	return TenantSchemaInsertCommand(schemaId, colocationId);
}


/*
 * UnregisterTenantSchema deletes tenant schema metadata related to given
 * schema.
 */
void
UnregisterTenantSchema(Oid schemaId)
{
	DeleteTenantSchemaLocally(schemaId);

	SendCommandToWorkersWithMetadataViaSuperUser(
		TenantSchemaDeleteCommand(schemaId));
}
