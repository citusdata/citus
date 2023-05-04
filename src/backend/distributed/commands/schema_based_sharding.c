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


static void SetTenantSchemaColocationId(Oid schemaId, uint32 colocationId);


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
 * ShouldCreateTenantTable returns true if given parsetree is a CREATE TABLE
 * statement and the table should be treated as a tenant table.
 */
bool
ShouldCreateTenantTable(Oid relationId)
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
		 * let ShouldCreateTenantTable() to return false to allow users to
		 * create a local table as usual but that would be confusing because
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
	 * Decide name of the table with lowest oid in the colocation group
	 * and use it as the colocate_with parameter.
	 */
	char *colocateWithTableName = "none";

	/*
	 * Acquire default colocation lock to prevent concurrently forming
	 * multiple colocation groups for the same schema.
	 *
	 * Note that the lock is based on schemaId to avoid serializing
	 * default colocation group creation for all tenant schemas.
	 */
	Oid schemaId = get_rel_namespace(relationId);
	AcquireCitusTenantSchemaDefaultColocationLock(schemaId);

	uint32 colocationId = SchemaIdGetTenantColocationId(schemaId);
	bool firstTableInSchema = (colocationId == INVALID_COLOCATION_ID);
	if (!firstTableInSchema)
	{
		/*
		 * Release the lock if the schema is already associated with a
		 * colocation group.
		 */
		ReleaseCitusTenantSchemaDefaultColocationLock(schemaId);

		Oid colocateWithTableId = ColocationGroupGetTableWithLowestOid(colocationId);
		colocateWithTableName = generate_qualified_relation_name(colocateWithTableId);
	}

	CreateSingleShardTable(relationId, colocateWithTableName);

	if (firstTableInSchema)
	{
		/*
		 * Save it into pg_dist_tenant_schema if this is the first tenant
		 * table in the schema.
		 */
		SetTenantSchemaColocationId(schemaId, TableColocationId(relationId));
	}
}


/*
 * RegisterTenantSchema registers given schema as a tenant schema locally and
 * returns the command to do the same on the workers.
 */
char *
RegisterTenantSchema(Oid schemaId)
{
	CheckCitusVersion(ERROR);

	/* not assign a colocation id until creating the first table */
	uint32 colocationId = INVALID_COLOCATION_ID;

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


/*
 * DisassociateTenantSchemaIfAny disassociates given colocation id from its
 * tenant schema if any.
 */
void
DisassociateTenantSchemaIfAny(uint32 colocationId)
{
	Oid schemaId = ColocationIdGetTenantSchemaId(colocationId);
	if (OidIsValid(schemaId))
	{
		SetTenantSchemaColocationId(schemaId, INVALID_COLOCATION_ID);
	}
}


/*
 * SetTenantSchemaColocationId sets the colocation id of given tenant schema.
 *
 * If colocationId is INVALID_COLOCATION_ID, then the colocation_id column
 * is set to NULL.
 */
static void
SetTenantSchemaColocationId(Oid schemaId, uint32 colocationId)
{
	SetTenantSchemaColocationIdLocally(schemaId, colocationId);

	SendCommandToWorkersWithMetadataViaSuperUser(
		TenantSchemaSetColocationIdCommand(schemaId, colocationId));
}
