/*-------------------------------------------------------------------------
 *
 * cstore.c
 *
 * This file contains...
 *
 * Copyright (c) 2016, Citus Data, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <sys/stat.h>
#include <unistd.h>

#include "access/heapam.h"
#include "catalog/objectaccess.h"
#include "catalog/storage.h"
#include "miscadmin.h"
#include "utils/guc.h"
#include "utils/rel.h"
#include "utils/relcache.h"

#include "cstore.h"

/* Default values for option parameters */
#define DEFAULT_COMPRESSION_TYPE COMPRESSION_NONE
#define DEFAULT_STRIPE_ROW_COUNT 150000
#define DEFAULT_BLOCK_ROW_COUNT 10000

int cstore_compression = DEFAULT_COMPRESSION_TYPE;
int cstore_stripe_row_count = DEFAULT_STRIPE_ROW_COUNT;
int cstore_block_row_count = DEFAULT_BLOCK_ROW_COUNT;

static const struct config_enum_entry cstore_compression_options[] =
{
	{ "none", COMPRESSION_NONE, false },
	{ "pglz", COMPRESSION_PG_LZ, false },
	{ NULL, 0, false }
};

static object_access_hook_type prevObjectAccess = NULL;

static void ObjectAccess(ObjectAccessType access, Oid classId, Oid objectId, int subId,
						 void *arg);

void
cstore_init()
{
	DefineCustomEnumVariable("cstore.compression",
							 "Compression type for cstore.",
							 NULL,
							 &cstore_compression,
							 DEFAULT_COMPRESSION_TYPE,
							 cstore_compression_options,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomIntVariable("cstore.stripe_row_count",
							"Maximum number of tuples per stripe.",
							NULL,
							&cstore_stripe_row_count,
							DEFAULT_STRIPE_ROW_COUNT,
							STRIPE_ROW_COUNT_MINIMUM,
							STRIPE_ROW_COUNT_MAXIMUM,
							PGC_USERSET,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable("cstore.block_row_count",
							"Maximum number of rows per block.",
							NULL,
							&cstore_block_row_count,
							DEFAULT_BLOCK_ROW_COUNT,
							BLOCK_ROW_COUNT_MINIMUM,
							BLOCK_ROW_COUNT_MAXIMUM,
							PGC_USERSET,
							0,
							NULL,
							NULL,
							NULL);

	prevObjectAccess = object_access_hook;
	object_access_hook = ObjectAccess;
}


/* ParseCompressionType converts a string to a compression type. */
CompressionType
ParseCompressionType(const char *compressionTypeString)
{
	CompressionType compressionType = COMPRESSION_TYPE_INVALID;
	Assert(compressionTypeString != NULL);

	if (strncmp(compressionTypeString, COMPRESSION_STRING_NONE, NAMEDATALEN) == 0)
	{
		compressionType = COMPRESSION_NONE;
	}
	else if (strncmp(compressionTypeString, COMPRESSION_STRING_PG_LZ, NAMEDATALEN) == 0)
	{
		compressionType = COMPRESSION_PG_LZ;
	}

	return compressionType;
}


/*
 * InitializeCStoreTableFile initializes metadata for the given relation
 * file node.
 */
void
InitializeCStoreTableFile(Oid relNode, CStoreOptions *cstoreOptions)
{
	InitCStoreTableMetadata(relNode, cstoreOptions->blockRowCount);
}


/*
 * Implements object_access_hook. One of the places this is called is just
 * before dropping an object, which allows us to clean-up resources for
 * cstore tables while the pg_class record for the table is still there.
 */
static void
ObjectAccess(ObjectAccessType access, Oid classId, Oid objectId, int subId, void *arg)
{
	if (prevObjectAccess)
	{
		prevObjectAccess(access, classId, objectId, subId, arg);
	}

	/*
	 * Do nothing if this is not a DROP relation command.
	 */
	if (access != OAT_DROP || classId != RelationRelationId || OidIsValid(subId))
	{
		return;
	}

	if (IsCStoreFdwTable(objectId))
	{
		/*
		 * Drop both metadata and storage. We need to drop storage here since
		 * we manage relfilenode for FDW tables in the extension.
		 */
		Relation rel = cstore_fdw_open(objectId, AccessExclusiveLock);
		RelationOpenSmgr(rel);
		RelationDropStorage(rel);
		DeleteTableMetadataRowIfExists(rel->rd_node.relNode);

		/* keep the lock since we did physical changes to the relation */
		relation_close(rel, NoLock);
	}
	else
	{
		Oid relNode = InvalidOid;
		Relation rel = try_relation_open(objectId, AccessExclusiveLock);
		if (rel == NULL)
		{
			return;
		}

		relNode = rel->rd_node.relNode;
		if (IsCStoreStorage(relNode))
		{
			/*
			 * Drop only metadata for table am cstore tables. Postgres manages
			 * storage for these tables, so we don't need to drop that.
			 */
			DeleteTableMetadataRowIfExists(relNode);

			/* keep the lock since we did physical changes to the relation */
			relation_close(rel, NoLock);
		}
		else
		{
			/*
			 * For non-cstore tables, we do nothing.
			 * Release the lock since we haven't changed the relation.
			 */
			relation_close(rel, AccessExclusiveLock);
		}
	}
}
