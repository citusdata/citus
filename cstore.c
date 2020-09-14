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

#include "miscadmin.h"
#include "utils/rel.h"

#include "cstore.h"

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
 * InitializeCStoreTableFile creates data and footer file for a cstore table.
 * The function assumes data and footer files do not exist, therefore
 * it should be called on empty or non-existing table. Notice that the caller
 * is expected to acquire AccessExclusiveLock on the relation.
 */
void
InitializeCStoreTableFile(Oid relationId, Relation relation, CStoreOptions *cstoreOptions)
{
	TableWriteState *writeState = NULL;
	TupleDesc tupleDescriptor = RelationGetDescr(relation);

	InitCStoreTableMetadata(relationId, cstoreOptions->blockRowCount);

	/*
	 * Initialize state to write to the cstore file. This creates an
	 * empty data file and a valid footer file for the table.
	 */
	writeState = CStoreBeginWrite(relationId,
								  cstoreOptions->compressionType,
								  cstoreOptions->stripeRowCount,
								  cstoreOptions->blockRowCount, tupleDescriptor);
	CStoreEndWrite(writeState);
}
