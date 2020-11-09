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
#include "utils/guc.h"
#include "utils/rel.h"

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
