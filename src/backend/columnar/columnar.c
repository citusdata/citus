/*-------------------------------------------------------------------------
 *
 * columnar.c
 *
 * This file contains...
 *
 * Copyright (c) 2016, Citus Data, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#include <sys/stat.h>
#include <unistd.h>

#include "postgres.h"

#include "miscadmin.h"

#include "utils/guc.h"
#include "utils/rel.h"

#include "citus_version.h"

#include "columnar/columnar.h"
#include "columnar/columnar_tableam.h"

/* Default values for option parameters */
#define DEFAULT_STRIPE_ROW_COUNT 150000
#define DEFAULT_CHUNK_ROW_COUNT 10000

#if HAVE_LIBZSTD
#define DEFAULT_COMPRESSION_TYPE COMPRESSION_ZSTD
#elif HAVE_CITUS_LIBLZ4
#define DEFAULT_COMPRESSION_TYPE COMPRESSION_LZ4
#else
#define DEFAULT_COMPRESSION_TYPE COMPRESSION_PG_LZ
#endif

int columnar_compression = DEFAULT_COMPRESSION_TYPE;
int columnar_stripe_row_limit = DEFAULT_STRIPE_ROW_COUNT;
int columnar_chunk_group_row_limit = DEFAULT_CHUNK_ROW_COUNT;
int columnar_compression_level = 3;

static const struct config_enum_entry columnar_compression_options[] =
{
	{ "none", COMPRESSION_NONE, false },
	{ "pglz", COMPRESSION_PG_LZ, false },
#if HAVE_CITUS_LIBLZ4
	{ "lz4", COMPRESSION_LZ4, false },
#endif
#if HAVE_LIBZSTD
	{ "zstd", COMPRESSION_ZSTD, false },
#endif
	{ NULL, 0, false }
};

void
columnar_init(void)
{
	columnar_init_gucs();
	columnar_tableam_init();
}


void
columnar_init_gucs()
{
	DefineCustomEnumVariable("columnar.compression",
							 "Compression type for columnar.",
							 NULL,
							 &columnar_compression,
							 DEFAULT_COMPRESSION_TYPE,
							 columnar_compression_options,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomIntVariable("columnar.compression_level",
							"Compression level to be used with zstd.",
							NULL,
							&columnar_compression_level,
							3,
							COMPRESSION_LEVEL_MIN,
							COMPRESSION_LEVEL_MAX,
							PGC_USERSET,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable("columnar.stripe_row_limit",
							"Maximum number of tuples per stripe.",
							NULL,
							&columnar_stripe_row_limit,
							DEFAULT_STRIPE_ROW_COUNT,
							STRIPE_ROW_COUNT_MINIMUM,
							STRIPE_ROW_COUNT_MAXIMUM,
							PGC_USERSET,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable("columnar.chunk_group_row_limit",
							"Maximum number of rows per chunk.",
							NULL,
							&columnar_chunk_group_row_limit,
							DEFAULT_CHUNK_ROW_COUNT,
							CHUNK_ROW_COUNT_MINIMUM,
							CHUNK_ROW_COUNT_MAXIMUM,
							PGC_USERSET,
							0,
							NULL,
							NULL,
							NULL);
}


/*
 * ParseCompressionType converts a string to a compression type.
 * For compression algorithms that are invalid or not compiled, it
 * returns COMPRESSION_TYPE_INVALID.
 */
CompressionType
ParseCompressionType(const char *compressionTypeString)
{
	Assert(compressionTypeString != NULL);

	for (int compressionIndex = 0;
		 columnar_compression_options[compressionIndex].name != NULL;
		 compressionIndex++)
	{
		const char *compressionName = columnar_compression_options[compressionIndex].name;
		if (strncmp(compressionTypeString, compressionName, NAMEDATALEN) == 0)
		{
			return columnar_compression_options[compressionIndex].val;
		}
	}

	return COMPRESSION_TYPE_INVALID;
}


/*
 * CompressionTypeStr returns string representation of a compression type.
 * For compression algorithms that are invalid or not compiled, it
 * returns NULL.
 */
const char *
CompressionTypeStr(CompressionType requestedType)
{
	for (int compressionIndex = 0;
		 columnar_compression_options[compressionIndex].name != NULL;
		 compressionIndex++)
	{
		CompressionType compressionType =
			columnar_compression_options[compressionIndex].val;
		if (compressionType == requestedType)
		{
			return columnar_compression_options[compressionIndex].name;
		}
	}

	return NULL;
}
