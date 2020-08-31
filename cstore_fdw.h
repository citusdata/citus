/*-------------------------------------------------------------------------
 *
 * cstore_fdw.h
 *
 * Type and function declarations for CStore foreign data wrapper.
 *
 * Copyright (c) 2016, Citus Data, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#ifndef CSTORE_FDW_H
#define CSTORE_FDW_H

#include "access/tupdesc.h"
#include "fmgr.h"
#include "catalog/pg_am.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_foreign_table.h"
#include "lib/stringinfo.h"
#include "utils/rel.h"

#include "cstore.h"

/* table containing information about how to partition distributed tables */
#define CITUS_EXTENSION_NAME "citus"
#define CITUS_PARTITION_TABLE_NAME "pg_dist_partition"

/* human-readable names for addressing columns of the pg_dist_partition table */
#define ATTR_NUM_PARTITION_RELATION_ID 1
#define ATTR_NUM_PARTITION_TYPE 2
#define ATTR_NUM_PARTITION_KEY 3

/*
 * CStoreValidOption keeps an option name and a context. When an option is passed
 * into cstore_fdw objects (server and foreign table), we compare this option's
 * name and context against those of valid options.
 */
typedef struct CStoreValidOption
{
	const char *optionName;
	Oid optionContextId;

} CStoreValidOption;

#define COMPRESSION_STRING_DELIMITED_LIST "none, pglz"

/* Array of options that are valid for cstore_fdw */
static const uint32 ValidOptionCount = 4;
static const CStoreValidOption ValidOptionArray[] =
{
	/* foreign table options */
	{ OPTION_NAME_FILENAME, ForeignTableRelationId },
	{ OPTION_NAME_COMPRESSION_TYPE, ForeignTableRelationId },
	{ OPTION_NAME_STRIPE_ROW_COUNT, ForeignTableRelationId },
	{ OPTION_NAME_BLOCK_ROW_COUNT, ForeignTableRelationId }
};

void cstore_fdw_init(void);
void cstore_fdw_finish(void);

/* event trigger function declarations */
extern Datum cstore_ddl_event_end_trigger(PG_FUNCTION_ARGS);

/* Function declarations for utility UDFs */
extern Datum cstore_table_size(PG_FUNCTION_ARGS);
extern Datum cstore_clean_table_resources(PG_FUNCTION_ARGS);

/* Function declarations for foreign data wrapper */
extern Datum cstore_fdw_handler(PG_FUNCTION_ARGS);
extern Datum cstore_fdw_validator(PG_FUNCTION_ARGS);

#endif   /* CSTORE_FDW_H */ 
