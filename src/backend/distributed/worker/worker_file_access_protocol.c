/*-------------------------------------------------------------------------
 *
 * worker_file_access_protocol.c
 *
 * Routines for accessing file related information on this worker node.
 *
 * Copyright (c) 2012-2016, Citus Data, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "funcapi.h"

#include "commands/defrem.h"
#include "distributed/master_protocol.h"
#include "distributed/worker_protocol.h"
#include "foreign/foreign.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"


/* exports for SQL callable functions */
PG_FUNCTION_INFO_V1(worker_foreign_file_path);
PG_FUNCTION_INFO_V1(worker_find_block_local_path);


/*
 * worker_foreign_file_path resolves the foreign table for the given table name,
 * and extracts and returns the file path associated with that foreign table.
 */
Datum
worker_foreign_file_path(PG_FUNCTION_ARGS)
{
	text *foreignTableName = PG_GETARG_TEXT_P(0);
	text *foreignFilePath = NULL;
	Oid relationId = ResolveRelationId(foreignTableName);
	ForeignTable *foreignTable = GetForeignTable(relationId);

	ListCell *optionCell = NULL;
	foreach(optionCell, foreignTable->options)
	{
		DefElem *option = (DefElem *) lfirst(optionCell);
		char *optionName = option->defname;

		int compareResult = strncmp(optionName, FOREIGN_FILENAME_OPTION, MAXPGPATH);
		if (compareResult == 0)
		{
			char *optionValue = defGetString(option);
			foreignFilePath = cstring_to_text(optionValue);
			break;
		}
	}

	/* check that we found the filename option */
	if (foreignFilePath == NULL)
	{
		char *relationName = get_rel_name(relationId);
		ereport(ERROR, (errmsg("could not find filename for foreign table: \"%s\"",
							   relationName)));
	}

	PG_RETURN_TEXT_P(foreignFilePath);
}


/*
 * Protocol declaration for a function whose future implementation will find the
 * given HDFS block's local file path.
 */
Datum
worker_find_block_local_path(PG_FUNCTION_ARGS)
{
	int64 blockId = PG_GETARG_INT64(0);
	ArrayType *dataDirectoryObject = PG_GETARG_ARRAYTYPE_P(1);

	/* keep the compiler silent */
	(void) blockId;
	(void) dataDirectoryObject;

	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("called function is currently unsupported")));

	PG_RETURN_TEXT_P(NULL);
}
