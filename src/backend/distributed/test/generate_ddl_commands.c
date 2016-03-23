/*-------------------------------------------------------------------------
 *
 * test/src/generate_ddl_commands.c
 *
 * This file contains functions to exercise DDL generation functionality
 * within Citus.
 *
 * Copyright (c) 2014-2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "c.h"
#include "fmgr.h"

#include <stddef.h>

#include "catalog/pg_type.h"
#include "distributed/master_protocol.h"
#include "distributed/test_helper_functions.h" /* IWYU pragma: keep */
#include "lib/stringinfo.h"
#include "nodes/makefuncs.h"
#include "nodes/nodes.h"
#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
#include "nodes/value.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/palloc.h"


/* declarations for dynamic loading */
PG_FUNCTION_INFO_V1(table_ddl_command_array);


/*
 * table_ddl_command_array returns an array of strings, each of which is a DDL
 * command required to recreate a table (specified by OID).
 */
Datum
table_ddl_command_array(PG_FUNCTION_ARGS)
{
	Oid distributedTableId = PG_GETARG_OID(0);
	ArrayType *ddlCommandArrayType = NULL;
	List *ddlCommandList = GetTableDDLEvents(distributedTableId);
	int ddlCommandCount = list_length(ddlCommandList);
	Datum *ddlCommandDatumArray = palloc0(ddlCommandCount * sizeof(Datum));

	ListCell *ddlCommandCell = NULL;
	int ddlCommandIndex = 0;
	Oid ddlCommandTypeId = TEXTOID;

	foreach(ddlCommandCell, ddlCommandList)
	{
		char *ddlCommand = (char *) lfirst(ddlCommandCell);
		Datum ddlCommandDatum = CStringGetTextDatum(ddlCommand);

		ddlCommandDatumArray[ddlCommandIndex] = ddlCommandDatum;
		ddlCommandIndex++;
	}

	ddlCommandArrayType = DatumArrayToArrayType(ddlCommandDatumArray, ddlCommandCount,
												ddlCommandTypeId);

	PG_RETURN_ARRAYTYPE_P(ddlCommandArrayType);
}
