/*-------------------------------------------------------------------------
 *
 * test/src/partitioning_utils.c
 *
 * This file contains functions to test partitioning utility functions
 * implemented in Citus.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "fmgr.h"

#include "catalog/pg_type.h"
#include "lib/stringinfo.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"

#include "distributed/listutils.h"
#include "distributed/multi_partitioning_utils.h"
#include "distributed/reference_table_utils.h"


PG_FUNCTION_INFO_V1(generate_alter_table_detach_partition_command);
PG_FUNCTION_INFO_V1(generate_alter_table_attach_partition_command);
PG_FUNCTION_INFO_V1(generate_partition_information);
PG_FUNCTION_INFO_V1(print_partitions);
PG_FUNCTION_INFO_V1(table_inherits);
PG_FUNCTION_INFO_V1(table_inherited);


/*
 * Just a wrapper around GenereateDetachPartitionCommand().
 */
Datum
generate_alter_table_detach_partition_command(PG_FUNCTION_ARGS)
{
	char *command = "";

	command = GenerateDetachPartitionCommand(PG_GETARG_OID(0));

	PG_RETURN_TEXT_P(cstring_to_text(command));
}


/*
 * Just a wrapper around GenerateAlterTableAttachPartitionCommand().
 */
Datum
generate_alter_table_attach_partition_command(PG_FUNCTION_ARGS)
{
	char *command = "";

	command = GenerateAlterTableAttachPartitionCommand(PG_GETARG_OID(0));

	PG_RETURN_TEXT_P(cstring_to_text(command));
}


/*
 * Just a wrapper around GenereatePartitioningInformation().
 */
Datum
generate_partition_information(PG_FUNCTION_ARGS)
{
	char *command = "";

	command = GeneratePartitioningInformation(PG_GETARG_OID(0));

	PG_RETURN_TEXT_P(cstring_to_text(command));
}


/*
 * Just a wrapper around PartitionList() with human readable table name outpus.
 */
Datum
print_partitions(PG_FUNCTION_ARGS)
{
	StringInfo resultRelationNames = makeStringInfo();

	List *partitionList = PartitionList(PG_GETARG_OID(0));
	partitionList = SortList(partitionList, CompareOids);

	Oid partitionOid = InvalidOid;
	foreach_declared_oid(partitionOid, partitionList)
	{
		/* at least one table is already added, add comma */
		if (resultRelationNames->len > 0)
		{
			appendStringInfoString(resultRelationNames, ",");
		}

		appendStringInfoString(resultRelationNames, get_rel_name(partitionOid));
	}

	PG_RETURN_TEXT_P(cstring_to_text(resultRelationNames->data));
}


/*
 * Just a wrapper around IsChildTable()
 */
Datum
table_inherits(PG_FUNCTION_ARGS)
{
	return IsChildTable(PG_GETARG_OID(0));
}


/*
 * Just a wrapper around IsParentTable()
 */
Datum
table_inherited(PG_FUNCTION_ARGS)
{
	return IsParentTable(PG_GETARG_OID(0));
}
