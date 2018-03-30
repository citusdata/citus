/*-------------------------------------------------------------------------
 *
 * distribution_column.c
 *
 * This file contains functions for translating distribution columns in
 * metadata tables.
 *
 * Copyright (c) 2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"


#include "access/attnum.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "distributed/distribution_column.h"
#include "distributed/metadata_cache.h"
#include "distributed/version_compat.h"
#include "nodes/makefuncs.h"
#include "nodes/nodes.h"
#include "nodes/primnodes.h"
#include "parser/scansup.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/errcodes.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/syscache.h"


/* exports for SQL callable functions */
PG_FUNCTION_INFO_V1(column_name_to_column);
PG_FUNCTION_INFO_V1(column_name_to_column_id);
PG_FUNCTION_INFO_V1(column_to_column_name);


/*
 * column_name_to_column is an internal UDF to obtain a textual representation
 * of a particular column node (Var), given a relation identifier and column
 * name. There is no requirement that the table be distributed; this function
 * simply returns the textual representation of a Var representing a column.
 * This function will raise an ERROR if no such column can be found or if the
 * provided name refers to a system column.
 */
Datum
column_name_to_column(PG_FUNCTION_ARGS)
{
	Oid relationId = PG_GETARG_OID(0);
	text *columnText = PG_GETARG_TEXT_P(1);
	Relation relation = NULL;
	char *columnName = text_to_cstring(columnText);
	Var *column = NULL;
	char *columnNodeString = NULL;
	text *columnNodeText = NULL;

	CheckCitusVersion(ERROR);

	relation = relation_open(relationId, AccessShareLock);

	column = BuildDistributionKeyFromColumnName(relation, columnName);
	columnNodeString = nodeToString(column);
	columnNodeText = cstring_to_text(columnNodeString);

	relation_close(relation, AccessShareLock);

	PG_RETURN_TEXT_P(columnNodeText);
}


/*
 * column_name_to_column_id takes a relation identifier and a name of a column
 * in that relation and returns the index of that column in the relation. If
 * the provided name is a system column or no column at all, this function will
 * throw an error instead.
 */
Datum
column_name_to_column_id(PG_FUNCTION_ARGS)
{
	Oid distributedTableId = PG_GETARG_OID(0);
	char *columnName = PG_GETARG_CSTRING(1);
	Relation relation = NULL;
	Var *column = NULL;

	relation = relation_open(distributedTableId, AccessExclusiveLock);

	column = BuildDistributionKeyFromColumnName(relation, columnName);

	relation_close(relation, NoLock);

	PG_RETURN_INT16((int16) column->varattno);
}


/*
 * column_to_column_name is an internal UDF to obtain the human-readable name
 * of a column given a relation identifier and the column's internal textual
 * (Var) representation. This function will raise an ERROR if no such column
 * can be found or if the provided Var refers to a system column.
 */
Datum
column_to_column_name(PG_FUNCTION_ARGS)
{
	Oid relationId = PG_GETARG_OID(0);
	text *columnNodeText = PG_GETARG_TEXT_P(1);

	char *columnNodeString = text_to_cstring(columnNodeText);
	char *columnName = NULL;
	text *columnText = NULL;

	CheckCitusVersion(ERROR);

	columnName = ColumnNameToColumn(relationId, columnNodeString);

	columnText = cstring_to_text(columnName);

	PG_RETURN_TEXT_P(columnText);
}


/*
 * BuildDistributionKeyFromColumnName builds a simple distribution key consisting
 * only out of a reference to the column of name columnName. Errors out if the
 * specified column does not exist or is not suitable to be used as a
 * distribution column.
 *
 * The function returns NULL if the passed column name is NULL. That case only
 * corresponds to reference tables.
 */
Var *
BuildDistributionKeyFromColumnName(Relation distributedRelation, char *columnName)
{
	HeapTuple columnTuple = NULL;
	Form_pg_attribute columnForm = NULL;
	Var *distributionColumn = NULL;
	char *tableName = RelationGetRelationName(distributedRelation);

	/* short circuit for reference tables */
	if (columnName == NULL)
	{
		return NULL;
	}

	/* it'd probably better to downcase identifiers consistent with SQL case folding */
	truncate_identifier(columnName, strlen(columnName), true);

	/* lookup column definition */
	columnTuple = SearchSysCacheAttName(RelationGetRelid(distributedRelation),
										columnName);
	if (!HeapTupleIsValid(columnTuple))
	{
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_COLUMN),
						errmsg("column \"%s\" of relation \"%s\" does not exist",
							   columnName, tableName)));
	}

	columnForm = (Form_pg_attribute) GETSTRUCT(columnTuple);

	/* check if the column may be referenced in the distribution key */
	if (columnForm->attnum <= 0)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot reference system column \"%s\" in relation \"%s\"",
							   columnName, tableName)));
	}

	/* build Var referencing only the chosen distribution column */
	distributionColumn = makeVar(1, columnForm->attnum, columnForm->atttypid,
								 columnForm->atttypmod, columnForm->attcollation, 0);

	ReleaseSysCache(columnTuple);

	return distributionColumn;
}


/*
 * ColumnNameToColumn returns the human-readable name of a column given a
 * relation identifier and the column's internal textual (Var) representation.
 * This function will raise an ERROR if no such column can be found or if the
 * provided Var refers to a system column.
 */
char *
ColumnNameToColumn(Oid relationId, char *columnNodeString)
{
	Node *columnNode = NULL;
	Var *column = NULL;
	AttrNumber columnNumber = InvalidAttrNumber;
	char *columnName = NULL;

	columnNode = stringToNode(columnNodeString);

	Assert(IsA(columnNode, Var));
	column = (Var *) columnNode;

	columnNumber = column->varattno;
	if (!AttrNumberIsForUserDefinedAttr(columnNumber))
	{
		char *relationName = get_rel_name(relationId);

		ereport(ERROR, (errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
						errmsg("attribute %d of relation \"%s\" is a system column",
							   columnNumber, relationName)));
	}

	columnName = get_attname_internal(relationId, column->varattno, false);
	if (columnName == NULL)
	{
		char *relationName = get_rel_name(relationId);

		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_COLUMN),
						errmsg("attribute %d of relation \"%s\" does not exist",
							   columnNumber, relationName)));
	}

	return columnName;
}
