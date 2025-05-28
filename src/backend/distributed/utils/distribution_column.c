/*-------------------------------------------------------------------------
 *
 * distribution_column.c
 *
 * This file contains functions for translating distribution columns in
 * metadata tables.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/attnum.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "nodes/makefuncs.h"
#include "nodes/nodes.h"
#include "nodes/primnodes.h"
#include "parser/parse_relation.h"
#include "parser/scansup.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/errcodes.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/syscache.h"

#include "distributed/distribution_column.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_partitioning_utils.h"
#include "distributed/version_compat.h"


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
	CheckCitusVersion(ERROR);

	Oid relationId = PG_GETARG_OID(0);
	text *columnText = PG_GETARG_TEXT_P(1);
	char *columnName = text_to_cstring(columnText);

	Var *column = BuildDistributionKeyFromColumnName(relationId, columnName,
													 AccessShareLock);
	Assert(column != NULL);
	char *columnNodeString = nodeToString(column);
	text *columnNodeText = cstring_to_text(columnNodeString);

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

	Var *column = BuildDistributionKeyFromColumnName(distributedTableId, columnName,
													 AccessExclusiveLock);
	Assert(column != NULL);

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
	CheckCitusVersion(ERROR);

	Oid relationId = PG_GETARG_OID(0);
	text *columnNodeText = PG_GETARG_TEXT_P(1);

	char *columnNodeString = text_to_cstring(columnNodeText);
	Node *columnNode = stringToNode(columnNodeString);

	char *columnName = ColumnToColumnName(relationId, columnNode);

	text *columnText = cstring_to_text(columnName);

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
BuildDistributionKeyFromColumnName(Oid relationId, char *columnName, LOCKMODE lockMode)
{
	Relation relation = try_relation_open(relationId, lockMode);

	if (relation == NULL)
	{
		ereport(ERROR, (errmsg("relation does not exist")));
	}

	relation_close(relation, NoLock);

	char *tableName = get_rel_name(relationId);

	/* short circuit for reference tables and single-shard tables */
	if (columnName == NULL)
	{
		return NULL;
	}

	/* it'd probably better to downcase identifiers consistent with SQL case folding */
	truncate_identifier(columnName, strlen(columnName), true);

	/* lookup column definition */
	HeapTuple columnTuple = SearchSysCacheAttName(relationId, columnName);
	if (!HeapTupleIsValid(columnTuple))
	{
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_COLUMN),
						errmsg("column \"%s\" of relation \"%s\" does not exist",
							   columnName, tableName)));
	}

	Form_pg_attribute columnForm = (Form_pg_attribute) GETSTRUCT(columnTuple);

	/* check if the column may be referenced in the distribution key */
	if (columnForm->attnum <= 0)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot reference system column \"%s\" in relation \"%s\"",
							   columnName, tableName)));
	}

	/* build Var referencing only the chosen distribution column */
	Var *distributionColumn = makeVar(1, columnForm->attnum, columnForm->atttypid,
									  columnForm->atttypmod, columnForm->attcollation, 0);

	ReleaseSysCache(columnTuple);

	return distributionColumn;
}


/*
 * EnsureValidDistributionColumn Errors out if the
 * specified column does not exist or is not suitable to be used as a
 * distribution column. It does not hold locks.
 */
void
EnsureValidDistributionColumn(Oid relationId, char *columnName)
{
	Relation relation = try_relation_open(relationId, AccessShareLock);

	if (relation == NULL)
	{
		ereport(ERROR, (errmsg("relation does not exist")));
	}

	char *tableName = get_rel_name(relationId);

	/* it'd probably better to downcase identifiers consistent with SQL case folding */
	truncate_identifier(columnName, strlen(columnName), true);

	/* lookup column definition */
	HeapTuple columnTuple = SearchSysCacheAttName(relationId, columnName);
	if (!HeapTupleIsValid(columnTuple))
	{
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_COLUMN),
						errmsg("column \"%s\" of relation \"%s\" does not exist",
							   columnName, tableName)));
	}

	Form_pg_attribute columnForm = (Form_pg_attribute) GETSTRUCT(columnTuple);

	/* check if the column may be referenced in the distribution key */
	if (columnForm->attnum <= 0)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot reference system column \"%s\" in relation \"%s\"",
							   columnName, tableName)));
	}

	ReleaseSysCache(columnTuple);

	relation_close(relation, AccessShareLock);
}


/*
 * ColumnTypeIdForRelationColumnName returns type id for the given relation's column name.
 */
Oid
ColumnTypeIdForRelationColumnName(Oid relationId, char *columnName)
{
	Assert(columnName != NULL);

	AttrNumber attNum = get_attnum(relationId, columnName);

	if (attNum == InvalidAttrNumber)
	{
		ereport(ERROR, (errmsg("invalid attr %s", columnName)));
	}

	Relation relation = relation_open(relationId, AccessShareLock);

	Oid typeId = attnumTypeId(relation, attNum);

	relation_close(relation, AccessShareLock);

	return typeId;
}


/*
 * ColumnToColumnName returns the human-readable name of a column given a
 * relation identifier and the column's internal (Var) representation.
 * This function will raise an ERROR if no such column can be found or if the
 * provided Var refers to a system column.
 */
char *
ColumnToColumnName(Oid relationId, Node *columnNode)
{
	if (columnNode == NULL || !IsA(columnNode, Var))
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("not a valid column")));
	}

	Var *column = (Var *) columnNode;

	AttrNumber columnNumber = column->varattno;
	if (!AttrNumberIsForUserDefinedAttr(columnNumber))
	{
		char *relationName = get_rel_name(relationId);

		ereport(ERROR, (errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
						errmsg("attribute %d of relation \"%s\" is a system column",
							   columnNumber, relationName)));
	}

	char *columnName = get_attname(relationId, column->varattno, false);
	if (columnName == NULL)
	{
		char *relationName = get_rel_name(relationId);

		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_COLUMN),
						errmsg("attribute %d of relation \"%s\" does not exist",
							   columnNumber, relationName)));
	}

	return columnName;
}


/*
 * GetAttrNumForMatchingColumn returns the attribute number for the column
 * in the target relation that matches the given Var. If the column does not
 * exist or is not comparable, it returns InvalidAttrNumber.
 */
AttrNumber
GetAttrNumForMatchingColumn(RangeTblEntry *rteTarget, Oid relid, Var *var)
{
	char *targetColumnName = get_attname(relid, var->varattno, false);
	AttrNumber attnum = get_attnum(rteTarget->relid, targetColumnName);
	if (attnum == InvalidAttrNumber)
	{
		ereport(DEBUG5, (errmsg("Column %s does not exist in relation %s",
							   targetColumnName, rteTarget->eref->aliasname)));
		return InvalidAttrNumber;
	}
	if(var->vartype != get_atttype(rteTarget->relid, attnum))
	{
		ereport(DEBUG5, (errmsg("Column %s is not comparable for tables with relids %d and %d",
							   targetColumnName, rteTarget->relid, relid)));
		return InvalidAttrNumber;
	}
	return attnum;
}
