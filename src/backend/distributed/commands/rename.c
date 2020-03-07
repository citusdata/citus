/*-------------------------------------------------------------------------
 *
 * rename.c
 *    Commands for renaming objects related to distributed tables
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "catalog/index.h"
#include "catalog/namespace.h"
#include "distributed/commands.h"
#include "distributed/commands/utility_hook.h"
#include "distributed/metadata_cache.h"
#include "nodes/parsenodes.h"


/*
 * PreprocessRenameStmt first determines whether a given rename statement involves
 * a distributed table. If so (and if it is supported, i.e. renames a column),
 * it creates a DDLJob to encapsulate information needed during the worker node
 * portion of DDL execution before returning that DDLJob in a List. If no dis-
 * tributed table is involved, this function returns NIL.
 */
List *
PreprocessRenameStmt(Node *node, const char *renameCommand)
{
	RenameStmt *renameStmt = castNode(RenameStmt, node);
	Oid objectRelationId = InvalidOid; /* SQL Object OID */
	Oid tableRelationId = InvalidOid; /* Relation OID, maybe not the same. */

	/*
	 * We only support some of the PostgreSQL supported RENAME statements, and
	 * our list include only renaming table and index (related) objects.
	 */
	if (!IsAlterTableRenameStmt(renameStmt) &&
		!IsIndexRenameStmt(renameStmt) &&
		!IsPolicyRenameStmt(renameStmt))
	{
		return NIL;
	}

	/*
	 * The lock levels here should be same as the ones taken in
	 * RenameRelation(), renameatt() and RenameConstraint(). However, since all
	 * three statements have identical lock levels, we just use a single statement.
	 */
	objectRelationId = RangeVarGetRelid(renameStmt->relation,
										AccessExclusiveLock,
										renameStmt->missing_ok);

	/*
	 * If the table does not exist, don't do anything here to allow PostgreSQL
	 * to throw the appropriate error or notice message later.
	 */
	if (!OidIsValid(objectRelationId))
	{
		return NIL;
	}

	/* we have no planning to do unless the table is distributed */
	switch (renameStmt->renameType)
	{
		case OBJECT_TABLE:
		case OBJECT_FOREIGN_TABLE:
		case OBJECT_COLUMN:
		case OBJECT_TABCONSTRAINT:
		case OBJECT_POLICY:
		{
			/* the target object is our tableRelationId. */
			tableRelationId = objectRelationId;
			break;
		}

		case OBJECT_INDEX:
		{
			/*
			 * here, objRelationId points to the index relation entry, and we
			 * are interested into the entry of the table on which the index is
			 * defined.
			 */
			tableRelationId = IndexGetRelation(objectRelationId, false);
			break;
		}

		default:

			/*
			 * Nodes that are not supported by Citus: we pass-through to the
			 * main PostgreSQL executor. Any Citus-supported RenameStmt
			 * renameType must appear above in the switch, explicitly.
			 */
			return NIL;
	}

	bool isCitusRelation = IsCitusTable(tableRelationId);
	if (!isCitusRelation)
	{
		return NIL;
	}

	/*
	 * We might ERROR out on some commands, but only for Citus tables.
	 * That's why this test comes this late in the function.
	 */
	ErrorIfUnsupportedRenameStmt(renameStmt);

	DDLJob *ddlJob = palloc0(sizeof(DDLJob));
	ddlJob->targetRelationId = tableRelationId;
	ddlJob->concurrentIndexCmd = false;
	ddlJob->commandString = renameCommand;
	ddlJob->taskList = DDLTaskList(tableRelationId, renameCommand);

	return list_make1(ddlJob);
}


/*
 * ErrorIfDistributedRenameStmt errors out if the corresponding rename statement
 * operates on any part of a distributed table other than a column.
 *
 * Note: This function handles RenameStmt applied to relations handed by Citus.
 * At the moment of writing this comment, it could be either tables or indexes.
 */
void
ErrorIfUnsupportedRenameStmt(RenameStmt *renameStmt)
{
	if (IsAlterTableRenameStmt(renameStmt) &&
		renameStmt->renameType == OBJECT_TABCONSTRAINT)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("renaming constraints belonging to distributed tables is "
							   "currently unsupported")));
	}
}


/*
 * PreprocessRenameAttributeStmt called for RenameStmt's that are targetting an attribute eg.
 * type attributes. Based on the relation type the attribute gets renamed it dispatches to
 * a specialized implementation if present, otherwise return an empty list for its DDLJobs
 */
List *
PreprocessRenameAttributeStmt(Node *node, const char *queryString)
{
	RenameStmt *stmt = castNode(RenameStmt, node);
	Assert(stmt->renameType == OBJECT_ATTRIBUTE);

	switch (stmt->relationType)
	{
		case OBJECT_TYPE:
		{
			return PreprocessRenameTypeAttributeStmt(node, queryString);
		}

		default:
		{
			/* unsupported relation for attribute rename, do nothing */
			return NIL;
		}
	}
}
