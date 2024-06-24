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
#include "nodes/parsenodes.h"
#include "utils/lsyscache.h"

#include "distributed/commands.h"
#include "distributed/commands/utility_hook.h"
#include "distributed/metadata_cache.h"


/*
 * PreprocessRenameStmt first determines whether a given rename statement involves
 * a distributed table. If so (and if it is supported, i.e. renames a column),
 * it creates a DDLJob to encapsulate information needed during the worker node
 * portion of DDL execution before returning that DDLJob in a List. If no dis-
 * tributed table is involved, this function returns NIL.
 */
List *
PreprocessRenameStmt(Node *node, const char *renameCommand,
					 ProcessUtilityContext processUtilityContext)
{
	RenameStmt *renameStmt = castNode(RenameStmt, node);
	Oid objectRelationId = InvalidOid; /* SQL Object OID */
	Oid tableRelationId = InvalidOid; /* Relation OID, maybe not the same. */

	/*
	 * We only support some of the PostgreSQL supported RENAME statements, and
	 * our list include only renaming table, index, policy and view (related) objects.
	 */
	if (!IsAlterTableRenameStmt(renameStmt) &&
		!IsIndexRenameStmt(renameStmt) &&
		!IsPolicyRenameStmt(renameStmt) &&
		!IsViewRenameStmt(renameStmt))
	{
		return NIL;
	}

	/*
	 * The lock levels here should be same as the ones taken in
	 * RenameRelation(), renameatt() and RenameConstraint(). All statements
	 * have identical lock levels except alter index rename.
	 */
	LOCKMODE lockmode = (IsIndexRenameStmt(renameStmt)) ?
						ShareUpdateExclusiveLock : AccessExclusiveLock;
	objectRelationId = RangeVarGetRelid(renameStmt->relation, lockmode,
										renameStmt->missing_ok);

	/*
	 * If the table does not exist, don't do anything here to allow PostgreSQL
	 * to throw the appropriate error or notice message later.
	 */
	if (!OidIsValid(objectRelationId))
	{
		return NIL;
	}

	/*
	 * Check whether we are dealing with a sequence or view here and route queries
	 * accordingly to the right processor function. We need to check both objects here
	 * since PG supports targeting sequences and views with ALTER TABLE commands.
	 */
	char relKind = get_rel_relkind(objectRelationId);
	if (relKind == RELKIND_SEQUENCE)
	{
		RenameStmt *stmtCopy = copyObject(renameStmt);
		stmtCopy->renameType = OBJECT_SEQUENCE;
		return PreprocessRenameSequenceStmt((Node *) stmtCopy, renameCommand,
											processUtilityContext);
	}
	else if (relKind == RELKIND_VIEW)
	{
		RenameStmt *stmtCopy = copyObject(renameStmt);
		stmtCopy->relationType = OBJECT_VIEW;
		if (stmtCopy->renameType == OBJECT_TABLE)
		{
			stmtCopy->renameType = OBJECT_VIEW;
		}

		return PreprocessRenameViewStmt((Node *) stmtCopy, renameCommand,
										processUtilityContext);
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
			if (relKind == RELKIND_INDEX ||
				relKind == RELKIND_PARTITIONED_INDEX)
			{
				/*
				 * Although weird, postgres allows ALTER TABLE .. RENAME command
				 * on indexes. We don't want to break non-distributed tables,
				 * so allow.
				 */
				tableRelationId = IndexGetRelation(objectRelationId, false);
				break;
			}

			/* the target object is our tableRelationId. */
			tableRelationId = objectRelationId;
			break;
		}

		case OBJECT_INDEX:
		{
			if (relKind == RELKIND_RELATION ||
				relKind == RELKIND_PARTITIONED_TABLE)
			{
				/*
				 * Although weird, postgres allows ALTER INDEX .. RENAME command
				 * on tables. We don't want to break non-distributed tables,
				 * so allow.
				 * Because of the weird syntax, we locked with wrong level, so relock
				 * the relation to acquire true level of lock. Same logic
				 * can be found in the function RenameRelation(RenameStmt) at tablecmds.c
				 */
				UnlockRelationOid(objectRelationId, lockmode);
				objectRelationId = RangeVarGetRelid(renameStmt->relation,
													AccessExclusiveLock,
													renameStmt->missing_ok);
				tableRelationId = objectRelationId;
				break;
			}

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

	if (renameStmt->renameType == OBJECT_TABLE ||
		renameStmt->renameType == OBJECT_FOREIGN_TABLE)
	{
		SwitchToSequentialAndLocalExecutionIfRelationNameTooLong(tableRelationId,
																 renameStmt->newname);
	}

	DDLJob *ddlJob = palloc0(sizeof(DDLJob));
	ObjectAddressSet(ddlJob->targetObjectAddress, RelationRelationId, tableRelationId);
	ddlJob->metadataSyncCommand = renameCommand;
	ddlJob->taskList = DDLTaskList(tableRelationId, renameCommand);

	return list_make1(ddlJob);
}


/*
 * ErrorIfUnsupportedRenameStmt errors out if the corresponding rename statement
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
PreprocessRenameAttributeStmt(Node *node, const char *queryString,
							  ProcessUtilityContext processUtilityContext)
{
	RenameStmt *stmt = castNode(RenameStmt, node);
	Assert(stmt->renameType == OBJECT_ATTRIBUTE);

	switch (stmt->relationType)
	{
		case OBJECT_TYPE:
		{
			return PreprocessRenameTypeAttributeStmt(node, queryString,
													 processUtilityContext);
		}

		default:
		{
			/* unsupported relation for attribute rename, do nothing */
			return NIL;
		}
	}
}
