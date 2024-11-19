/*-------------------------------------------------------------------------
 *
 * qualify_sequence_stmt.c
 *	  Functions specialized in fully qualifying all sequence statements. These
 *	  functions are dispatched from qualify.c
 *
 *	  Fully qualifying sequence statements consists of adding the schema name
 *	  to the subject of the sequence.
 *
 *	  Goal would be that the deparser functions for these statements can
 *	  serialize the statement without any external lookups.
 *
 * Copyright (c), Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "parser/parse_func.h"
#include "utils/lsyscache.h"

#include "distributed/commands.h"
#include "distributed/deparser.h"
#include "distributed/listutils.h"
#include "distributed/version_compat.h"


/*
 * QualifyAlterSequenceOwnerStmt transforms a
 * ALTER SEQUENCE .. OWNER TO ..
 * statement in place and makes the sequence name fully qualified.
 */
void
QualifyAlterSequenceOwnerStmt(Node *node)
{
	AlterTableStmt *stmt = castNode(AlterTableStmt, node);
	Assert(stmt->objtype == OBJECT_SEQUENCE);

	RangeVar *seq = stmt->relation;

	if (seq->schemaname == NULL)
	{
		Oid seqOid = RangeVarGetRelid(seq, NoLock, stmt->missing_ok);

		if (OidIsValid(seqOid))
		{
			Oid schemaOid = get_rel_namespace(seqOid);
			seq->schemaname = get_namespace_name(schemaOid);
		}
	}
}


/*
 * QualifyAlterSequencePersistenceStmt transforms a
 * ALTER SEQUENCE .. SET LOGGED/UNLOGGED
 * statement in place and makes the sequence name fully qualified.
 */
void
QualifyAlterSequencePersistenceStmt(Node *node)
{
	AlterTableStmt *stmt = castNode(AlterTableStmt, node);
	Assert(stmt->objtype == OBJECT_SEQUENCE);

	RangeVar *seq = stmt->relation;

	if (seq->schemaname == NULL)
	{
		Oid seqOid = RangeVarGetRelid(seq, NoLock, stmt->missing_ok);

		if (OidIsValid(seqOid))
		{
			Oid schemaOid = get_rel_namespace(seqOid);
			seq->schemaname = get_namespace_name(schemaOid);
		}
	}
}


/*
 * QualifyAlterSequenceSchemaStmt transforms a
 * ALTER SEQUENCE .. SET SCHEMA ..
 * statement in place and makes the sequence name fully qualified.
 */
void
QualifyAlterSequenceSchemaStmt(Node *node)
{
	AlterObjectSchemaStmt *stmt = castNode(AlterObjectSchemaStmt, node);
	Assert(stmt->objectType == OBJECT_SEQUENCE);

	RangeVar *seq = stmt->relation;

	if (seq->schemaname == NULL)
	{
		Oid seqOid = RangeVarGetRelid(seq, NoLock, stmt->missing_ok);

		if (OidIsValid(seqOid))
		{
			Oid schemaOid = get_rel_namespace(seqOid);
			seq->schemaname = get_namespace_name(schemaOid);
		}
	}
}


/*
 * QualifyRenameSequenceStmt transforms a
 * ALTER SEQUENCE .. RENAME TO ..
 * statement in place and makes the sequence name fully qualified.
 */
void
QualifyRenameSequenceStmt(Node *node)
{
	RenameStmt *stmt = castNode(RenameStmt, node);
	Assert(stmt->renameType == OBJECT_SEQUENCE);

	RangeVar *seq = stmt->relation;

	if (seq->schemaname == NULL)
	{
		Oid seqOid = RangeVarGetRelid(seq, NoLock, stmt->missing_ok);

		if (OidIsValid(seqOid))
		{
			Oid schemaOid = get_rel_namespace(seqOid);
			seq->schemaname = get_namespace_name(schemaOid);
		}
	}
}


/*
 * QualifyDropSequenceStmt transforms a DROP SEQUENCE
 * statement in place and makes the sequence name fully qualified.
 */
void
QualifyDropSequenceStmt(Node *node)
{
	DropStmt *stmt = castNode(DropStmt, node);

	Assert(stmt->removeType == OBJECT_SEQUENCE);

	List *objectNameListWithSchema = NIL;
	List *objectNameList = NULL;
	foreach_declared_ptr(objectNameList, stmt->objects)
	{
		RangeVar *seq = makeRangeVarFromNameList(objectNameList);

		if (seq->schemaname == NULL)
		{
			Oid seqOid = RangeVarGetRelid(seq, NoLock, stmt->missing_ok);

			if (OidIsValid(seqOid))
			{
				Oid schemaOid = get_rel_namespace(seqOid);
				seq->schemaname = get_namespace_name(schemaOid);
			}
		}

		objectNameListWithSchema = lappend(objectNameListWithSchema,
										   MakeNameListFromRangeVar(seq));
	}

	stmt->objects = objectNameListWithSchema;
}


/*
 * QualifyGrantOnSequenceStmt transforms a
 * GRANT ON SEQUENCE ...
 * statement in place and makes the sequence names fully qualified.
 */
void
QualifyGrantOnSequenceStmt(Node *node)
{
	GrantStmt *stmt = castNode(GrantStmt, node);
	Assert(stmt->objtype == OBJECT_SEQUENCE);

	/*
	 * The other option would be GRANT ALL SEQUENCES ON SCHEMA ...
	 * For that we don't need to qualify
	 */
	if (stmt->targtype != ACL_TARGET_OBJECT)
	{
		return;
	}
	List *qualifiedSequenceRangeVars = NIL;
	RangeVar *sequenceRangeVar = NULL;
	foreach_declared_ptr(sequenceRangeVar, stmt->objects)
	{
		if (sequenceRangeVar->schemaname == NULL)
		{
			Oid seqOid = RangeVarGetRelid(sequenceRangeVar, NoLock, false);
			Oid schemaOid = get_rel_namespace(seqOid);
			sequenceRangeVar->schemaname = get_namespace_name(schemaOid);
		}

		qualifiedSequenceRangeVars = lappend(qualifiedSequenceRangeVars,
											 sequenceRangeVar);
	}

	stmt->objects = qualifiedSequenceRangeVars;
}
