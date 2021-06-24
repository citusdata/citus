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

#include "distributed/deparser.h"
#include "parser/parse_func.h"
#include "utils/lsyscache.h"


/*
 * QualifyAlterSequenceOwnerStmt transforms a
 * ALTER SEQUENCE .. OWNER TO ..
 * statement in place and makes the sequence name fully qualified.
 */
void
QualifyAlterSequenceOwnerStmt(Node *node)
{
	AlterTableStmt *stmt = castNode(AlterTableStmt, node);
	Assert(stmt->relkind == OBJECT_SEQUENCE);

	RangeVar *seq = stmt->relation;

	if (seq->schemaname == NULL)
	{
		Oid schemaOid = RangeVarGetCreationNamespace(seq);
		seq->schemaname = get_namespace_name(schemaOid);
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
		Oid schemaOid = RangeVarGetCreationNamespace(seq);
		seq->schemaname = get_namespace_name(schemaOid);
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
		Oid schemaOid = RangeVarGetCreationNamespace(seq);
		seq->schemaname = get_namespace_name(schemaOid);
	}
}
