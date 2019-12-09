/*-------------------------------------------------------------------------
 *
 * qualify.c
 *	  The deparser relies on fully qualified names on all statements to
 *	  work transparently on a remote worker node. Here we have helpers to
 *	  fully qualify parsetrees.
 *
 *	  Fully qualified parsetrees contain names for all identifiers that
 *	  are search_path agnostic. Meaning we need to include the schema name
 *	  for each and every identifier in the parsetree.
 *
 *	  This file contains mostly the distpatching functions to specialized
 *	  functions for each class of objects. eg qualify_type_stmt.c contains
 *	  all functions related to fully qualifying parsetrees that interact
 *	  with types.
 *
 * Copyright (c) 2019, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "distributed/deparser.h"


static void QualifyRenameStmt(RenameStmt *stmt);
static void QualifyRenameAttributeStmt(RenameStmt *stmt);
static void QualifyAlterTableStmt(AlterTableStmt *stmt);
static void QualifyAlterObjectSchemaStmt(AlterObjectSchemaStmt *stmt);
static void QualifyAlterOwnerStmt(AlterOwnerStmt *stmt);
static void QualifyAlterObjectDependsStmt(AlterObjectDependsStmt *stmt);
static void QualifyDropObjectStmt(DropStmt *stmt);

/*
 * QualifyTreeNode transforms the statement in place and makes all (supported) statements
 * fully qualified. Fully qualified statements allow for application on a remote postgres
 * server irregardless of their search_path.
 */
void
QualifyTreeNode(Node *stmt)
{
	switch (nodeTag(stmt))
	{
		case T_RenameStmt:
		{
			QualifyRenameStmt(castNode(RenameStmt, stmt));
			return;
		}

		case T_AlterEnumStmt:
		{
			QualifyAlterEnumStmt(castNode(AlterEnumStmt, stmt));
			return;
		}

		case T_AlterTableStmt:
		{
			QualifyAlterTableStmt(castNode(AlterTableStmt, stmt));
			return;
		}

		case T_CompositeTypeStmt:
		{
			QualifyCompositeTypeStmt(castNode(CompositeTypeStmt, stmt));
			return;
		}

		case T_CreateEnumStmt:
		{
			QualifyCreateEnumStmt(castNode(CreateEnumStmt, stmt));
			return;
		}

		case T_AlterObjectSchemaStmt:
		{
			QualifyAlterObjectSchemaStmt(castNode(AlterObjectSchemaStmt, stmt));
			return;
		}

		case T_AlterOwnerStmt:
		{
			QualifyAlterOwnerStmt(castNode(AlterOwnerStmt, stmt));
			return;
		}

		case T_AlterFunctionStmt:
		{
			QualifyAlterFunctionStmt(castNode(AlterFunctionStmt, stmt));
			return;
		}

		case T_AlterObjectDependsStmt:
		{
			QualifyAlterObjectDependsStmt(castNode(AlterObjectDependsStmt, stmt));
			return;
		}

		case T_DropStmt:
		{
			QualifyDropObjectStmt(castNode(DropStmt, stmt));
			break;
		}

		default:
		{
			/* skip unsupported statements */
			break;
		}
	}
}


/*
 * QualifyRenameStmt transforms a RENAME statement in place and makes all (supported)
 * statements fully qualified.
 */
static void
QualifyRenameStmt(RenameStmt *stmt)
{
	switch (stmt->renameType)
	{
		case OBJECT_TYPE:
		{
			QualifyRenameTypeStmt(stmt);
			return;
		}

		case OBJECT_ATTRIBUTE:
		{
			QualifyRenameAttributeStmt(stmt);
			return;
		}

		case OBJECT_COLLATION:
		{
			QualifyRenameCollationStmt(stmt);
			return;
		}

		case OBJECT_AGGREGATE:
		case OBJECT_FUNCTION:
		case OBJECT_PROCEDURE:
		{
			QualifyRenameFunctionStmt(stmt);
			return;
		}

		default:
		{
			/* skip unsupported statements */
			break;
		}
	}
}


/*
 * QualifyRenameAttributeStmt transforms a RENAME ATTRIBUTE statement in place and makes all (supported)
 * statements fully qualified.
 */
static void
QualifyRenameAttributeStmt(RenameStmt *stmt)
{
	Assert(stmt->renameType == OBJECT_ATTRIBUTE);

	switch (stmt->relationType)
	{
		case OBJECT_TYPE:
		{
			QualifyRenameTypeAttributeStmt(stmt);
			return;
		}

		default:
		{
			return;
		}
	}
}


static void
QualifyAlterTableStmt(AlterTableStmt *stmt)
{
	switch (stmt->relkind)
	{
		case OBJECT_TYPE:
		{
			QualifyAlterTypeStmt(stmt);
			return;
		}

		default:
		{
			/* skip unsupported statements */
			break;
		}
	}
}


static void
QualifyAlterObjectSchemaStmt(AlterObjectSchemaStmt *stmt)
{
	switch (stmt->objectType)
	{
		case OBJECT_TYPE:
		{
			QualifyAlterTypeSchemaStmt(stmt);
			return;
		}

		case OBJECT_COLLATION:
		{
			QualifyAlterCollationSchemaStmt(stmt);
			return;
		}

		case OBJECT_AGGREGATE:
		case OBJECT_FUNCTION:
		case OBJECT_PROCEDURE:
		{
			QualifyAlterFunctionSchemaStmt(stmt);
			return;
		}

		default:
		{
			/* skip unsupported statements */
			break;
		}
	}
}


static void
QualifyAlterOwnerStmt(AlterOwnerStmt *stmt)
{
	switch (stmt->objectType)
	{
		case OBJECT_TYPE:
		{
			QualifyAlterTypeOwnerStmt(stmt);
			return;
		}

		case OBJECT_COLLATION:
		{
			QualifyAlterCollationOwnerStmt(stmt);
			return;
		}

		case OBJECT_AGGREGATE:
		case OBJECT_FUNCTION:
		case OBJECT_PROCEDURE:
		{
			QualifyAlterFunctionOwnerStmt(stmt);
			return;
		}

		default:
		{
			return;
		}
	}
}


static void
QualifyAlterObjectDependsStmt(AlterObjectDependsStmt *stmt)
{
	switch (stmt->objectType)
	{
		case OBJECT_FUNCTION:
		case OBJECT_PROCEDURE:
		{
			QualifyAlterFunctionDependsStmt(stmt);
			return;
		}

		default:
		{
			return;
		}
	}
}


static void
QualifyDropObjectStmt(DropStmt *stmt)
{
	switch (stmt->removeType)
	{
		case OBJECT_COLLATION:
		{
			QualifyDropCollationStmt(stmt);
			return;
		}

		default:
		{
			return;
		}
	}
}
