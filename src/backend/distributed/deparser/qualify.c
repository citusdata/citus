/*-------------------------------------------------------------------------
 *
 * qualify.c
 *	  The deparser relies on fully qualified names on all statements to
 *	  work transparently on a remote worker node. Here we have helpers to
 *	  fully qualify parsetrees
 *
 * Copyright (c) 2019, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "distributed/deparser.h"


static void qualify_rename_stmt(RenameStmt *stmt);
static void qualify_alter_table_stmt(AlterTableStmt *stmt);
static void qualify_alter_object_schema_stmt(AlterObjectSchemaStmt *stmt);


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
			qualify_rename_stmt(castNode(RenameStmt, stmt));
			return;
		}

		case T_AlterEnumStmt:
		{
			qualify_alter_enum_stmt(castNode(AlterEnumStmt, stmt));
			return;
		}

		case T_AlterTableStmt:
		{
			qualify_alter_table_stmt(castNode(AlterTableStmt, stmt));
			return;
		}

		case T_CompositeTypeStmt:
		{
			qualify_composite_type_stmt(castNode(CompositeTypeStmt, stmt));
			return;
		}

		case T_CreateEnumStmt:
		{
			qualify_create_enum_stmt(castNode(CreateEnumStmt, stmt));
			return;
		}

		case T_AlterObjectSchemaStmt:
		{
			qualify_alter_object_schema_stmt(castNode(AlterObjectSchemaStmt, stmt));
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
qualify_rename_stmt(RenameStmt *stmt)
{
	switch (stmt->renameType)
	{
		case OBJECT_TYPE:
		{
			qualify_rename_type_stmt(stmt);
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
qualify_alter_table_stmt(AlterTableStmt *stmt)
{
	switch (stmt->relkind)
	{
		case OBJECT_TYPE:
		{
			qualify_alter_type_stmt(stmt);
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
qualify_alter_object_schema_stmt(AlterObjectSchemaStmt *stmt)
{
	switch (stmt->objectType)
	{
		case OBJECT_TYPE:
		{
			qualify_alter_type_schema_stmt(stmt);
			return;
		}

		default:
		{
			/* skip unsupported statements */
			break;
		}
	}
}
