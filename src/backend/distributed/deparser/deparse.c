
#include "postgres.h"

#include "distributed/deparser.h"

static const char * deparse_drop_stmt(DropStmt *stmt);
static const char * deparse_alter_table_stmt(AlterTableStmt *stmt);
static const char * deparse_rename_stmt(RenameStmt *stmt);


/*
 * DeparseTreeNode aims to be the inverse of postgres' ParseTreeNode. Currently with
 * limited support. Check support before using, and add support for new statements as
 * required.
 *
 * Currently supported:
 *  - CREATE TYPE
 *  - ALTER TYPE
 *  - DROP TYPE
 */
const char *
DeparseTreeNode(Node *stmt)
{
	switch (nodeTag(stmt))
	{
		case T_DropStmt:
		{
			return deparse_drop_stmt(castNode(DropStmt, stmt));
		}

		case T_CompositeTypeStmt:
		{
			return deparse_composite_type_stmt(castNode(CompositeTypeStmt, stmt));
		}

		case T_CreateEnumStmt:
		{
			return deparse_create_enum_stmt(castNode(CreateEnumStmt, stmt));
		}

		case T_AlterTableStmt:
		{
			return deparse_alter_table_stmt(castNode(AlterTableStmt, stmt));
		}

		case T_AlterEnumStmt:
		{
			return deparse_alter_enum_stmt(castNode(AlterEnumStmt, stmt));
		}

		case T_RenameStmt:
		{
			return deparse_rename_stmt(castNode(RenameStmt, stmt));
		}

		default:
		{
			ereport(ERROR, (errmsg("unsupported statement for deparsing")));
		}
	}
}


static const char *
deparse_drop_stmt(DropStmt *stmt)
{
	switch (stmt->removeType)
	{
		case OBJECT_TYPE:
		{
			return deparse_drop_type_stmt(stmt);
		}

		default:
		{
			ereport(ERROR, (errmsg("unsupported drop statement for deparsing")));
		}
	}
}


static const char *
deparse_alter_table_stmt(AlterTableStmt *stmt)
{
	switch (stmt->relkind)
	{
		case OBJECT_TYPE:
		{
			return deparse_alter_type_stmt(stmt);
		}

		default:
		{
			ereport(ERROR, (errmsg("unsupported alter statement for deparsing")));
		}
	}
}


static const char *
deparse_rename_stmt(RenameStmt *stmt)
{
	switch (stmt->renameType)
	{
		case OBJECT_TYPE:
		{
			return deparse_rename_type_stmt(stmt);
		}

		default:
		{
			ereport(ERROR, (errmsg("unsupported rename statement for deparsing")));
		}
	}
}
