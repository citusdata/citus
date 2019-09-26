/*-------------------------------------------------------------------------
 *
 * deparse.c
 *    Entrypoint for deparsing parsetrees.
 *
 *    The goal of deparsing parsetrees is to reconstruct sql statements
 *    from any parsed sql statement by ParseTreeNode. Deparsed statements
 *    can be used to reapply them on remote postgres nodes like the citus
 *    workers.
 *
 * Copyright (c) 2019, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "distributed/deparser.h"

static const char * DeparseDropStmt(DropStmt *stmt);
static const char * DeparseAlterTableStmt(AlterTableStmt *stmt);
static const char * DeparseRenameStmt(RenameStmt *stmt);
static const char * DeparseRenameAttributeStmt(RenameStmt *stmt);
static const char * DeparseAlterObjectSchemaStmt(AlterObjectSchemaStmt *stmt);
static const char * DeparseAlterOwnerStmt(AlterOwnerStmt *stmt);
static const char * DeparseAlterObjectDependsStmt(AlterObjectDependsStmt *stmt);

/*
 * DeparseTreeNode aims to be the inverse of postgres' ParseTreeNode. Currently with
 * limited support. Check support before using, and add support for new statements as
 * required.
 *
 * Currently supported:
 *  - CREATE TYPE
 *  - ALTER TYPE
 *  - DROP TYPE
 *
 *  - ALTER FUNCTION
 *  - DROP FUNCTION
 *
 *  - ALTER PROCEDURE
 *  - DROP PROCEDURE
 */
const char *
DeparseTreeNode(Node *stmt)
{
	switch (nodeTag(stmt))
	{
		case T_DropStmt:
		{
			return DeparseDropStmt(castNode(DropStmt, stmt));
		}

		case T_CompositeTypeStmt:
		{
			return DeparseCompositeTypeStmt(castNode(CompositeTypeStmt, stmt));
		}

		case T_CreateEnumStmt:
		{
			return DeparseCreateEnumStmt(castNode(CreateEnumStmt, stmt));
		}

		case T_AlterTableStmt:
		{
			return DeparseAlterTableStmt(castNode(AlterTableStmt, stmt));
		}

		case T_AlterEnumStmt:
		{
			return DeparseAlterEnumStmt(castNode(AlterEnumStmt, stmt));
		}

		case T_AlterFunctionStmt:
		{
			return DeparseAlterFunctionStmt(castNode(AlterFunctionStmt, stmt));
		}

		case T_RenameStmt:
		{
			return DeparseRenameStmt(castNode(RenameStmt, stmt));
		}

		case T_AlterObjectSchemaStmt:
		{
			return DeparseAlterObjectSchemaStmt(castNode(AlterObjectSchemaStmt, stmt));
		}

		case T_AlterOwnerStmt:
		{
			return DeparseAlterOwnerStmt(castNode(AlterOwnerStmt, stmt));
		}

		case T_AlterObjectDependsStmt:
		{
			return DeparseAlterObjectDependsStmt(castNode(AlterObjectDependsStmt, stmt));
		}

		default:
		{
			ereport(ERROR, (errmsg("unsupported statement for deparsing")));
		}
	}
}


/*
 * DeparseDropStmt aims to deparse DROP statements.
 *
 * Currently with limited support. Check support before using, and add support for new
 * statements as required.
 */
static const char *
DeparseDropStmt(DropStmt *stmt)
{
	switch (stmt->removeType)
	{
		case OBJECT_TYPE:
		{
			return DeparseDropTypeStmt(stmt);
		}

		case OBJECT_FUNCTION:
#if PG_VERSION_NUM >= 110000
		case OBJECT_PROCEDURE:
#endif
			{
				return DeparseDropFunctionStmt(stmt);
			}

		default:
		{
			ereport(ERROR, (errmsg("unsupported drop statement for deparsing")));
		}
	}
}


/*
 * DeparseAlterTableStmt deparses an AlterTableStmt to its SQL command.
 * Contrary to its name these statements are covering not only ALTER TABLE ...
 * statements but are used for almost all relation-esque objects in postgres,
 * including but not limited to, Tables, Types, ...
 *
 * Currently with limited support. Check support before using, and add support for new
 * statements as required.
 */
static const char *
DeparseAlterTableStmt(AlterTableStmt *stmt)
{
	switch (stmt->relkind)
	{
		case OBJECT_TYPE:
		{
			return DeparseAlterTypeStmt(stmt);
		}

		default:
		{
			ereport(ERROR, (errmsg("unsupported alter statement for deparsing")));
		}
	}
}


/*
 * DeparseRenameStmt deparses an RenameStmt to its SQL command.
 * Contrary to its name these statements are not covering all ALTER .. RENAME
 * statements. 
 * 
 * e.g. ALTER TYPE name RENAME VALUE old TO new is an AlterEnumStmt
 *
 * Currently with limited support. Check support before using, and add support for new
 * statements as required.
 */
static const char *
DeparseRenameStmt(RenameStmt *stmt)
{
	switch (stmt->renameType)
	{
		case OBJECT_TYPE:
		{
			return DeparseRenameTypeStmt(stmt);
		}

		case OBJECT_ATTRIBUTE:
		{
			return DeparseRenameAttributeStmt(stmt);
		}

		case OBJECT_FUNCTION:
#if PG_VERSION_NUM >= 110000
		case OBJECT_PROCEDURE:
#endif
			{
				return DeparseRenameFunctionStmt(stmt);
			}

		default:
		{
			ereport(ERROR, (errmsg("unsupported rename statement for deparsing")));
		}
	}
}


static const char *
DeparseRenameAttributeStmt(RenameStmt *stmt)
{
	Assert(stmt->renameType == OBJECT_ATTRIBUTE);

	switch (stmt->relationType)
	{
		case OBJECT_TYPE:
		{
			return DeparseRenameTypeAttributeStmt(stmt);
		}

		default:
		{
			ereport(ERROR, (errmsg("unsupported rename attribute statement for"
								   " deparsing")));
		}
	}
}


/*
 * DeparseAlterObjectSchemaStmt aims to deparse
 * ALTER .. SET SCHEMA ..
 * statements.
 *
 * Currently with limited support. Check support before using, and add support for new
 * statements as required.
 */
static const char *
DeparseAlterObjectSchemaStmt(AlterObjectSchemaStmt *stmt)
{
	switch (stmt->objectType)
	{
		case OBJECT_TYPE:
		{
			return DeparseAlterTypeSchemaStmt(stmt);
		}

		case OBJECT_FUNCTION:
#if PG_VERSION_NUM >= 110000
		case OBJECT_PROCEDURE:
#endif
			{
				return DeparseAlterFunctionSchemaStmt(stmt);
			}

		default:
		{
			ereport(ERROR, (errmsg("unsupported rename statement for deparsing")));
		}
	}
}


/*
 * DeparseAlterOwnerStmt aims to deparse
 * ALTER .. OWNER TO ..
 * statements.
 *
 * Currently with limited support. Check support before using, and add support for new
 * statements as required.
 */
static const char *
DeparseAlterOwnerStmt(AlterOwnerStmt *stmt)
{
	switch (stmt->objectType)
	{
		case OBJECT_TYPE:
		{
			return DeparseAlterTypeOwnerStmt(stmt);
		}

		case OBJECT_FUNCTION:
#if PG_VERSION_NUM >= 110000
		case OBJECT_PROCEDURE:
#endif
			{
				return DeparseAlterFunctionOwnerStmt(stmt);
			}

		default:
		{
			ereport(ERROR, (errmsg("unsupported alter owner statement for deparsing")));
		}
	}
}


/*
 * DeparseAlterObjectDependsStmt aims to deparse
 * ALTER .. DEPENDS ON EXTENSION ..
 * statements.
 *
 * Currently with limited support. Check support before using, and add support for new
 * statements as required.
 */
static const char *
DeparseAlterObjectDependsStmt(AlterObjectDependsStmt *stmt)
{
	switch (stmt->objectType)
	{
		case OBJECT_FUNCTION:
#if PG_VERSION_NUM >= 110000
		case OBJECT_PROCEDURE:
#endif
			{
				return DeparseAlterFunctionDependsStmt(stmt);
			}

		default:
		{
			ereport(ERROR, (errmsg("unsupported alter depends statement for deparsing")));
		}
	}
}
