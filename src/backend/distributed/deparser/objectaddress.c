/*-------------------------------------------------------------------------
 *
 * objectaddress.c
 *    Parstrees almost always target a object that postgres can address by
 *    an ObjectAddress. Here we have a walker for parsetrees to find the
 *    address of the object targeted.
 *
 * Copyright (c) 2019, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "distributed/commands.h"
#include "distributed/deparser.h"

static const ObjectAddress * AlterTableStmtObjectAddress(AlterTableStmt *stmt,
														 bool missing_ok);
static const ObjectAddress * RenameStmtObjectAddress(RenameStmt *stmt, bool missing_ok);
static const ObjectAddress * AlterObjectSchemaStmtObjectAddress(
	AlterObjectSchemaStmt *stmt, bool missing_ok);
static const ObjectAddress * RenameAttributeStmtObjectAddress(RenameStmt *stmt,
															  bool missing_ok);
static const ObjectAddress * AlterOwnerStmtObjectAddress(AlterOwnerStmt *stmt,
														 bool missing_ok);
static const ObjectAddress * AlterObjectDependsStmtObjectAddress(
	AlterObjectDependsStmt *stmt, bool missing_ok);


/*
 * GetObjectAddressFromParseTree returns the ObjectAdderss of the main target of the parse
 * tree.
 */
const ObjectAddress *
GetObjectAddressFromParseTree(Node *parseTree, bool missing_ok)
{
	switch (parseTree->type)
	{
		case T_CompositeTypeStmt:
		{
			return CompositeTypeStmtObjectAddress(castNode(CompositeTypeStmt, parseTree),
												  missing_ok);
		}

		case T_AlterTableStmt:
		{
			return AlterTableStmtObjectAddress(castNode(AlterTableStmt, parseTree),
											   missing_ok);
		}

		case T_CreateEnumStmt:
		{
			return CreateEnumStmtObjectAddress(castNode(CreateEnumStmt, parseTree),
											   missing_ok);
		}

		case T_AlterEnumStmt:
		{
			return AlterEnumStmtObjectAddress(castNode(AlterEnumStmt, parseTree),
											  missing_ok);
		}

		case T_RenameStmt:
		{
			return RenameStmtObjectAddress(castNode(RenameStmt, parseTree), missing_ok);
		}

		case T_AlterObjectSchemaStmt:
		{
			return AlterObjectSchemaStmtObjectAddress(castNode(AlterObjectSchemaStmt,
															   parseTree), missing_ok);
		}

		case T_AlterOwnerStmt:
		{
			return AlterOwnerStmtObjectAddress(castNode(AlterOwnerStmt, parseTree),
											   missing_ok);
		}

		case T_AlterFunctionStmt:
		{
			return AlterFunctionStmtObjectAddress(castNode(AlterFunctionStmt, parseTree),
												  missing_ok);
		}

		case T_CreateFunctionStmt:
		{
			return CreateFunctionStmtObjectAddress(
				castNode(CreateFunctionStmt, parseTree), missing_ok);
		}

		case T_AlterObjectDependsStmt:
		{
			return AlterObjectDependsStmtObjectAddress(
				castNode(AlterObjectDependsStmt, parseTree), missing_ok);
		}

		default:
		{
			/*
			 * should not be reached, indicates the coordinator is sending unsupported
			 * statements
			 */
			ereport(ERROR, (errmsg("unsupported statement to get object address for")));
			return NULL;
		}
	}
}


static const ObjectAddress *
AlterTableStmtObjectAddress(AlterTableStmt *stmt, bool missing_ok)
{
	switch (stmt->relkind)
	{
		case OBJECT_TYPE:
		{
			return AlterTypeStmtObjectAddress(stmt, missing_ok);
		}

		default:
		{
			ereport(ERROR, (errmsg("unsupported alter statement to get object address for"
								   )));
		}
	}
}


static const ObjectAddress *
RenameStmtObjectAddress(RenameStmt *stmt, bool missing_ok)
{
	switch (stmt->renameType)
	{
		case OBJECT_TYPE:
		{
			return RenameTypeStmtObjectAddress(stmt, missing_ok);
		}

		case OBJECT_ATTRIBUTE:
		{
			return RenameAttributeStmtObjectAddress(stmt, missing_ok);
		}

		case OBJECT_PROCEDURE:
		case OBJECT_AGGREGATE:
		case OBJECT_FUNCTION:
		{
			return RenameFunctionStmtObjectAddress(stmt, missing_ok);
		}

		default:
		{
			ereport(ERROR, (errmsg("unsupported rename statement to get object address "
								   "for")));
		}
	}
}


static const ObjectAddress *
AlterObjectSchemaStmtObjectAddress(AlterObjectSchemaStmt *stmt, bool missing_ok)
{
	switch (stmt->objectType)
	{
		case OBJECT_TYPE:
		{
			return AlterTypeSchemaStmtObjectAddress(stmt, missing_ok);
		}

		case OBJECT_PROCEDURE:
		case OBJECT_AGGREGATE:
		case OBJECT_FUNCTION:
		{
			return AlterFunctionSchemaStmtObjectAddress(stmt, missing_ok);
		}

		default:
		{
			ereport(ERROR, (errmsg("unsupported alter schema statement to get object "
								   "address for")));
		}
	}
}


static const ObjectAddress *
RenameAttributeStmtObjectAddress(RenameStmt *stmt, bool missing_ok)
{
	Assert(stmt->renameType == OBJECT_ATTRIBUTE);

	switch (stmt->relationType)
	{
		case OBJECT_TYPE:
		{
			return RenameTypeAttributeStmtObjectAddress(stmt, missing_ok);
		}

		default:
		{
			ereport(ERROR, (errmsg("unsupported alter rename attribute statement to get "
								   "object address for")));
		}
	}
}


static const ObjectAddress *
AlterOwnerStmtObjectAddress(AlterOwnerStmt *stmt, bool missing_ok)
{
	switch (stmt->objectType)
	{
		case OBJECT_TYPE:
		{
			return AlterTypeOwnerObjectAddress(stmt, missing_ok);
		}

		case OBJECT_PROCEDURE:
		case OBJECT_AGGREGATE:
		case OBJECT_FUNCTION:
		{
			return AlterFunctionOwnerObjectAddress(stmt, missing_ok);
		}

		default:
		{
			ereport(ERROR, (errmsg("unsupported alter owner statement to get object "
								   "address for")));
		}
	}
}


/*
 * AlterObjectDependsStmtObjectAddress resolves the ObjectAddress for the object targeted
 * by the AlterObjectDependStmt. This is done by dispatching the call to the object
 * specific implementation based on the ObjectType captured in the original statement. If
 * a specific implementation is not present an error will be raised. This is a developer
 * error since this function should only be reachable by calls of supported types.
 *
 * If missing_ok is set to fails the object specific implementation is supposed to raise
 * an error explaining the user the object is not existing.
 */
static const ObjectAddress *
AlterObjectDependsStmtObjectAddress(AlterObjectDependsStmt *stmt, bool missing_ok)
{
	switch (stmt->objectType)
	{
		case OBJECT_PROCEDURE:
		case OBJECT_FUNCTION:
		{
			return AlterFunctionDependsStmtObjectAddress(stmt, missing_ok);
		}

		default:
		{
			ereport(ERROR, (errmsg("unsupported alter depends on extension statement to "
								   "get object address for")));
		}
	}
}
